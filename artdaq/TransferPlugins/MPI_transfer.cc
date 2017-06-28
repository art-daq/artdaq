#include "artdaq/TransferPlugins/MPITransfer.hh"
#include <algorithm>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"

#include "artdaq-core/Data/Fragment.hh"


/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
*/

#define MPI_TAG_HEADER 0x8E // 142
#define MPI_TAG_DATA 0xDA   // 218
#define USE_RECV 1

std::mutex artdaq::MPITransfer::mpi_mutex_;

artdaq::MPITransfer::MPITransfer(fhicl::ParameterSet pset, TransferInterface::Role role)
	: TransferInterface(pset, role)
	, reqs_(2 * buffer_count_, MPI_REQUEST_NULL)
	, payload_(buffer_count_)
	, pos_()
{
	{
		std::ostringstream debugstream;
		debugstream << "MPITransfer construction: "
			<< "source rank " << source_rank() << ", "
			<< "destination rank " << destination_rank() << ", "
			<< buffer_count_ << " buffers. ";
		TRACE(TLVL_TRACE, debugstream.str().c_str());
	}

	if (buffer_count_ == 0)
	{
		throw art::Exception(art::errors::Configuration, "MPITransfer: ")
			<< "No buffers configured.";
	}
}

artdaq::MPITransfer::
~MPITransfer()
{
	TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: BEGIN");
	TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: Collecting requests that need to be waited on");
	std::vector<MPI_Request> reqs;
	for (size_t ii = 0; ii < reqs_.size(); ++ii)
	{
		if (reqs_[ii] != MPI_REQUEST_NULL)
		{
			reqs.push_back(reqs_[ii]);
		}
	}
	if (reqs.size() > 0)
	{
		TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: Waiting on %zu reqs.", reqs.size());
		MPI_Waitall(reqs.size(), &reqs[0], MPI_STATUSES_IGNORE);
	}
	/*
	TRACE(TLVL_VERBOSE, "MPITransfer::~MPITransfer: Entering Barrier");
	MPI_Barrier(MPI_COMM_WORLD);
	TRACE(TLVL_VERBOSE, "MPITransfer::~MPITransfer: Done with Barrier");*/
	TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: DONE");
}

artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
copyFragment(Fragment& frag, size_t send_timeout_usec)
{
	return moveFragment(std::move(frag), send_timeout_usec);
}

artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
moveFragment(Fragment&& frag, size_t send_timeout_usec)
{
	if (frag.dataSize() > max_fragment_size_words_)
	{
		TLOG_WARNING("MPITransfer") << "Fragment has size (" << frag.dataSize() << ") larger than max_fragment_size_words_ (" << max_fragment_size_words_ << ")."
			<< " Total buffer space is: " << max_fragment_size_words_ * buffer_count_ << " words. Multiple over-size Fragments will exhaust the buffer!" << TLOG_ENDL;
	}

	auto start_time = std::chrono::steady_clock::now();

	TRACE(5, "MPITransfer::sendFragment: Finding available send slot, send_timeout_usec=%zu", send_timeout_usec);
	auto req_idx = findAvailable();
	auto counter = 0;
	while (req_idx == RECV_TIMEOUT && std::chrono::duration_cast<std::chrono::duration<size_t, std::ratio<1, 1000000>>>(std::chrono::steady_clock::now() - start_time).count() < send_timeout_usec)
	{
		usleep(1000);
		req_idx = findAvailable();
		counter++;
		if (counter % 1000 == 0)
		{
			TLOG_INFO("MPITransfer") << "Rank " << source_rank() << " waiting for available buffer to " << destination_rank() << ". "
				<< "Waited " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() << " ms so far." << TLOG_ENDL;
		}

	}
	if (req_idx == TransferInterface::RECV_TIMEOUT)
	{
		TRACE(TLVL_WARNING, "MPITransfer::sendFragment: No buffers available! Returning RECV_TIMEOUT!");
		return CopyStatus::kTimeout;
	}

	TRACE(5, "MPITransfer::moveFragment send slot is %d", req_idx);
	auto buffer_idx = req_idx / 2;
	TRACE(5, "MPITransfer::sendFragment: Swapping in fragment to send to buffer %d", buffer_idx);
	Fragment& curfrag = payload_[buffer_idx];
	curfrag = std::move(frag);

	TRACE(5, "sendFragTo before send src=%d dest=%d seqID=%lu type=%d found_idx=%d"
		  , source_rank(), destination_rank(), curfrag.sequenceID(), curfrag.type(), req_idx);

	std::unique_lock<std::mutex> lk(mpi_mutex_);

	// 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
	TRACE(5, "MPITransfer::sendFragment: Using MPI_Isend");
	//Waits for the receiver to acknowledge header
	MPI_Issend(curfrag.headerAddress(), detail::RawFragmentHeader::num_words() * sizeof(RawDataType), MPI_BYTE, destination_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &reqs_[req_idx]);

	auto sizeWrds = curfrag.size() - detail::RawFragmentHeader::num_words();
	auto offset = curfrag.headerAddress() + detail::RawFragmentHeader::num_words();
	MPI_Issend(offset, sizeWrds * sizeof(RawDataType), MPI_BYTE, destination_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &reqs_[req_idx + 1]);
	TRACE(5, "sendFragTo COMPLETE");

	{
		std::ostringstream debugstream;
		debugstream << "send COMPLETE: "
			<< " buffer_idx=" << buffer_idx
			<< " send_size=" << curfrag.size()
			<< " src=" << source_rank()
			<< " dest=" << destination_rank()
			<< " sequenceID=" << curfrag.sequenceID()
			<< " fragID=" << curfrag.fragmentID()
			<< '\n';
		TRACE(11, debugstream.str().c_str());
	}
	return CopyStatus::kSuccess;
}

int artdaq::MPITransfer::receiveFragmentHeader(detail::RawFragmentHeader& header, size_t timeout_usec)
{
	TRACE(6, "MPITransfer::receiveFragmentHeader entered tmo=%lu us (ignored)", timeout_usec);
	MPI_Status status;
	int wait_result = MPI_SUCCESS;
	{
		std::unique_lock<std::mutex> lk(mpi_mutex_);
#if USE_RECV
		wait_result = MPI_Recv(&header, header.num_words() * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &status);
#else
		MPI_Request req;
		MPI_Irecv(&header, header.num_words() * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &req);

		//TLOG_DEBUG(uniqueLabel()) << "Start of receiveFragment" << TLOG_ENDL;

		wait_result = MPI_Wait(&req, &status);

		if (req != MPI_REQUEST_NULL)
		{
			throw art::Exception(art::errors::LogicError, "MPITransfer: ") << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in recvFragment.\n";
		}
#endif
	}
	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TRACE(8, "recvFragmentHeader recvd");


	{TLOG_ARB(TRANSFER_RECEIVE2, "MPITransfer") << "recv: " << my_rank
		<< " Wait_error=" << wait_result
		<< " status_error=" << status.MPI_ERROR
		<< " source=" << status.MPI_SOURCE
		<< " tag=" << status.MPI_TAG
		<< " Fragment_sequenceID=" << header.sequence_id
		<< " Fragment_size=" << header.word_count
		<< " fragID=" << header.fragment_id << TLOG_ENDL;
	}
	char err_buffer[MPI_MAX_ERROR_STRING];
	int resultlen;
	switch (wait_result)
	{
	case MPI_SUCCESS:
		break;
	case MPI_ERR_IN_STATUS:
		MPI_Error_string(status.MPI_ERROR, err_buffer, &resultlen);
		TLOG_ERROR(uniqueLabel())
			<< "MPITransfer: Waitany ERROR: " << err_buffer << "\n" << TLOG_ENDL;
		break;
	default:
		MPI_Error_string(wait_result, err_buffer, &resultlen);
		TLOG_ERROR(uniqueLabel())
			<< "MPITransfer: Waitany ERROR: " << err_buffer << "\n" << TLOG_ENDL;
	}

	//TLOG_INFO(uniqueLabel()) << "End of receiveFragment" << TLOG_ENDL;
	return status.MPI_SOURCE;
	}

int artdaq::MPITransfer::receiveFragmentData(RawDataType* destination, size_t wordCount, size_t timeout_usec)
{
	TRACE(6, "MPITransfer::receiveFragmentData entered wordCount=%zu tmo=%lu us (ignored)", wordCount, timeout_usec);
	int wait_result;
	MPI_Status status;
	{
		std::unique_lock<std::mutex> lk(mpi_mutex_);
#if USE_RECV	
		wait_result = MPI_Recv(destination, wordCount * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &status);

#else
		MPI_Request req;
		MPI_Irecv(destination, wordCount * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &req);
		//TLOG_DEBUG(uniqueLabel()) << "Start of receiveFragment" << TLOG_ENDL;

		wait_result = MPI_Wait(&req, &status);
		if (req != MPI_REQUEST_NULL)
		{
			throw art::Exception(art::errors::LogicError, "MPITransfer: ") << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in recvFragment.\n";
		}
#endif
	}
	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TRACE(8, "recvFragmentData recvd");


	char err_buffer[MPI_MAX_ERROR_STRING];
	int resultlen;
	switch (wait_result)
	{
	case MPI_SUCCESS:
		break;
	case MPI_ERR_IN_STATUS:
		MPI_Error_string(status.MPI_ERROR, err_buffer, &resultlen);
		TLOG_ERROR(uniqueLabel())
			<< "MPITransfer: Waitany ERROR: " << err_buffer << "\n" << TLOG_ENDL;
		break;
	default:
		MPI_Error_string(wait_result, err_buffer, &resultlen);
		TLOG_ERROR(uniqueLabel())
			<< "MPITransfer: Waitany ERROR: " << err_buffer << "\n" << TLOG_ENDL;
	}

	//TLOG_INFO(uniqueLabel()) << "End of receiveFragment" << TLOG_ENDL;
	return status.MPI_SOURCE;
	}

void
artdaq::MPITransfer::
cancelReq_(MPI_Request req) const
{
	if (req == MPI_REQUEST_NULL) return;

	TLOG_ARB(TRANSFER_RECEIVE2, "MPITransfer") << "Cancelling post" << TLOG_ENDL;

	std::unique_lock<std::mutex> lk(mpi_mutex_);
	int result = MPI_Cancel(&req);
	if (result == MPI_SUCCESS)
	{
		MPI_Status status;
		MPI_Wait(&req, &status);
	}
	else
	{
		switch (result)
		{
		case MPI_ERR_REQUEST:
			throw art::Exception(art::errors::LogicError, "MPITransfer: ")
				<< "MPI_Cancel returned MPI_ERR_REQUEST.\n";
		case MPI_ERR_ARG:
			throw art::Exception(art::errors::LogicError, "MPITransfer: ")
				<< "MPI_Cancel returned MPI_ERR_ARG.\n";
		default:
			throw art::Exception(art::errors::LogicError, "MPITransfer: ")
				<< "MPI_Cancel returned unknown error code.\n";
		}
	}
}

int artdaq::MPITransfer::findAvailable()
{
	int use_me;
	int flag;
	size_t loops = 0;
	TRACE(5, "findAvailable initial pos_=%d", pos_);
	do
	{
		use_me = pos_;
		std::unique_lock<std::mutex> lk(mpi_mutex_);
		MPI_Test(&reqs_[use_me], &flag, MPI_STATUS_IGNORE);
		if (flag) {
			MPI_Test(&reqs_[use_me + 1], &flag, MPI_STATUS_IGNORE);
		}
		pos_ = (pos_ + 2) % reqs_.size();
		++loops;
	} while (!flag && loops < buffer_count_);
	if (loops == buffer_count_) { return TransferInterface::RECV_TIMEOUT; }
	TRACE(5, "findAvailable returning use_me=%d loops=%zu", use_me, loops);
	// pos_ is pointing at the next slot to check
	// use_me is pointing at the slot to use
	return use_me;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::MPITransfer)
