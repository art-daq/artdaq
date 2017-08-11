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
	TLOG_TRACE(uniqueLabel()) << "MPITransfer construction: "
		<< "source rank " << source_rank() << ", "
		<< "destination rank " << destination_rank() << ", "
		<< buffer_count_ << " buffers. " << TLOG_ENDL;

	if (buffer_count_ == 0)
	{
		throw art::Exception(art::errors::Configuration, "MPITransfer: ")
			<< "No buffers configured.";
	}
}

artdaq::MPITransfer::
~MPITransfer()
{
	TLOG_TRACE(uniqueLabel()) << "MPITransfer::~MPITransfer: BEGIN" << TLOG_ENDL;
	TLOG_TRACE(uniqueLabel()) << "MPITransfer::~MPITransfer: Collecting requests that need to be waited on" << TLOG_ENDL;
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
		TLOG_TRACE(uniqueLabel()) << "MPITransfer::~MPITransfer: Waiting on " << std::to_string(reqs.size()) << " reqs." << TLOG_ENDL;
		MPI_Waitall(reqs.size(), &reqs[0], MPI_STATUSES_IGNORE);
	}
	/*
	TLOG_ARB(TLVL_VERBOSE, "MPITransfer::~MPITransfer: Entering Barrier");
	MPI_Barrier(MPI_COMM_WORLD);
	TLOG_ARB(TLVL_VERBOSE, "MPITransfer::~MPITransfer: Done with Barrier");*/
	TLOG_TRACE(uniqueLabel()) << "MPITransfer::~MPITransfer: DONE" << TLOG_ENDL;
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
		TLOG_WARNING(uniqueLabel()) << "Fragment has size (" << frag.dataSize() << ") larger than max_fragment_size_words_ (" << max_fragment_size_words_ << ")."
			<< " Total buffer space is: " << max_fragment_size_words_ * buffer_count_ << " words. Multiple over-size Fragments will exhaust the buffer!" << TLOG_ENDL;
	}

	auto start_time = std::chrono::steady_clock::now();

	TLOG_ARB(5, uniqueLabel()) << "MPITransfer::moveFragment: Finding available send slot, send_timeout_usec=" << std::to_string(send_timeout_usec) << TLOG_ENDL;
	auto req_idx = findAvailable();
	auto counter = 0;
	while (req_idx == RECV_TIMEOUT && std::chrono::duration_cast<std::chrono::duration<size_t, std::ratio<1, 1000000>>>(std::chrono::steady_clock::now() - start_time).count() < send_timeout_usec)
	{
		usleep(1000);
		req_idx = findAvailable();
		counter++;
		if (counter % 1000 == 0)
		{
			TLOG_INFO(uniqueLabel()) << "Rank " << source_rank() << " waiting for available buffer to " << destination_rank() << ". "
				<< "Waited " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() << " ms so far." << TLOG_ENDL;
		}

	}
	if (req_idx == TransferInterface::RECV_TIMEOUT)
	{
		TLOG_WARNING(uniqueLabel()) << "MPITransfer::sendFragment: No buffers available! Returning RECV_TIMEOUT!" << TLOG_ENDL;
		return CopyStatus::kTimeout;
	}

	TLOG_ARB(5, uniqueLabel()) << "MPITransfer::moveFragment send slot is " << req_idx << TLOG_ENDL;
	auto buffer_idx = req_idx / 2;
	TLOG_ARB(5, uniqueLabel()) << "MPITransfer::moveFragment: Swapping in fragment to send to buffer " << buffer_idx << TLOG_ENDL;
	Fragment& curfrag = payload_[buffer_idx];
	curfrag = std::move(frag);

	TLOG_ARB(5, uniqueLabel()) << "MPITransfer::moveFragment before send src=" << source_rank() << " dest=" << destination_rank() << " seqID=" << std::to_string(curfrag.sequenceID()) << " type=" << curfrag.typeString() << " found_idx=" << req_idx << TLOG_ENDL;

	std::unique_lock<std::mutex> lk(mpi_mutex_);

	// 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
	TLOG_ARB(5, uniqueLabel()) << "MPITransfer::moveFragment: Using MPI_Isend" << TLOG_ENDL;
	//Waits for the receiver to acknowledge header
	MPI_Issend(curfrag.headerAddress(), detail::RawFragmentHeader::num_words() * sizeof(RawDataType), MPI_BYTE, destination_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &reqs_[req_idx]);

	auto sizeWrds = curfrag.size() - detail::RawFragmentHeader::num_words();
	auto offset = curfrag.headerAddress() + detail::RawFragmentHeader::num_words();
	MPI_Issend(offset, sizeWrds * sizeof(RawDataType), MPI_BYTE, destination_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &reqs_[req_idx + 1]);
	TLOG_ARB(5, uniqueLabel()) << "MPITransfer::moveFragment COMPLETE" << TLOG_ENDL;

	TLOG_ARB(11, uniqueLabel()) << "MPITransfer::moveFragment COMPLETE: "
		<< " buffer_idx=" << buffer_idx
		<< " send_size=" << curfrag.size()
		<< " src=" << source_rank()
		<< " dest=" << destination_rank()
		<< " sequenceID=" << curfrag.sequenceID()
		<< " fragID=" << curfrag.fragmentID() << TLOG_ENDL;
	return CopyStatus::kSuccess;
}

int artdaq::MPITransfer::receiveFragmentHeader(detail::RawFragmentHeader& header, size_t timeout_usec)
{
	TLOG_ARB(6, uniqueLabel()) << "MPITransfer::receiveFragmentHeader entered tmo=" << std::to_string(timeout_usec) << " us (ignored)" << TLOG_ENDL;
	MPI_Status status;
	int wait_result = MPI_SUCCESS;

	MPI_Request req;
	{
		std::unique_lock<std::mutex> lk(mpi_mutex_);
		MPI_Irecv(&header, header.num_words() * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &req);
	}
	//TLOG_DEBUG(uniqueLabel()) << "MPITransfer::receiveFragmentHeader: Start of receiveFragment" << TLOG_ENDL;

	int flag;
	do {
		std::unique_lock<std::mutex> lk(mpi_mutex_);
		wait_result = MPI_Test(&req, &flag, &status);
		if (!flag) {
			usleep(1000);
			//TLOG_ARB(6, uniqueLabel()) << "MPITransfer::receiveFragmentHeader wait loop, flag=" << flag << TLOG_ENDL;
		}
	} while (!flag);

	if (req != MPI_REQUEST_NULL)
	{
		TLOG_ERROR(uniqueLabel()) << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in receiveFragmentHeader." << TLOG_ENDL;
		throw art::Exception(art::errors::LogicError, "MPITransfer: ") << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in receiveFragmentHeader.";
	}
	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TLOG_ARB(8, uniqueLabel()) << "MPITransfer::receiveFragmentHeader recvd" << TLOG_ENDL;


	{TLOG_ARB(TRANSFER_RECEIVE2, uniqueLabel()) << "MPITransfer::receiveFragmentHeader: " << my_rank
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

int artdaq::MPITransfer::receiveFragmentData(RawDataType* destination, size_t wordCount)
{
	TLOG_ARB(6, uniqueLabel()) << "MPITransfer::receiveFragmentData entered wordCount=" << std::to_string(wordCount) << TLOG_ENDL;
	int wait_result = MPI_SUCCESS;
	MPI_Status status;

	MPI_Request req;
	{
		std::unique_lock<std::mutex> lk(mpi_mutex_);
		MPI_Irecv(destination, wordCount * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &req);
	}
	//TLOG_DEBUG(uniqueLabel()) << "Start of receiveFragment" << TLOG_ENDL;

	int flag;
	do {
		std::unique_lock<std::mutex> lk(mpi_mutex_);
		wait_result = MPI_Test(&req, &flag, &status);
		if (!flag) {
			usleep(1000);
			//TLOG_ARB(6, uniqueLabel()) << "MPITransfer::receiveFragmentData wait loop, flag=" << flag << TLOG_ENDL;
		}
	} while (!flag);
	if (req != MPI_REQUEST_NULL)
	{
		TLOG_ERROR(uniqueLabel()) << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in receiveFragmentData." << TLOG_ENDL;
		throw art::Exception(art::errors::LogicError, "MPITransfer: ") << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in receiveFragmentData.";
	}

	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TLOG_ARB(8, uniqueLabel()) << "MPITransfer::receiveFragmentData recvd" << TLOG_ENDL;


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

	//TLOG_INFO(uniqueLabel()) << "End of MPITransfer::receiveFragmentData" << TLOG_ENDL;
	return status.MPI_SOURCE;
}

void
artdaq::MPITransfer::
cancelReq_(MPI_Request req) const
{
	if (req == MPI_REQUEST_NULL) return;

	TLOG_ARB(TRANSFER_RECEIVE2, uniqueLabel()) << "Cancelling post" << TLOG_ENDL;

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
	int flag, flag2;
	size_t loops = 0;
	//TLOG_ARB(5,uniqueLabel()) <<  "findAvailable initial pos_=" << pos_ << TLOG_ENDL;
	do
	{
		use_me = pos_;
		std::unique_lock<std::mutex> lk(mpi_mutex_);
		MPI_Test(&reqs_[use_me], &flag, MPI_STATUS_IGNORE);
		if (flag) {
			MPI_Test(&reqs_[use_me + 1], &flag2, MPI_STATUS_IGNORE);
		}
		pos_ = (pos_ + 2) % reqs_.size();
		++loops;
	} while (!flag2 && loops < buffer_count_);
	if (loops == buffer_count_) { return TransferInterface::RECV_TIMEOUT; }
	TLOG_ARB(5, uniqueLabel()) << "findAvailable returning use_me=" << use_me << " loops=" << std::to_string(loops) << TLOG_ENDL;
	// pos_ is pointing at the next slot to check
	// use_me is pointing at the slot to use
	return use_me;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::MPITransfer)
