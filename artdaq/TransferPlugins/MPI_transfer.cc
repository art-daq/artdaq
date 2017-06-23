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

#define MPI_TAG_HEADER 0x8E
#define MPI_TAG_DATA 0xDA

std::mutex artdaq::MPITransfer::mpi_mutex_;

artdaq::MPITransfer::MPITransfer(fhicl::ParameterSet pset, TransferInterface::Role role)
	: TransferInterface(pset, role)
	, reqs_(2 * buffer_count_, MPI_REQUEST_NULL)
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
	if (role == Role::kSend)
	{
		auto size = buffer_count_ * max_fragment_size_words_ * sizeof(RawDataType);
		buffer_ = std::vector<uint8_t>(size);
		MPI_Buffer_attach(&buffer_[0], size);
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
	if (role() == Role::kSend)
	{
		int size;
		MPI_Buffer_detach(&buffer_[0], &size);
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
	TRACE(5, "copyFragmentTo timeout unused: %zu", send_timeout_usec);
	if (frag.dataSize() > max_fragment_size_words_)
	{
		TLOG_WARNING("MPITransfer") << "Fragment has size (" << frag.dataSize() << ") larger than max_fragment_size_words_ (" << max_fragment_size_words_ << ")."
			<< " Total buffer space is: " << max_fragment_size_words_ * buffer_count_ << " words. Multiple over-size Fragments will exhaust the buffer!" << TLOG_ENDL;
	}

	TRACE(5, "MPITransfer::sendFragment: Finding available send slot");
	int buffer_idx = findAvailable();
	if (buffer_idx == TransferInterface::RECV_TIMEOUT)
	{
		TRACE(TLVL_WARNING, "MPITransfer::sendFragment: No buffers available! Returning RECV_TIMEOUT!");
		return CopyStatus::kTimeout;
	}
	TRACE(5, "MPITransfer::sendFragment: Swapping in fragment to send to buffer %d", buffer_idx);

	TRACE(5, "sendFragTo before send src=%d dest=%d seqID=%lu type=%d found_idx=%d"
		  , source_rank(), destination_rank(), frag.sequenceID(), frag.type(), buffer_idx);
	std::unique_lock<std::mutex> lk(mpi_mutex_);

	// 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
	TRACE(5, "MPITransfer::sendFragment: Using MPI_Isend");
	MPI_Ibsend(frag.headerAddress(), detail::RawFragmentHeader::num_words() * sizeof(RawDataType), MPI_BYTE, destination_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &reqs_[buffer_idx]);

	auto sizeWrds = frag.size() - detail::RawFragmentHeader::num_words();
	auto offset = frag.headerAddress() + detail::RawFragmentHeader::num_words();
	MPI_Ibsend(offset, sizeWrds * sizeof(RawDataType), MPI_BYTE, destination_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &reqs_[buffer_idx + 1]);
	TRACE(5, "sendFragTo COMPLETE");

	{
		std::ostringstream debugstream;
		debugstream << "send COMPLETE: "
			<< " buffer_idx=" << buffer_idx
			<< " send_size=" << frag.size()
			<< " src=" << source_rank()
			<< " dest=" << destination_rank()
			<< " sequenceID=" << frag.sequenceID()
			<< " fragID=" << frag.fragmentID()
			<< '\n';
		TRACE(11, debugstream.str().c_str());
	}
	return CopyStatus::kSuccess;
}

int artdaq::MPITransfer::receiveFragmentHeader(detail::RawFragmentHeader& header, size_t timeout_usec)
{
	TRACE(6, "MPITransfer::receiveFragmentHeader entered tmo=%lu us", timeout_usec);
	MPI_Request req;
	MPI_Irecv(&header, header.num_words() * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_HEADER, MPI_COMM_WORLD, &req);

	//TLOG_DEBUG(uniqueLabel()) << "Start of receiveFragment" << TLOG_ENDL;
	int wait_result;
	MPI_Status status;

	if (timeout_usec > 0)
	{
		int flag = 0;
		//TLOG_DEBUG(uniqueLabel()) << "Before first Testany buffer_count_=" << buffer_count_ << ", reqs_.size()=" << reqs_.size() << ", &reqs_[0]=" << &reqs_[0] << ", which=" << which << ", flag=" << flag << TLOG_ENDL;
		{
			std::unique_lock<std::mutex> lk(mpi_mutex_);
			wait_result = MPI_Test(&req, &flag, &status);
		}
		if (!flag)
		{
			size_t sleep_loops = 10;
			size_t sleep_time = timeout_usec / sleep_loops;
			if (sleep_time > 250)
			{
				sleep_time = 250;
				sleep_loops = timeout_usec / sleep_time;
			}
			for (size_t idx = 0; idx < sleep_loops; ++idx)
			{
				usleep(sleep_time);
				//TLOG_DEBUG(uniqueLabel()) << "Before second Testany buffer_count_=" << buffer_count_ << ", reqs_.size()=" << reqs_.size() << ", &reqs_[0]=" << &reqs_[0] << ", which=" << which << ", flag=" << flag << TLOG_ENDL;
				{
					std::unique_lock<std::mutex> lk(mpi_mutex_);
					wait_result = MPI_Test(&req, &flag, &status);
				}
				if (flag) { break; }
			}
			if (!flag)
			{
				cancelReq_(req);
				return RECV_TIMEOUT;
			}
		}
	}
	else
	{
		{
			std::unique_lock<std::mutex> lk(mpi_mutex_);
			wait_result = MPI_Wait(&req, &status);
		}
	}
	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TRACE(8, "recvFragment recvd");

	if (req != MPI_REQUEST_NULL)
	{
		throw art::Exception(art::errors::LogicError, "MPITransfer: ") << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in recvFragment.\n";
	}


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
	TRACE(6, "MPITransfer::receiveFragmentData entered tmo=%lu us", timeout_usec);
	MPI_Request req;
	MPI_Irecv(destination, wordCount * sizeof(RawDataType), MPI_BYTE, source_rank(), MPI_TAG_DATA, MPI_COMM_WORLD, &req);

	//TLOG_DEBUG(uniqueLabel()) << "Start of receiveFragment" << TLOG_ENDL;
	int wait_result;
	MPI_Status status;

	if (timeout_usec > 0)
	{
		int flag = 0;
		//TLOG_DEBUG(uniqueLabel()) << "Before first Testany buffer_count_=" << buffer_count_ << ", reqs_.size()=" << reqs_.size() << ", &reqs_[0]=" << &reqs_[0] << ", which=" << which << ", flag=" << flag << TLOG_ENDL;
		{
			std::unique_lock<std::mutex> lk(mpi_mutex_);
			wait_result = MPI_Test(&req, &flag, &status);
		}
		if (!flag)
		{
			size_t sleep_loops = 10;
			size_t sleep_time = timeout_usec / sleep_loops;
			if (sleep_time > 250)
			{
				sleep_time = 250;
				sleep_loops = timeout_usec / sleep_time;
			}
			for (size_t idx = 0; idx < sleep_loops; ++idx)
			{
				usleep(sleep_time);
				//TLOG_DEBUG(uniqueLabel()) << "Before second Testany buffer_count_=" << buffer_count_ << ", reqs_.size()=" << reqs_.size() << ", &reqs_[0]=" << &reqs_[0] << ", which=" << which << ", flag=" << flag << TLOG_ENDL;
				{
					std::unique_lock<std::mutex> lk(mpi_mutex_);
					wait_result = MPI_Test(&req, &flag, &status);
				}
				if (flag) { break; }
			}
			if (!flag)
			{
				cancelReq_(req);
				return RECV_TIMEOUT;
			}
		}
	}
	else
	{
		{
			std::unique_lock<std::mutex> lk(mpi_mutex_);
			wait_result = MPI_Wait(&req, &status);
		}
	}
	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TRACE(8, "recvFragment recvd");

	if (req != MPI_REQUEST_NULL)
	{
		throw art::Exception(art::errors::LogicError, "MPITransfer: ") << "INTERNAL ERROR: req is not MPI_REQUEST_NULL in recvFragment.\n";
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
		pos_ = (pos_ + 1) % buffer_count_;
		++loops;
	} while (!flag && loops < buffer_count_);
	if (loops == buffer_count_) { return TransferInterface::RECV_TIMEOUT; }
	TRACE(5, "findAvailable returning use_me=%d loops=%zu", use_me, loops);
	// pos_ is pointing at the next slot to check
	// use_me is pointing at the slot to use
	return use_me;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::MPITransfer)
