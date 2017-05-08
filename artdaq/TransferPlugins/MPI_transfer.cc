#include "artdaq/TransferPlugins/MPITransfer.hh"
#include <algorithm>

#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "cetlib/container_algorithms.h"

#include "artdaq-core/Data/Fragment.hh"

#include "artdaq/DAQrate/MPITag.hh"
#include "artdaq/DAQdata/Debug.hh"
#include "artdaq/DAQrate/Utils.hh"


/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
*/

std::mutex artdaq::MPITransfer::mpi_mutex_;

artdaq::MPITransfer::MPITransfer(fhicl::ParameterSet pset, TransferInterface::Role role)
	: TransferInterface(pset, role)
	, src_status_(status_t::SENDING)
	, recvd_count_(0)
	, expected_count_(-1)
	, payload_(buffer_count_)
	, synchronous_sends_(pset.get<bool>("synchronous_sends", true))
	, reqs_(buffer_count_, MPI_REQUEST_NULL)
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
			<< "No buffers configured.\n";
	}
	// Post all the buffers.
	for (size_t i = 0; i < buffer_count_; ++i)
	{
		// make sure all buffers are the correct size
		payload_[i].resize(max_fragment_size_words_);
		// Note that nextSource_() is not used here: it is not necessary to
		// check whether a source is DONE, and we avoid violating the
		// precondition of nextSource_().
		if (role == TransferInterface::Role::kReceive) post_(i);
	}
}

artdaq::MPITransfer::
~MPITransfer()
{
	TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: BEGIN");
	if (role() == TransferInterface::Role::kReceive)
	{
		// clean up the remaining buffers
		TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: Cancelling all reqs");
		for (size_t i = 0; i < buffer_count_; ++i)
		{
			cancelReq_(i, false);
		}
	}
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
	}/*
	TRACE(TLVL_VERBOSE, "MPITransfer::~MPITransfer: Entering Barrier");
	MPI_Barrier(MPI_COMM_WORLD);
	TRACE(TLVL_VERBOSE, "MPITransfer::~MPITransfer: Done with Barrier");*/
	TRACE(TLVL_TRACE, "MPITransfer::~MPITransfer: DONE");
}

int
artdaq::MPITransfer::
receiveFragment(Fragment& output, size_t timeout_usec)
{
	TRACE(6, "MPITransfer::receiveFragment entered tmo=%lu us", timeout_usec);
	//TLOG_DEBUG(uniqueLabel()) << "Start of receiveFragment" << TLOG_ENDL;
	int wait_result;
	int which;
	MPI_Status status;

	if (timeout_usec > 0)
	{
#if USE_TESTSOME
		if (ready_indices_.size() == 0) {
			ready_indices_.resize(buffer_count_, -1);
			ready_statuses_.resize(buffer_count_);

			int readyCount = 0;
			wait_result = MPI_Testsome(buffer_count_, &(reqs_[0]), &readyCount, &(ready_indices_[0]), &(ready_statuses_[0]));
			if (readyCount > 0) {
				saved_wait_result_ = wait_result;
				ready_indices_.resize(readyCount);
				ready_statuses_.resize(readyCount);
			}
			else {
				size_t sleep_loops = 10;
				size_t sleep_time = timeout_usec / sleep_loops;
				if (sleep_time > 250) {
					sleep_time = 250;
					sleep_loops = timeout_usec / sleep_time;
				}
				for (size_t idx = 0; idx < sleep_loops; ++idx) {
					usleep(sleep_time);
					wait_result = MPI_Testsome(buffer_count_, &reqs_[0], &readyCount,
											   &ready_indices_[0], &ready_statuses_[0]);
					if (readyCount > 0) { break; }
				}
				if (readyCount > 0) {
					saved_wait_result_ = wait_result;
					ready_indices_.resize(readyCount);
					ready_statuses_.resize(readyCount);
				}
				else {
					ready_indices_.clear();
					ready_statuses_.clear();
				}
			}
		}
		if (ready_indices_.size() > 0) {
			wait_result = saved_wait_result_;
			which = ready_indices_.front();
			status = ready_statuses_.front();
			ready_indices_.erase(ready_indices_.begin());
			ready_statuses_.erase(ready_statuses_.begin());
		}
		else {
			return RECV_TIMEOUT;
		}
#else
		int flag = 0;
		//TLOG_DEBUG(uniqueLabel()) << "Before first Testany buffer_count_=" << buffer_count_ << ", reqs_.size()=" << reqs_.size() << ", &reqs_[0]=" << &reqs_[0] << ", which=" << which << ", flag=" << flag << TLOG_ENDL;
		{
			std::unique_lock<std::mutex> lk(mpi_mutex_);
			wait_result = MPI_Testany(buffer_count_, &reqs_[0], &which, &flag, &status);
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
					wait_result = MPI_Testany(buffer_count_, &reqs_[0], &which, &flag, &status);
				}
				if (flag || which >= 0) { break; }
			}
			if (!flag)
			{
				return RECV_TIMEOUT;
			}
		}
#endif
	}
	else
	{
		{
			std::unique_lock<std::mutex> lk(mpi_mutex_);
			wait_result = MPI_Waitany(buffer_count_, &reqs_[0], &which, &status);
		}
	}
	//TLOG_DEBUG(uniqueLabel()) << "After testing/waiting res=" << wait_result << TLOG_ENDL;
	TRACE(8, "recvFragment recvd");

	if (which == MPI_UNDEFINED)
	{
		throw art::Exception(art::errors::LogicError, "MPITransfer: ")
			<< "MPI_UNDEFINED returned as on index value from Waitany.\n";
	}
	if (reqs_[which] != MPI_REQUEST_NULL)
	{
		throw art::Exception(art::errors::LogicError, "MPITransfer: ")
			<< "INTERNAL ERROR: req is not MPI_REQUEST_NULL in recvFragment.\n";
	}
	Fragment::sequence_id_t sequence_id = payload_[which].sequenceID();

	{
		std::ostringstream debugstream;
		debugstream << "recv: " << my_rank
			<< " idx=" << which
			<< " Waitany_error=" << wait_result
			<< " status_error=" << status.MPI_ERROR
			<< " source=" << status.MPI_SOURCE
			<< " tag=" << status.MPI_TAG
			<< " Fragment_sequenceID=" << sequence_id
			<< " Fragment_size=" << payload_[which].size()
			<< " preAutoResize_Fragment_dataSize=" << payload_[which].dataSize()
			<< " fragID=" << payload_[which].fragmentID()
			<< '\n';
		//TLOG_INFO(uniqueLabel()) << debugstream.str() << TLOG_ENDL;
		TRACE(4, debugstream.str().c_str());
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
	// The Fragment at index 'which' is now available.
	// Resize (down) to size to remove trailing garbage.
	TRACE(7, "recvFragment before autoResize/swap");
	//TLOG_INFO(uniqueLabel()) << "receiveFragment before resizing for output" << TLOG_ENDL;
	payload_[which].autoResize();
	output.swap(payload_[which]);
	TRACE(7, "recvFragment after autoResize/swap seqID=%lu. "
		  "Reset our buffer. max=%zu adr=%p"
		  , output.sequenceID(), max_fragment_size_words_, (void*)output.headerAddress());
	// Reset our buffer.
	Fragment tmp(max_fragment_size_words_);
	TRACE(7, "recvFragment before payload_[which].swap(tmp) adr=%p", (void*)tmp.headerAddress());
	payload_[which].swap(tmp);
	TRACE(7, "recvFragment after payload_[which].swap(tmp)");
	// Fragment accounting.
	if (output.type() == Fragment::EndOfDataFragmentType)
	{
		src_status_ = status_t::PENDING;
		expected_count_ = *output.dataBegin();

		{
			std::ostringstream debugstream;
			debugstream << "Received EOD from source " << status.MPI_SOURCE
				<< "  expecting total of "
				<< *output.dataBegin() << " fragments" << '\n';
			//TLOG_INFO(uniqueLabel()) << debugstream.str() << TLOG_ENDL;
			TRACE(4, debugstream.str().c_str());
		}
	}
	else
	{
		recvd_count_++;
	}
	switch (src_status_)
	{
	case status_t::PENDING:
	{
		std::ostringstream debugstream;
		debugstream << "Checking received count "
			<< recvd_count_
			<< " against expected total "
			<< expected_count_
			<< '\n';
		//TLOG_INFO(uniqueLabel()) << debugstream.str() << TLOG_ENDL;
		TRACE(4, debugstream.str().c_str());
	}
	if (recvd_count_ == expected_count_)
	{
		src_status_ = status_t::DONE;
	}
	break;
	case status_t::DONE:
		throw art::Exception(art::errors::LogicError, "MPITransfer: ")
			<< "Received extra fragments from source "
			<< status.MPI_SOURCE
			<< ".\n";
	case status_t::SENDING:
		break;
	default:
		throw art::Exception(art::errors::LogicError, "MPITransfer: ")
			<< "INTERNAL ERROR: Unrecognized status_t value "
			<< static_cast<int>(src_status_)
			<< ".\n";
	}
	// Repost to receive more data.
	if (src_status_ == status_t::DONE)
	{ // Just happened.
		if (nextSource_() != MPI_ANY_SOURCE)
		{ // No active sources left.
			post_(which); // This buffer doesn't need cancelling.
		}
	}
	else
	{
		post_(which);
	}
	//TLOG_INFO(uniqueLabel()) << "End of receiveFragment" << TLOG_ENDL;
	return status.MPI_SOURCE;
}

int
artdaq::MPITransfer::
nextSource_()
{
	// Precondition: last_source_posted_ must be set. This is ensured
	// provided nextSource_() is never called from the constructor.
	if (src_status_ != status_t::DONE)
	{
		return source_rank();
	}
	return MPI_ANY_SOURCE;
}

void
artdaq::MPITransfer::
cancelReq_(size_t buf, bool blocking_wait)
{
	if (reqs_[buf] == MPI_REQUEST_NULL) return;

	{
		std::ostringstream debugstream;
		debugstream << "Cancelling post for buffer "
			<< buf
			<< '\n';
		TRACE(4, debugstream.str().c_str());
		//TLOG_INFO(uniqueLabel()) << debugstream.str() << TLOG_ENDL;
	}

	std::unique_lock<std::mutex> lk(mpi_mutex_);
	int result = MPI_Cancel(&reqs_[buf]);
	if (result == MPI_SUCCESS)
	{
		MPI_Status status;
		if (blocking_wait)
		{
			MPI_Wait(&reqs_[buf], &status);
		}
		else
		{
			int doneFlag;
			MPI_Test(&reqs_[buf], &doneFlag, &status);
			if (!doneFlag)
			{
				size_t sleep_loops = 10;
				size_t sleep_time = 100000;
				for (size_t idx = 0; idx < sleep_loops; ++idx)
				{
					usleep(sleep_time);
					MPI_Test(&reqs_[buf], &doneFlag, &status);
					if (doneFlag) { break; }
				}
				if (!doneFlag)
				{
					TLOG_ERROR(uniqueLabel())
						<< "MPITransfer::cancelReq_: Timeout waiting to cancel the request for MPI buffer "
						<< buf << TLOG_ENDL;
				}
			}
		}
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

void
artdaq::MPITransfer::
post_(size_t buf)
{
	{
		std::ostringstream debugstream;
		debugstream << "Posting buffer " << buf
			<< " size=" << payload_[buf].size()
			<< " header address=0x" << std::hex << payload_[buf].headerAddress() << std::dec
			<< '\n';
		TRACE(4, debugstream.str().c_str());
		//TLOG_INFO(uniqueLabel()) << debugstream.str() << TLOG_ENDL;
	}

	std::unique_lock<std::mutex> lk(mpi_mutex_);
	MPI_Irecv(&*payload_[buf].headerBegin(),
		(payload_[buf].size() * sizeof(Fragment::value_type)),
			  MPI_BYTE,
			  source_rank(),
			  MPI_ANY_TAG,
			  MPI_COMM_WORLD,
			  &reqs_[buf]);
}

int artdaq::MPITransfer::findAvailable()
{
	int use_me = 0;
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

artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
moveFragment(Fragment&& frag, size_t send_timeout_usec)
{
	return sendFragment(std::move(frag), send_timeout_usec, false);
}

artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
copyFragment(Fragment& frag, size_t send_timeout_usec)
{
	return sendFragment(std::move(frag), send_timeout_usec, true);
}


artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
sendFragment(Fragment&& frag, size_t send_timeout_usec, bool force_async)
{
	TRACE(5, "copyFragmentTo timeout unused: %zu", send_timeout_usec);
	if (frag.dataSize() > max_fragment_size_words_)
	{
		throw cet::exception("Unimplemented")
			<< "Currently unable to deal with overlarge fragment payload ("
			<< frag.dataSize()
			<< " words > "
			<< max_fragment_size_words_
			<< ").";
	}

	TRACE(5, "MPITransfer::sendFragment: Checking whether to force async mode...");
	if (frag.type() == Fragment::EndOfDataFragmentType)
	{
		TRACE(5, "MPITransfer::sendFragment: EndOfDataFragment detected. Forcing async mode");
		force_async = true;
	}
	TRACE(5, "MPITransfer::sendFragment: Finding available buffer");
	int buffer_idx = findAvailable();
	if (buffer_idx == TransferInterface::RECV_TIMEOUT)
	{
		TRACE(TLVL_WARNING, "MPITransfer::sendFragment: No buffers available! Returning RECV_TIMEOUT!");
		return CopyStatus::kTimeout;
	}
	TRACE(5, "MPITransfer::sendFragment: Swapping in fragment to send to buffer %d", buffer_idx);
	Fragment& curfrag = payload_[buffer_idx];
	curfrag = std::move(frag);
	TRACE(5, "sendFragTo before send src=%d dest=%d seqID=%lu type=%d found_idx=%d"
		  , source_rank(), destination_rank(), curfrag.sequenceID(), curfrag.type(), buffer_idx);
	std::unique_lock<std::mutex> lk(mpi_mutex_);
	if (!synchronous_sends_ || force_async)
	{
		// 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
		TRACE(5, "MPITransfer::sendFragment: Using MPI_Isend");
		MPI_Isend(&*curfrag.headerBegin(),
				  curfrag.size() * sizeof(Fragment::value_type),
				  MPI_BYTE,
				  destination_rank(),
				  MPITag::FINAL,
				  MPI_COMM_WORLD,
				  &reqs_[buffer_idx]);
	}
	else
	{
		// 14-Sep-2015, KAB: switched from MPI_Send to MPI_Ssend based on
		// http://www.mcs.anl.gov/research/projects/mpi/sendmode.html.
		// This change was made after we noticed that MPI buffering
		// downstream of RootMPIOutput was causing EventBuilder memory
		// usage to grow when using MPI_Send with MPICH 3.1.4 and 3.1.2a.
		TRACE(5, "MPITransfer::sendFragment: Using MPI_Ssend");
		MPI_Ssend(&*curfrag.headerBegin(),
				  curfrag.size() * sizeof(Fragment::value_type),
				  MPI_BYTE,
				  destination_rank(),
				  MPITag::FINAL,
				  MPI_COMM_WORLD);
	}
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

DEFINE_ARTDAQ_TRANSFER(artdaq::MPITransfer)
