
#include <algorithm>
#include <vector>

#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "cetlib/container_algorithms.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"

#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq/DAQrate/MPITag.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQdata/Debug.hh"
#include "artdaq/DAQrate/Utils.hh"
#include "artdaq/TransferPlugins/MPITransfer.hh"

#define TRACE_NAME "MPI_Transfer"
#include "trace.h"		// TRACE

/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
*/

artdaq::MPITransfer::MPITransfer(fhicl::ParameterSet pset, TransferInterface::Role role)
  : TransferInterface(pset, role)
  , buffer_count_(pset.get<size_t>("buffer_count", 10))
  , max_payload_size_(pset.get<size_t>("max_fragment_size_words", 1024))
  , src_status_(status_t::SENDING)
  , recvd_count_(0)
  , expected_count_(0)
  , payload_(buffer_count_)
  , synchronous_sends_(pset.get<bool>("synchronous_sends", true))
  , reqs_(buffer_count_, MPI_REQUEST_NULL)
  , pos_()
{
  {
    std::ostringstream debugstream;
    debugstream << "MPITransfer construction: "
                << "source rank " << source_rank()  << ", "
				<< "destination rank " << destination_rank() << ", "
				<< buffer_count_ << " buffers. ";
    TRACE(4, debugstream.str().c_str());
  }

  if (buffer_count_ == 0) {
    throw art::Exception(art::errors::Configuration, "MPITransfer: ")
      << "No buffers configured.\n";
  }
  // Post all the buffers.
  for (size_t i = 0; i < buffer_count_; ++i) {
    // make sure all buffers are the correct size
    payload_[i].resize(max_payload_size_);
    // Note that nextSource_() is not used here: it is not necessary to
    // check whether a source is DONE, and we avoid violating the
    // precondition of nextSource_().
		if(role == TransferInterface::Role::kReceive) post_(i);
  }
}

artdaq::MPITransfer::
~MPITransfer()
{
  waitAll_();
}

size_t
artdaq::MPITransfer::
receiveFragment(Fragment & output, size_t timeout_usec)
{
  TRACE( 6,"recvFragment entered tmo=%lu us",timeout_usec  );
  int wait_result;
  int which;
  MPI_Status status;

  if (timeout_usec > 0) {
    if (ready_indices_.size() == 0) {
      ready_indices_.resize(buffer_count_, -1);
      ready_statuses_.resize(buffer_count_);

      int readyCount = 0;
      wait_result = MPI_Testsome(buffer_count_, &reqs_[0], &readyCount,
                                 &ready_indices_[0], &ready_statuses_[0]);
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
          if (readyCount > 0) {break;}
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
  }
  else {
    wait_result = MPI_Waitany(buffer_count_, &reqs_[0], &which, &status);
  }
  TRACE( 8, "recvFragment recvd" );

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (which == MPI_UNDEFINED)
	{ throw art::Exception(art::errors::LogicError, "MPITransfer: ")
		<< "MPI_UNDEFINED returned as on index value from Waitany.\n"; }
  if (reqs_[which] != MPI_REQUEST_NULL)
	{ throw art::Exception(art::errors::LogicError, "MPITransfer: ")
		<< "INTERNAL ERROR: req is not MPI_REQUEST_NULL in recvFragment.\n"; }
  Fragment::sequence_id_t sequence_id = payload_[which].sequenceID();

  {
    std::ostringstream debugstream;
    debugstream << "recv: " << rank
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
    TRACE(4, debugstream.str().c_str());
  }
  char err_buffer[MPI_MAX_ERROR_STRING];
  int resultlen;
  switch (wait_result) {
  case MPI_SUCCESS:
    break;
  case MPI_ERR_IN_STATUS:
    MPI_Error_string(status.MPI_ERROR, err_buffer, &resultlen);
    mf::LogError("MPITransfer_WaitError")
      << "Waitany ERROR: " << err_buffer << "\n";
    break;
  default:
    MPI_Error_string(wait_result, err_buffer, &resultlen);
    mf::LogError("MPITransfer_WaitError")
      << "Waitany ERROR: " << err_buffer << "\n";
  }
  // The Fragment at index 'which' is now available.
  // Resize (down) to size to remove trailing garbage.
  TRACE( 7, "recvFragment before autoResize/swap" );
  payload_[which].autoResize();
  output.swap(payload_[which]);
  TRACE( 7, "recvFragment after autoResize/swap seqID=%lu. "
		 "Reset our buffer. max=%zu adr=%p"
		 , output.sequenceID(), max_payload_size_, (void*)output.headerAddress() );
  // Reset our buffer.
  Fragment tmp(max_payload_size_);
  TRACE( 7, "recvFragment before payload_[which].swap(tmp) adr=%p", (void*)tmp.headerAddress() );
  payload_[which].swap(tmp);
  TRACE( 7, "recvFragment after payload_[which].swap(tmp)" );
  // Fragment accounting.
  if (output.type() == Fragment::EndOfDataFragmentType) {
    src_status_ = status_t::PENDING;
    expected_count_ = *output.dataBegin();

    {
      std::ostringstream debugstream;
      debugstream << "Received EOD from source " << status.MPI_SOURCE
				  << "  expecting total of "
				  << *output.dataBegin() << " fragments" << '\n';
      TRACE(4, debugstream.str().c_str());
    }
  }
  else {
	recvd_count_++;
  }
  switch (src_status_) {
  case status_t::PENDING:
    {
      std::ostringstream debugstream;
      debugstream << "Checking received count "
				  << recvd_count_
				  << " against expected total "
				  << expected_count_
				  << '\n';
      TRACE(4, debugstream.str().c_str());
    }
    if (recvd_count_ == expected_count_) {
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
  if (src_status_ == status_t::DONE) { // Just happened.
    if (nextSource_() == MPI_ANY_SOURCE) { // No active sources left.
      req_sources_[which] = MPI_ANY_SOURCE; // Done with this buffer.
    }
    else { // Post for input from a still-active source.
      post_(which); // This buffer doesn't need cancelling.
    }
    cancelAndRepost_(status.MPI_SOURCE); // Cancel and possibly repost.
  }
  else {
    post_(status.MPI_SOURCE);
  }
  return status.MPI_SOURCE;
}

void
artdaq::MPITransfer::
waitAll_()
{
  MPI_Waitall(buffer_count_, &reqs_[0], MPI_STATUSES_IGNORE);
  // clean up the remaining buffers
  for (size_t i = 0; i < buffer_count_; ++i) {
    if (req_sources_[i] != MPI_ANY_SOURCE) {
      cancelReq_(i, false);
    }
  }
}

int
artdaq::MPITransfer::
nextSource_()
{
  // Precondition: last_source_posted_ must be set. This is ensured
  // provided nextSource_() is never called from the constructor.
  if (src_status_ != status_t::DONE) {
	return source_rank();
  }
  return MPI_ANY_SOURCE;
}

void
artdaq::MPITransfer::
cancelReq_(size_t buf, bool blocking_wait)
{

  {
    std::ostringstream debugstream;
    debugstream << "Cancelling post for buffer "
				<< buf
				<< '\n';
    TRACE(4, debugstream.str().c_str());
  }
  int result = MPI_Cancel(&reqs_[buf]);
  if (result == MPI_SUCCESS) {
    MPI_Status status;
    if (blocking_wait) {
      MPI_Wait(&reqs_[buf], &status);
    }
    else {
      int doneFlag;
      MPI_Test(&reqs_[buf], &doneFlag, &status);
      if (! doneFlag) {
        size_t sleep_loops = 10;
        size_t sleep_time = 100000;
        for (size_t idx = 0; idx < sleep_loops; ++idx) {
          usleep(sleep_time);
          MPI_Test(&reqs_[buf], &doneFlag, &status);
          if (doneFlag) {break;}
        }
        if (! doneFlag) {
          mf::LogError("MPITransfer")
            << "Timeout waiting to cancel the request for MPI buffer "
            << buf;
        }
      }
    }
  }
  else {
    switch (result) {
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
  }
  MPI_Irecv(&*payload_[buf].headerBegin(),
            (payload_[buf].size() * sizeof(Fragment::value_type)),
            MPI_BYTE,
            source_rank(),
            MPI_ANY_TAG,
            MPI_COMM_WORLD,
            &reqs_[buf]);
}

void
artdaq::MPITransfer::
cancelAndRepost_(size_t src)
{
  for (size_t i = 0; i < buffer_count_; ++i) {
    if (static_cast<int>(src) == req_sources_[i]) {
      cancelReq_(i);
      int nextSource = nextSource_();
      if (nextSource == MPI_ANY_SOURCE) { // Done.
        req_sources_[i] = MPI_ANY_SOURCE;
      }
      else { // Still busy.
        post_(i);
      }
    }
  }
}

size_t artdaq::MPITransfer::findAvailable()
{
  size_t use_me = 0;
  int flag;
  size_t loops=0;
  TRACE(5, "findAvailable initial pos_=%zu", pos_);
  mf::LogDebug("MPITransfer") << "findAvailable: initial pos_ = " << pos_;
  do {
    use_me = pos_;
    MPI_Test(&reqs_[use_me], &flag, MPI_STATUS_IGNORE);
    pos_ = (pos_ + 1) % buffer_count_;
	++loops;
  }
  while (!flag && loops < buffer_count_);
  if(loops == buffer_count_) { return TransferInterface::RECV_TIMEOUT; }
  mf::LogDebug("MPITransfer") << "findAvailable returning " << use_me << " after " << loops << " iterations.";
  TRACE(5, "findAvailable returning use_me=%zu loops=%zu", use_me, loops );
  // pos_ is pointing at the next slot to check
  // use_me is pointing at the slot to use
  return use_me;
}

artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
moveFragment(Fragment&& frag, size_t send_timeout_usec) {
	return sendFragment(std::move(frag), send_timeout_usec, false);
}

artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
copyFragment(Fragment& frag, size_t send_timeout_usec) {
	return sendFragment(std::move(frag), send_timeout_usec, true);
}


artdaq::TransferInterface::CopyStatus
artdaq::MPITransfer::
sendFragment(Fragment&& frag, size_t send_timeout_usec, bool force_async)
{
  TRACE(5, "copyFragmentTo timeout unused: %zu", send_timeout_usec);
  if (frag.dataSize() > max_payload_size_) {
    throw cet::exception("Unimplemented")
	  << "Currently unable to deal with overlarge fragment payload ("
	  << frag.dataSize()
	  << " words > "
	  << max_payload_size_
	  << ").";
  }

  mf::LogDebug("MPITransfer") << "Checking whether to force async mode...";
  if (frag.type() == Fragment::EndOfDataFragmentType) {
	mf::LogDebug("MPITransfer") << "EndOfDataFragment detected. Forcing async mode";
	force_async = true;
  }
  mf::LogDebug("MPITransfer") << "Finding available buffer";
  size_t buffer_idx = findAvailable();
  if(buffer_idx == TransferInterface::RECV_TIMEOUT) {
	mf::LogWarning("MPITransfer") << "No buffers available! Returning RECV_TIMEOUT!";
	return CopyStatus::kTimeout;
  }
  mf::LogDebug("MPITransfer") << "Swapping in fragment to send to buffer " << buffer_idx;
  Fragment & curfrag = payload_[buffer_idx];
  curfrag = std::move(frag);
  mf::LogDebug("MPITransfer") << "Sending fragment from " << source_rank() << " to " << destination_rank() << " sequenceID " << curfrag.sequenceID() << " using buffer " << buffer_idx;
  TRACE( 5, "sendFragTo before send src=%zu dest=%zu seqID=%lu found_idx=%zu"
		 , source_rank() , destination_rank(), curfrag.sequenceID(), buffer_idx );
  if (! synchronous_sends_ || force_async) {
    // 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
	mf::LogDebug("MPITransfer") << "Using MPI_Isend";
    MPI_Isend(&*curfrag.headerBegin(),
              curfrag.size() * sizeof(Fragment::value_type),
              MPI_BYTE,
              destination_rank(),
              MPITag::FINAL,
              MPI_COMM_WORLD,
              &reqs_[buffer_idx]);
  }
  else {
    // 14-Sep-2015, KAB: switched from MPI_Send to MPI_Ssend based on
    // http://www.mcs.anl.gov/research/projects/mpi/sendmode.html.
    // This change was made after we noticed that MPI buffering
    // downstream of RootMPIOutput was causing EventBuilder memory
    // usage to grow when using MPI_Send with MPICH 3.1.4 and 3.1.2a.
	mf::LogDebug("MPITransfer") << "Using MPI_Ssend";
    MPI_Ssend(&*curfrag.headerBegin(),
              curfrag.size() * sizeof(Fragment::value_type),
              MPI_BYTE,
              destination_rank(),
              MPITag::FINAL,
              MPI_COMM_WORLD );
  }
  mf::LogDebug("MPITransfer") << "copyFragmentTo COMPLETE";
  TRACE( 5, "sendFragTo COMPLETE" );
  
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
