#include "artdaq/DAQrate/RHandles.hh"

#ifdef CANVAS
#include "canvas/Utilities/Exception.h"
#else
#include "art/Utilities/Exception.h"
#endif
//#include "artdaq/DAQdata/Debug.hh"
#include "artdaq/DAQrate/MPITag.hh"
#include "artdaq/DAQrate/Utils.hh"
#include "cetlib/container_algorithms.h"
#include "cetlib/exception.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#define TRACE_NAME "RHandles"
#include "trace.h"		// TRACE

const size_t artdaq::RHandles::RECV_TIMEOUT = 0xfedcba98;

artdaq::RHandles::RHandles(size_t buffer_count,
                           uint64_t max_payload_size,
                           size_t src_count,
                           size_t src_start):
  buffer_count_(buffer_count),
  max_payload_size_(max_payload_size),
  src_count_(src_count),
  src_start_(src_start),
  recv_frag_count_(src_count, src_start),
  src_status_(src_count, status_t::SENDING),
  expected_count_(src_count, 0),
  reqs_(buffer_count_, MPI_REQUEST_NULL),
  req_sources_(buffer_count_, MPI_ANY_SOURCE),
  last_source_posted_(-1),
  payload_(buffer_count_),
  my_mpi_rank_([](){ auto rank=0; MPI_Comm_rank(MPI_COMM_WORLD, &rank);return rank;}())
{

  {
    std::ostringstream debugstream;
    debugstream << "RHandles construction: "
                << "rank " << my_mpi_rank_ << ", "
		<< buffer_count << " buffers, "
		<< src_count << " sources starting at rank "
		<< src_start << '\n';
    TRACE(4, debugstream.str().c_str());
  }

  if (src_count == 0) {
    throw art::Exception(art::errors::Configuration, "RHandles: ")
      << "No sources configured.\n";
  }
  if (buffer_count == 0) {
    throw art::Exception(art::errors::Configuration, "RHandles: ")
      << "No buffers configured.\n";
  }
  // Post all the buffers.
  for (size_t i = 0; i < buffer_count_; ++i) {
    // make sure all buffers are the correct size
    payload_[i].resize(max_payload_size_);
    // Note that nextSource_() is not used here: it is not necessary to
    // check whether a source is DONE, and we avoid violating the
    // precondition of nextSource_().
    post_(i, (i % src_count_) + src_start_);
  }
}

artdaq::RHandles::
~RHandles()
{
  waitAll_();
}

size_t
artdaq::RHandles::
recvFragment(Fragment & output, size_t timeout_usec)
{
  if (!anySourceActive()) {
    return MPI_ANY_SOURCE; // Nothing to do.
  }
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

  size_t src_index(indexFromSource_(status.MPI_SOURCE));
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (which == MPI_UNDEFINED)
  { throw art::Exception(art::errors::LogicError, "RHandles: ")
      << "MPI_UNDEFINED returned as on index value from Waitany.\n"; }
  if (reqs_[which] != MPI_REQUEST_NULL)
  { throw art::Exception(art::errors::LogicError, "RHandles: ")
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
    mf::LogError("RHandles_WaitError")
      << "Waitany ERROR: " << err_buffer << "\n";
    break;
  default:
    MPI_Error_string(wait_result, err_buffer, &resultlen);
    mf::LogError("RHandles_WaitError")
      << "Waitany ERROR: " << err_buffer << "\n";
  }
  // The Fragment at index 'which' is now available.
  // Resize (down) to size to remove trailing garbage.
  TRACE( 7, "recvFragment before autoResize/swap" );
  payload_[which].autoResize();
  output.swap(payload_[which]);
  TRACE( 7, "recvFragment after autoResize/swap seqID=%lu. "
	"Reset our buffer. max=%d adr=%p"
	, output.sequenceID(), max_payload_size_, (void*)output.headerAddress() );
  // Reset our buffer.
  Fragment tmp(max_payload_size_);
  TRACE( 7, "recvFragment before payload_[which].swap(tmp) adr=%p", (void*)tmp.headerAddress() );
  payload_[which].swap(tmp);
  TRACE( 7, "recvFragment after payload_[which].swap(tmp)" );
  // Fragment accounting.
  if (output.type() == Fragment::EndOfDataFragmentType) {
    src_status_[src_index] = status_t::PENDING;
    expected_count_[src_index] = *output.dataBegin();

    {
      std::ostringstream debugstream;
      debugstream << "Received EOD from source " << status.MPI_SOURCE
		  << " (index " << src_index << ") expecting total of "
		  << *output.dataBegin() << " fragments" << '\n';
      TRACE(4, debugstream.str().c_str());
    }
  }
  else {
    recv_frag_count_.incSlot(status.MPI_SOURCE);
  }
  switch (src_status_[src_index]) {
  case status_t::PENDING:

    {
      std::ostringstream debugstream;
      debugstream << "Checking received count "
		  << recv_frag_count_.slotCount(status.MPI_SOURCE)
		  << " against expected total "
		  << expected_count_[src_index]
		  << '\n';
      TRACE(4, debugstream.str().c_str());
    }
    if (recv_frag_count_.slotCount(status.MPI_SOURCE) ==
        expected_count_[src_index]) {
      src_status_[src_index] = status_t::DONE;
    }
    break;
  case status_t::DONE:
    throw art::Exception(art::errors::LogicError, "RHandles: ")
      << "Received extra fragments from source "
      << status.MPI_SOURCE
      << ".\n";
  case status_t::SENDING:
    break;
  default:
    throw art::Exception(art::errors::LogicError, "RHandles: ")
      << "INTERNAL ERROR: Unrecognized status_t value "
      << static_cast<int>(src_status_[src_index])
      << ".\n";
  }
  // Repost to receive more data.
  if (src_status_[src_index] == status_t::DONE) { // Just happened.
    int nextSource = nextSource_();
    if (nextSource == MPI_ANY_SOURCE) { // No active sources left.
      req_sources_[which] = MPI_ANY_SOURCE; // Done with this buffer.
    }
    else { // Post for input from a still-active source.
      post_(which, nextSource); // This buffer doesn't need cancelling.
    }
    cancelAndRepost_(status.MPI_SOURCE); // Cancel and possibly repost.
  }
  else {
    post_(which, status.MPI_SOURCE);
  }
  return status.MPI_SOURCE;
}

void
artdaq::RHandles::
waitAll_()
{
  // clean up the remaining buffers
  for (size_t i = 0; i < buffer_count_; ++i) {
    if (req_sources_[i] != MPI_ANY_SOURCE) {
      cancelReq_(i, false);
    }
  }
}

int
artdaq::RHandles::
nextSource_()
{
  // Precondition: last_source_posted_ must be set. This is ensured
  // provided nextSource_() is never called from the constructor.
  int last_index = indexFromSource_(last_source_posted_);
  for (int result = (last_index + 1) % src_count_;
       result != last_index;
       result = (result + 1) % src_count_) {
    if (src_status_[result] != status_t::DONE) {
      return result + src_start_;
    }
  }
  return MPI_ANY_SOURCE;
}

void
artdaq::RHandles::
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
          mf::LogError("RHandles")
            << "Timeout waiting to cancel the request for MPI buffer "
            << buf;
        }
      }
    }
  }
  else {
    switch (result) {
    case MPI_ERR_REQUEST:
      throw art::Exception(art::errors::LogicError, "RHandles: ")
        << "MPI_Cancel returned MPI_ERR_REQUEST.\n";
    case MPI_ERR_ARG:
      throw art::Exception(art::errors::LogicError, "RHandles: ")
        << "MPI_Cancel returned MPI_ERR_ARG.\n";
    default:
      throw art::Exception(art::errors::LogicError, "RHandles: ")
        << "MPI_Cancel returned unknown error code.\n";
    }
  }
}

void
artdaq::RHandles::
post_(size_t buf, size_t src)
{

  {
    std::ostringstream debugstream;
    debugstream << "Posting buffer " << buf
		<< " size=" << payload_[buf].size()
		<< " for receive src=" << src
		<< " header address=0x" << std::hex << payload_[buf].headerAddress() << std::dec
		<< '\n';
    TRACE(4, debugstream.str().c_str());
  }
  MPI_Irecv(&*payload_[buf].headerBegin(),
            (payload_[buf].size() * sizeof(Fragment::value_type)),
            MPI_BYTE,
            src,
            MPI_ANY_TAG,
            MPI_COMM_WORLD,
            &reqs_[buf]);
  req_sources_[buf] = src;
  last_source_posted_ = src;
}

void
artdaq::RHandles::
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
        post_(i, nextSource);
      }
    }
  }
}
