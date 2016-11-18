
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

#define TRACE_NAME "MPI_Transfer"
#include "trace.h"		// TRACE

/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
 */

namespace artdaq {

  class MPITransfer : public TransferInterface {
public:

  MPITransfer();
  ~MPITransfer();

  // recvFragment() puts the next received fragment in frag, with the
  // source of that fragment as its return value.
  //
  // It is a precondition that a sources_sending() != 0.
  size_t recvFragment(Fragment & frag, size_t timeout_usec = 0);

  // Number of sources still not done.
  size_t sourcesActive() const;

  // Are any sources still active (faster)?
  bool anySourceActive() const;

  // Number of sources pending (last fragments still in-flight).
  size_t sourcesPending() const;

  typedef std::vector<MPI_Request> Requests;

  // buffer_count is the number of MPI_Request objects that will be used.
  // Fragments with dataSize() greater than max_payload_size will not be sent.
  // dest_count is the number of receivers used in the round-robin algorithm
  // dest_start is the rank of the first receiver
  // broadcast_sends determines whether fragments will be sent to all
  // destinations or will use the round-robin algorithm
  MPITransfer(size_t buffer_count,
           uint64_t max_payload_size,
           size_t dest_count,
           size_t dest_start,
           bool broadcast_sends = false,
           bool synchronous_sends = true);

  // How many fragments have been sent using this MPITransfer object?
  size_t count() const;

  // How many fragments have been sent to a particular destination.
  size_t slotCount(size_t rank) const;

private:
  enum class status_t { SENDING, PENDING, DONE };

  void waitAll_();

  size_t indexFromSource_(size_t src) const;

  int nextSource_();

  void cancelReq_(size_t buf, bool blocking_wait = true);
  void post_(size_t buf, size_t src);
  void cancelAndRepost_(size_t src);

  size_t buffer_count_;
  int max_payload_size_;
  int src_count_;
  int src_start_; // Start of the source ranks.
  detail::FragCounter recv_frag_count_; // Number of frags received per source.
  std::vector<status_t> src_status_; // Status of each sender.
  std::vector<size_t> expected_count_; // After EOD received: expected frags.

  std::vector<MPI_Request> reqs_; // Request to fill each buffer.
  std::vector<int> req_sources_; // Source for each request.
  int last_source_posted_;

  Fragments payload_;

  int saved_wait_result_;
  std::vector<int> ready_indices_;
  std::vector<MPI_Status> ready_statuses_;
  int my_mpi_rank_;

  // Identify an available buffer.
  size_t findAvailable();

  // Send the fragment to the specified destination.
  void sendFragTo(Fragment && frag,
                  size_t dest,
				  bool force_async = false);

  size_t const buffer_count_;
  uint64_t const max_payload_size_;
  size_t const dest_count_;
  size_t const dest_start_;
  size_t pos_; // next slot to check
  detail::FragCounter sent_frag_count_;
  bool broadcast_sends_;
  bool synchronous_sends_;

  Requests reqs_;
};

inline
size_t
artdaq::MPITransfer::
sourcesActive() const
{
  return std::count_if(src_status_.begin(),
                       src_status_.end(),
  [](status_t const & s) { return s != status_t::DONE; });
}

inline
bool
artdaq::MPITransfer::
anySourceActive() const {
  return
    std::any_of(src_status_.begin(),
                src_status_.end(),
                [](status_t const & s)
                { return s != status_t::DONE; }
               );
}

inline
size_t
artdaq::MPITransfer::
sourcesPending() const
{
  return std::count(src_status_.begin(),
                    src_status_.end(),
                    status_t::PENDING);
}

inline
size_t
artdaq::MPITransfer::
indexFromSource_(size_t src) const
{
  return src - src_start_;
}

inline
size_t
artdaq::MPITransfer::
count() const
{
  return sent_frag_count_.count();
}

inline
size_t
artdaq::MPITransfer::
slotCount(size_t rank) const
{
  return sent_frag_count_.slotCount(rank);
}


artdaq::MPITransfer::MPITransfer(size_t buffer_count,
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
    debugstream << "MPITransfer construction: "
                << "rank " << my_mpi_rank_ << ", "
		<< buffer_count << " buffers, "
		<< src_count << " sources starting at rank "
		<< src_start << '\n';
    TRACE(4, debugstream.str().c_str());
  }

  if (src_count == 0) {
    throw art::Exception(art::errors::Configuration, "MPITransfer: ")
      << "No sources configured.\n";
  }
  if (buffer_count == 0) {
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
    post_(i, (i % src_count_) + src_start_);
  }
}

artdaq::MPITransfer::
~MPITransfer()
{
  waitAll_();
}

size_t
artdaq::MPITransfer::
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
    throw art::Exception(art::errors::LogicError, "MPITransfer: ")
      << "Received extra fragments from source "
      << status.MPI_SOURCE
      << ".\n";
  case status_t::SENDING:
    break;
  default:
    throw art::Exception(art::errors::LogicError, "MPITransfer: ")
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
artdaq::MPITransfer::
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
artdaq::MPITransfer::
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
        post_(i, nextSource);
      }
    }
  }
}


artdaq::MPITransfer::MPITransfer(size_t buffer_count,
                           uint64_t max_payload_size,
                           size_t dest_count,
                           size_t dest_start,
			   bool broadcast_sends,
                           bool synchronous_sends)
  :
  buffer_count_(buffer_count),
  max_payload_size_(max_payload_size),
  dest_count_(dest_count),
  dest_start_(dest_start),
  pos_(),
  sent_frag_count_(dest_count, dest_start),
  broadcast_sends_(broadcast_sends),
  synchronous_sends_(synchronous_sends),
  reqs_(buffer_count_, MPI_REQUEST_NULL),
  payload_(buffer_count_),
  my_mpi_rank_([](){ auto rank=0; MPI_Comm_rank(MPI_COMM_WORLD, &rank);return rank;}())
{
    std::ostringstream debugstream;
    debugstream << "MPITransfer construction: "
                << "rank " << my_mpi_rank_ << ", "
		<< buffer_count << " buffers, "
		<< dest_count << " destination starting at rank "
		<< dest_start << '\n';
    TRACE(4, debugstream.str().c_str());
}


size_t artdaq::MPITransfer::findAvailable()
{
  size_t use_me = 0;
  int flag;
  int loops=0;
  TRACE(5, "findAvailable initial pos_=%zu", pos_);
  do {
    use_me = pos_;
    MPI_Test(&reqs_[use_me], &flag, MPI_STATUS_IGNORE);
    pos_ = (pos_ + 1) % buffer_count_;
	++loops;
  }
  while (!flag);
  TRACE(5, "findAvailable returning use_me=%zu loops=%d", use_me, loops );
  // pos_ is pointing at the next slot to check
  // use_me is pointing at the slot to use
  return use_me;
}



void artdaq::MPITransfer::waitAll()
{
  MPI_Waitall(buffer_count_, &reqs_[0], MPI_STATUSES_IGNORE);
}

void
artdaq::MPITransfer::
sendFragTo(Fragment && frag, size_t dest, bool force_async)
{
  if (frag.dataSize() > max_payload_size_) {
    throw cet::exception("Unimplemented")
        << "Currently unable to deal with overlarge fragment payload ("
        << frag.dataSize()
        << " words > "
        << max_payload_size_
        << ").";
  }
  size_t buffer_idx = findAvailable();
  Fragment & curfrag = payload_[buffer_idx];
  curfrag = std::move(frag);
  TRACE( 5, "sendFragTo before send src=%i dest=%lu seqID=%lu found_idx=%zu"
        , my_mpi_rank_ , dest, curfrag.sequenceID(), buffer_idx );
  if (! synchronous_sends_ || force_async) {
    // 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
    MPI_Isend(&*curfrag.headerBegin(),
              curfrag.size() * sizeof(Fragment::value_type),
              MPI_BYTE,
              dest,
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
    MPI_Ssend(&*curfrag.headerBegin(),
              curfrag.size() * sizeof(Fragment::value_type),
              MPI_BYTE,
              dest,
              MPITag::FINAL,
              MPI_COMM_WORLD );
  }
  TRACE( 5, "sendFragTo COMPLETE" );
  
  {
    std::ostringstream debugstream;
    debugstream << "send COMPLETE: "
		<< " buffer_idx=" << buffer_idx
		<< " send_size=" << curfrag.size()
		<< " src=" << my_mpi_rank_
		<< " dest=" << dest
		<< " sequenceID=" << curfrag.sequenceID()
		<< " fragID=" << curfrag.fragmentID()
		<< '\n';
    TRACE(11, debugstream.str().c_str());
  }
}
