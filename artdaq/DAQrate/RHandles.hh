#ifndef artdaq_DAQrate_RHandles_hh
#define artdaq_DAQrate_RHandles_hh

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"

#include <algorithm>
#include <vector>

#include "artdaq/DAQrate/quiet_mpi.hh"

/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
 */

namespace artdaq {
  class RHandles;
}

class artdaq::RHandles {
public:
  static const size_t RECV_TIMEOUT;

  RHandles(size_t buffer_count,
           uint64_t max_payload_size,
           size_t src_count,
           size_t src_start);
  ~RHandles();

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
};

inline
size_t
artdaq::RHandles::
sourcesActive() const
{
  return std::count_if(src_status_.begin(),
                       src_status_.end(),
  [](status_t const & s) { return s != status_t::DONE; });
}

inline
bool
artdaq::RHandles::
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
artdaq::RHandles::
sourcesPending() const
{
  return std::count(src_status_.begin(),
                    src_status_.end(),
                    status_t::PENDING);
}

inline
size_t
artdaq::RHandles::
indexFromSource_(size_t src) const
{
  return src - src_start_;
}

#endif /* artdaq_DAQrate_RHandles_hh */
