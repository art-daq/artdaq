
#include <algorithm>
#include <vector>

#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "cetlib/container_algorithms.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"

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

  typedef std::vector<MPI_Request> Requests;

	MPITransfer(fhicl::ParameterSet);
  ~MPITransfer();

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

  // Identify an available buffer.
  size_t findAvailable();

  // Send the fragment to the specified destination.
  void sendFragTo(Fragment && frag,
                  size_t dest,
				  bool force_async = false);

	size_t destination_;
	size_t source_;
  size_t buffer_count_;
  int max_payload_size_;
  std::vector<status_t> src_status_; // Status of each sender.
  std::vector<size_t> expected_count_; // After EOD received: expected frags.

  std::vector<int> req_sources_; // Source for each request.
  int last_source_posted_;

  Fragments payload_;

  int saved_wait_result_;
  std::vector<int> ready_indices_;
  std::vector<MPI_Status> ready_statuses_;
  int my_mpi_rank_;

  size_t pos_; // next slot to check
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

