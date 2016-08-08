#ifndef artdaq_DAQrate_SHandles_hh
#define artdaq_DAQrate_SHandles_hh

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/DAQrate/MPITag.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"

#include <vector>

#include "artdaq/DAQrate/quiet_mpi.hh"

// SHandles handles the transmission of Fragments to one or more receivers in a
// round-robin fashion. Multiple receivers must have consecutive rank numbers.

namespace artdaq {
  class SHandles;
}

class artdaq::SHandles {
public:
  typedef std::vector<MPI_Request> Requests;

  // buffer_count is the number of MPI_Request objects that will be used.
  // Fragments with dataSize() greater than max_payload_size will not be sent.
  // dest_count is the number of receivers used in the round-robin algorithm
  // dest_start is the rank of the first receiver
  // broadcast_sends determines whether fragments will be sent to all
  // destinations or will use the round-robin algorithm
  SHandles(size_t buffer_count,
           uint64_t max_payload_size,
           size_t dest_count,
           size_t dest_start,
           bool broadcast_sends = false,
           bool synchronous_sends = true);

  // Make sure we clean up and wait for in-flight sends.
  ~SHandles();

  // Send the given Fragment. Return the rank of the destination to which
  // the Fragment was sent.
  size_t sendFragment(Fragment &&);

  // How many fragments have been sent using this SHandles object?
  size_t count() const;

  // How many fragments have been sent to a particular destination.
  size_t slotCount(size_t rank) const;

  // Wait for all the data transfers scheduled by calls
  // to MPI_Isend to finish, then return.
  void waitAll();

private:
  // Send an EOF Fragment to the receiver at rank dest;
  // the EOF Fragment will report that numFragmentsSent
  // fragments have been sent before this one.
  void sendEODFrag(size_t dest, size_t numFragmentsSent);

  // Calculate where the fragment with this sequenceID should go.
  size_t calcDest(Fragment::sequence_id_t) const;

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
  Fragments payload_; 
  int  my_mpi_rank_;
};

inline
size_t
artdaq::SHandles::
count() const
{
  return sent_frag_count_.count();
}

inline
size_t
artdaq::SHandles::
slotCount(size_t rank) const
{
  return sent_frag_count_.slotCount(rank);
}

#endif /* artdaq_DAQrate_SHandles_hh */
