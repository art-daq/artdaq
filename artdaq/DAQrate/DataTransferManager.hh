#ifndef ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH
#define ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH

#include <unordered_map>
#include <memory>

#include <fhiclcpp/fwd.h>

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"

namespace artdaq {
  class DataTransferManager;
}

class artdaq::DataTransferManager {
public:

  DataTransferManager(fhicl::ParameterSet);
  ~DataTransferManager();

  // Send the given Fragment. Return the rank of the destination to which
  // the Fragment was sent.
  size_t sendFragment(Fragment &&);

  // recvFragment() puts the next received fragment in frag, with the
  // source of that fragment as its return value.
  //
  // It is a precondition that a sources_sending() != 0.
  size_t recvFragment(Fragment & frag, size_t timeout_usec = 0);

  // How many fragments have been sent using this DataTransferManager object?
  size_t count() const;

  // How many fragments have been sent to a particular destination.
  size_t slotCount(size_t rank) const;

size_t destinationCount() const { return destinations_.size(); }
private:
  // Send an EOF Fragment to the receiver at rank dest;
  // the EOF Fragment will report that numFragmentsSent
  // fragments have been sent before this one.
  void sendEODFrag(size_t dest, size_t numFragmentsSent);

  // Calculate where the fragment with this sequenceID should go.
size_t calcDest(Fragment::sequence_id_t) const;

private:

std::unordered_map<size_t, std::unique_ptr<artdaq::TransferInterface>> destinations_;
  std::unordered_map<size_t, std::unique_ptr<artdaq::TransferInterface>> sources_;
  size_t current_source_;

  detail::FragCounter recv_frag_count_; // Number of frags received per source.
  detail::FragCounter sent_frag_count_;

  bool broadcast_sends_;
};

inline
size_t
artdaq::DataTransferManager::
count() const
{
  return sent_frag_count_.count();
}

inline
size_t
artdaq::DataTransferManager::
slotCount(size_t rank) const
{
  return sent_frag_count_.slotCount(rank);
}
#endif //ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH
