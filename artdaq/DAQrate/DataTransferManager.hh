#ifndef ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH
#define ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH

#include <fhiclcpp/fwd.h>

namespace artdaq {
  class DataTransferManager;
}

class artdaq::DataTransferManager {
public:

  DataTransferManager(fhicl::ParameterSet);
  ~DataTransferManager();

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

  std::unordered_map<size_t, artdaq::TransferInterface> destinations_;
  std::unordered_map<size_t, artdaq::TransferInterface> sources_;
}

#endif //ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH
