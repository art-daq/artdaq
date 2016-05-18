#ifndef artdaq_ArtModules_TransferServiceRTIDDS_h
#define artdaq_ArtModules_TransferServiceRTIDDS_h

#include "artdaq/ArtModules/TransferServiceInterface.h"
#include "artdaq/RTIDDS/RTIDDS.hh"

#include "art/Framework/Services/Registry/ServiceMacros.h"

#include <memory>

//#include <ndds/ndds_cpp.h>

// Why are these namespace declarations necessary?
namespace art {
class ActivityRegistry;
}

namespace fhicl {
class ParameterSet;
}

// ----------------------------------------------------------------------

class TransferServiceRTIDDS : public TransferServiceInterface {

public:
  ~TransferServiceRTIDDS() = default;
  TransferServiceRTIDDS(fhicl::ParameterSet const&, art::ActivityRegistry&) :
    rtidds_(std::make_unique<artdaq::RTIDDS>("TransferServiceRTIDDS"))
  {}

  virtual void receiveFragmentFrom(artdaq::Fragment& fragment,
				   size_t receiveTimeout);

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment);
private:
  
  std::unique_ptr<artdaq::RTIDDS> rtidds_;

};

DECLARE_ART_SERVICE_INTERFACE_IMPL(TransferServiceRTIDDS, TransferServiceInterface, LEGACY)
#endif /* artdaq_ArtModules_TransferServiceRTIDDS_h */

// Local Variables:
// mode: c++
// End:
