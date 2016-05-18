
#include "artdaq/ArtModules/TransferServiceRTIDDS.h"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

//#include "art/Framework/Services/Registry/ActivityRegistry.h"

// #include "art/Utilities/Exception.h"
// #include "cetlib/container_algorithms.h"
// #include "cetlib/exception.h"
// #include "fhiclcpp/ParameterSet.h"
// #include "fhiclcpp/ParameterSetRegistry.h"
// #include "messagefacility/MessageLogger/MessageLogger.h"

// #include "TClass.h"
// #include "TBufferFile.h"

//#include "artdaq/RTIDDS/RTIDDS.hh"


void TransferServiceRTIDDS::receiveFragmentFrom(artdaq::Fragment& fragment,
						size_t receiveTimeout) {
  while (true) {

    // rtidds_ has to be a listener here...
	
	try {
	  rtidds_->octets_listener_.receiveFragmentFromDDS(fragment, receiveTimeout);
	  break;

	} catch (...) {
	  artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no,
			   "Call to octets_listener_ resulted in a timeout");
	}
  }
}

void TransferServiceRTIDDS::copyFragmentTo(bool& fragmentWasCopied,
					   bool& esrWasCopied,
					   bool& eodWasCopied,
					   artdaq::Fragment& fragment) {

  rtidds_->copyFragmentToDDS_(fragmentWasCopied,
			      esrWasCopied, eodWasCopied,
			      fragment);
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(TransferServiceRTIDDS,
                                  TransferServiceInterface)
