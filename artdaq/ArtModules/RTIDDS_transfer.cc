#ifndef artdaq_ArtModules_RTIDDSTransfer_h
#define artdaq_ArtModules_RTIDDSTransfer_h

#include "artdaq/ArtModules/TransferInterface.h"
#include "artdaq/RTIDDS/RTIDDS.hh"

#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "messagefacility/MessageLogger/MessageLogger.h"

#include <memory>
#include <iostream>


namespace fhicl {
class ParameterSet;
}

// ----------------------------------------------------------------------

namespace artdaq {

class RTIDDSTransfer : public TransferInterface {

public:
  ~RTIDDSTransfer() = default;
  RTIDDSTransfer(fhicl::ParameterSet const& ps, Role role) :
    TransferInterface(ps, role),
    rtidds_reader_(std::make_unique<artdaq::RTIDDS>("RTIDDSTransfer_reader", artdaq::RTIDDS::IOType::reader)),
    rtidds_writer_(std::make_unique<artdaq::RTIDDS>("RTIDDSTransfer_writer", artdaq::RTIDDS::IOType::writer))
  {
  }

  virtual void receiveFragmentFrom(artdaq::Fragment& fragment,
				   size_t receiveTimeout);

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment);
private:

  std::unique_ptr<artdaq::RTIDDS> rtidds_reader_;
  std::unique_ptr<artdaq::RTIDDS> rtidds_writer_;

};

}

void artdaq::RTIDDSTransfer::receiveFragmentFrom(artdaq::Fragment& fragment,
						size_t receiveTimeout) {

  bool receivedFragment = false;
  static std::size_t consecutive_timeouts = 0;
  std::size_t message_after_N_timeouts = 10;

  while (!receivedFragment) {
        
    try {
      receivedFragment = rtidds_reader_->octets_listener_.receiveFragmentFromDDS(fragment, receiveTimeout);
    } catch (...) {
      ExceptionHandler(ExceptionHandlerRethrow::yes, 
		       "Error in RTIDDS transfer plugin: caught exception in call to OctetsListener::receiveFragmentFromDDS, rethrowing");
    }

    if (!receivedFragment) {
 
      consecutive_timeouts++;

      if (consecutive_timeouts % message_after_N_timeouts == 0) {
	mf::LogInfo("RTIDDSTransfer") << consecutive_timeouts << " consecutive " << 
	  static_cast<float>(receiveTimeout)/1e6 << "-second timeouts calling OctetsListener::receiveFragmentFromDDS, will continue trying...";
      }
    } else {
      consecutive_timeouts = 0;
    }
  }

}

void artdaq::RTIDDSTransfer::copyFragmentTo(bool& fragmentWasCopied,
					   bool& esrWasCopied,
					   bool& eodWasCopied,
					   artdaq::Fragment& fragment) {

  rtidds_writer_->copyFragmentToDDS_(fragmentWasCopied,
                                     esrWasCopied, eodWasCopied,
                                     fragment);
}

DEFINE_ARTDAQ_TRANSFER(artdaq::RTIDDSTransfer)

#endif /* artdaq_ArtModules_RTIDDSTransfer_h */

// Local Variables:
// mode: c++
// End:
