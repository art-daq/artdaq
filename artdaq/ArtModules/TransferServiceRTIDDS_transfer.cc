#ifndef artdaq_ArtModules_TransferServiceRTIDDS_h
#define artdaq_ArtModules_TransferServiceRTIDDS_h

#include "artdaq/ArtModules/TransferServiceInterface.h"
#include "artdaq/RTIDDS/RTIDDS.hh"

#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include <iostream>

#include <memory>
#include <iostream>


namespace fhicl {
class ParameterSet;
}

// ----------------------------------------------------------------------

class TransferServiceRTIDDS : public TransferServiceInterface {

public:
  ~TransferServiceRTIDDS() = default;
  TransferServiceRTIDDS(fhicl::ParameterSet const&) :
    rtidds_reader_(std::make_unique<artdaq::RTIDDS>("TransferServiceRTIDDS_reader", artdaq::RTIDDS::IOType::reader)),
    rtidds_writer_(std::make_unique<artdaq::RTIDDS>("TransferServiceRTIDDS_writer", artdaq::RTIDDS::IOType::writer))
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


void TransferServiceRTIDDS::receiveFragmentFrom(artdaq::Fragment& fragment,
						size_t receiveTimeout) {

  while (true) {
        
    try {
      rtidds_reader_->octets_listener_.receiveFragmentFromDDS(fragment, receiveTimeout);
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

  rtidds_writer_->copyFragmentToDDS_(fragmentWasCopied,
                                     esrWasCopied, eodWasCopied,
                                     fragment);
}

// JCF, May-20-2016
// Will probably turn this into a macro, usable by other types of TransferService plugins

extern "C"							  \
std::unique_ptr<TransferServiceInterface>				      \
make(fhicl::ParameterSet const & ps) {					\
  return std::unique_ptr<TransferServiceInterface>(new TransferServiceRTIDDS(ps)); \
}



#endif /* artdaq_ArtModules_TransferServiceRTIDDS_h */

// Local Variables:
// mode: c++
// End:
