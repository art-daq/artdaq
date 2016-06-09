#ifndef artdaq_ArtModules_TransferInterface_h
#define artdaq_ArtModules_TransferInterface_h

#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/fwd.h"

namespace fhicl {
  class ParameterSet;
}

namespace artdaq {

class TransferInterface {
public:

  enum class Role { send, receive };

  TransferInterface(const fhicl::ParameterSet& , Role role) :
    role_(role)
  {}
  
  virtual void receiveFragmentFrom(artdaq::Fragment& fragment,
			   size_t receiveTimeout) = 0;

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment) = 0;

private:
  Role role_;
};

}

#define DEFINE_ARTDAQ_TRANSFER(klass)                                \
  extern "C" std::unique_ptr<artdaq::TransferInterface> make(fhicl::ParameterSet const & ps, \
							     artdaq::TransferInterface::Role role) { \
    return std::unique_ptr<artdaq::TransferInterface>(new klass(ps, role)); \
}


#endif /* artdaq_ArtModules_TransferInterface_h */

// Local Variables:
// mode: c++
// End:
