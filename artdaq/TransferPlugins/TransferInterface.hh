#ifndef artdaq_ArtModules_TransferInterface_hh
#define artdaq_ArtModules_TransferInterface_hh

#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <limits>
#include <iostream>

namespace artdaq {

class TransferInterface {
public:

  enum class Role { send, receive };

  TransferInterface(const fhicl::ParameterSet& ps, Role role) :
    role_(role),
    unique_label_(ps.get<std::string>("unique_label", "unlabeled"))
  {
    mf::LogDebug( uniqueLabel() ) << "TransferInterface constructor has " << ps.to_string();
  }

  TransferInterface(const TransferInterface& ) = delete;
  TransferInterface& operator=(const TransferInterface& ) = delete;
  
  virtual size_t receiveFragmentFrom(artdaq::Fragment& fragment,
				     size_t receiveTimeout) = 0;

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment,
			      size_t send_timeout_usec = std::numeric_limits<size_t>::max()) = 0;

  std::string uniqueLabel() const { return unique_label_; }

protected:
  Role role() const { return role_; }

private:
  const Role role_;
  const std::string unique_label_;
};

}

#define DEFINE_ARTDAQ_TRANSFER(klass)                                \
  extern "C" std::unique_ptr<artdaq::TransferInterface> make(fhicl::ParameterSet const & ps, \
							     artdaq::TransferInterface::Role role) { \
    return std::unique_ptr<artdaq::TransferInterface>(new klass(ps, role)); \
}


#endif /* artdaq_ArtModules_TransferInterface.hh */

// Local Variables:
// mode: c++
// End:
