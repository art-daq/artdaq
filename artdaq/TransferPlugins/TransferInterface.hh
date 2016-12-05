#ifndef artdaq_ArtModules_TransferInterface_hh
#define artdaq_ArtModules_TransferInterface_hh

#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <limits>
#include <iostream>
#include <sstream>

namespace artdaq {

class TransferInterface {
public:
  static const size_t RECV_TIMEOUT = 0xfedcba98;
  static int my_rank;

  enum class Role { kSend, kReceive };

  enum class CopyStatus { kSuccess, kTimeout, kErrorNotRequiringException };

  TransferInterface(const fhicl::ParameterSet& ps, Role role);

  TransferInterface(const TransferInterface& ) = delete;
  TransferInterface& operator=(const TransferInterface& ) = delete;
  
  virtual size_t receiveFragmentFrom(artdaq::Fragment& fragment,
				     size_t receiveTimeout) = 0;

  virtual CopyStatus copyFragmentTo(artdaq::Fragment& fragment,
			      size_t send_timeout_usec = std::numeric_limits<size_t>::max()) = 0;

  std::string uniqueLabel() const { return unique_label_; }

private:
  const Role role_;
  const int source_rank_;
  const int destination_rank_;

protected:
  Role role() const { return role_; }
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
