#ifndef artdaq_Application_Routing_PolicyMacros_hh
#define artdaq_Application_Routing_PolicyMacros_hh

#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "fhiclcpp/fwd.h"

#include <memory>

namespace artdaq
{
	typedef std::unique_ptr<artdaq::RoutingMasterPolicy>
		(makeFunc_t)(fhicl::ParameterSet const& ps);
}

#define DEFINE_ARTDAQ_ROUTING_POLICY(klass)                    \
  extern "C"                                                          \
  std::unique_ptr<artdaq::RoutingMasterPolicy>               \
  make(fhicl::ParameterSet const & ps) {                              \
	return std::unique_ptr<artdaq::RoutingMasterPolicy>(new klass(ps)); \
  }

#endif /* artdaq_Application_Routing_PolicyMacros_hh */
