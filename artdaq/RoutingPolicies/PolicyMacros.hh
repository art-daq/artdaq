#ifndef artdaq_Application_Routing_PolicyMacros_hh
#define artdaq_Application_Routing_PolicyMacros_hh

#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "cetlib/compiler_macros.h"

namespace fhicl { class ParameterSet; }

#include <memory>

namespace artdaq {
/**
* \brief Constructs a RoutingManagerPolicy instance, and returns a pointer to it
* \param ps Parameter set for initializing the RoutingManagerPolicy
* \return A smart pointer to the RoutingManagerPolicy
*/
typedef std::unique_ptr<artdaq::RoutingManagerPolicy> makeFunc_t(fhicl::ParameterSet const& ps);
}  // namespace artdaq

#ifndef EXTERN_C_FUNC_DECLARE_START
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

#define DEFINE_ARTDAQ_ROUTING_POLICY(klass)                                  \
	EXTERN_C_FUNC_DECLARE_START                                              \
	std::unique_ptr<artdaq::RoutingManagerPolicy>                            \
	make(fhicl::ParameterSet const& ps)                                      \
	{                                                                        \
		return std::unique_ptr<artdaq::RoutingManagerPolicy>(new klass(ps)); \
	}                                                                        \
	}

#endif /* artdaq_Application_Routing_PolicyMacros_hh */
