#ifndef ARTDAQ_ARTDAQ_ROUTINGPOLICIES_POLICYMACROS_HH_
#define ARTDAQ_ARTDAQ_ROUTINGPOLICIES_POLICYMACROS_HH_

#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "cetlib/compiler_macros.h"

namespace fhicl {
class ParameterSet;
}  // namespace fhicl

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
// NOLINTNEXTLINE
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

// NOLINTNEXTLINE
#define DEFINE_ARTDAQ_ROUTING_POLICY(klass)                                  \
	EXTERN_C_FUNC_DECLARE_START                                              \
	std::unique_ptr<artdaq::RoutingManagerPolicy>                            \
	make(fhicl::ParameterSet const& ps)                                      \
	{                                                                        \
		return std::unique_ptr<artdaq::RoutingManagerPolicy>(new klass(ps)); \
	}                                                                        \
	}

#endif  // ARTDAQ_ARTDAQ_ROUTINGPOLICIES_POLICYMACROS_HH_
