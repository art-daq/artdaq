#ifndef artdaq_Application_Routing_makeRoutingManagerPolicy_hh
#define artdaq_Application_Routing_makeRoutingManagerPolicy_hh
// Using LibraryManager, find the correct library and return an instance
// of the specified generator.

#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "fhiclcpp/fwd.h"

#include <memory>
#include <string>

namespace artdaq {
/**
	 * \brief Load a RoutingManagerPolicy plugin
	 * \param policy_plugin_spec Name of the RoutingManagerPolicy
	 * \param ps ParameterSet used to configure the RoutingManagerPolicy
	 * \return std::shared_ptr<RoutingManagerPolicy> to the new RoutingManagerPolicy instance
	 */
std::shared_ptr<RoutingManagerPolicy>
makeRoutingManagerPolicy(std::string const& policy_plugin_spec,
                        fhicl::ParameterSet const& ps);
}  // namespace artdaq
#endif /* artdaq_Application_Routing_makeRoutingManagerPolicy_hh */
