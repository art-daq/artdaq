#ifndef artdaq_Application_Routing_makeRoutingMasterPolicy_hh
#define artdaq_Application_Routing_makeRoutingMasterPolicy_hh
// Using LibraryManager, find the correct library and return an instance
// of the specified generator.

#include "artdaq/RoutingPolicies/RoutingMasterPolicy.hh"
#include "fhiclcpp/fwd.h"

#include <memory>
#include <string>

namespace artdaq {
/**
	 * \brief Load a RoutingMasterPolicy plugin
	 * \param policy_plugin_spec Name of the RoutingMasterPolicy
	 * \param ps ParameterSet used to configure the RoutingMasterPolicy
	 * \return std::shared_ptr<RoutingMasterPolicy> to the new RoutingMasterPolicy instance
	 */
std::shared_ptr<RoutingMasterPolicy>
makeRoutingMasterPolicy(std::string const& policy_plugin_spec,
                        fhicl::ParameterSet const& ps);
}  // namespace artdaq
#endif /* artdaq_Application_Routing_makeRoutingMasterPolicy_hh */
