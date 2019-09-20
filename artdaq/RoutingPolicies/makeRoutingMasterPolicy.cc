#include "artdaq/RoutingPolicies/makeRoutingMasterPolicy.hh"

#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "cetlib/BasicPluginFactory.h"
#include "fhiclcpp/ParameterSet.h"

std::shared_ptr<artdaq::RoutingMasterPolicy>
artdaq::makeRoutingMasterPolicy(std::string const& policy_plugin_spec,
                                fhicl::ParameterSet const& ps)
{
	static cet::BasicPluginFactory bpf("policy", "make");

	std::unique_ptr<artdaq::RoutingMasterPolicy> uptr =
	    bpf.makePlugin<std::unique_ptr<artdaq::RoutingMasterPolicy>,
	                   fhicl::ParameterSet const&>(policy_plugin_spec, ps);
	std::shared_ptr<artdaq::RoutingMasterPolicy> sptr(std::move(uptr));
	return sptr;
}
