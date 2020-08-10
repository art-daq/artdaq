#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"

#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "cetlib/BasicPluginFactory.h"
#include "fhiclcpp/ParameterSet.h"

std::shared_ptr<artdaq::RoutingManagerPolicy>
artdaq::makeRoutingManagerPolicy(std::string const& policy_plugin_spec,
                                fhicl::ParameterSet const& ps)
{
	static cet::BasicPluginFactory bpf("policy", "make");

	std::unique_ptr<artdaq::RoutingManagerPolicy> uptr =
	    bpf.makePlugin<std::unique_ptr<artdaq::RoutingManagerPolicy>,
	                   fhicl::ParameterSet const&>(policy_plugin_spec, ps);
	std::shared_ptr<artdaq::RoutingManagerPolicy> sptr(std::move(uptr));
	return sptr;
}
