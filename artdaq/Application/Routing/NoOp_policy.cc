#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include <fhiclcpp/ParameterSet.h>

namespace artdaq
{
	class NoOpPolicy : public RoutingMasterPolicy
	{
	public:
		explicit NoOpPolicy(fhicl::ParameterSet ps) : RoutingMasterPolicy(ps) {}

		virtual ~NoOpPolicy() { }

		detail::RoutingPacket GetCurrentTable() override;
	};

	detail::RoutingPacket NoOpPolicy::GetCurrentTable()
	{
		auto tokens = getTokensSnapshot();
		detail::RoutingPacket output;
		for(auto token : *tokens.get())
		{
			output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, token));
		}

		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::NoOpPolicy)