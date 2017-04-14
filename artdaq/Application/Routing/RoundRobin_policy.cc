#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include <fhiclcpp/ParameterSet.h>

namespace artdaq
{
	class RoundRobinPolicy : public RoutingMasterPolicy
	{
	public:
		explicit RoundRobinPolicy(fhicl::ParameterSet ps) : RoutingMasterPolicy(ps) {}

		virtual ~RoundRobinPolicy() { }

		detail::RoutingPacket GetCurrentTable() override;
	};

	detail::RoutingPacket RoundRobinPolicy::GetCurrentTable()
	{
		auto table = getTableSnapshot();
		detail::RoutingPacket output;
		auto endCondition = false;
		while(!endCondition)
		{
			for(auto r : *table)
			{
				if (r.second > 0) {
					output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, r.first));
					r.second--;
				}
				else
				{
					endCondition = true;
				}
			}
		}
		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::RoundRobinPolicy)