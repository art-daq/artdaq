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
		auto tokens = getTokensSnapshot();
		std::map<int, int> table;
		for (auto token : *tokens.get())
		{
			table[token]++;
		}
		tokens->clear();

		detail::RoutingPacket output;
		auto endCondition = table.size() < GetReceiverCount();
		while (!endCondition)
		{
			for (auto r : table)
			{
				output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, r.first));
				if(!endCondition) endCondition = r.second == 1;
				table[r.first]--;
			}
		}

		for(auto r : table)
		{
			for(auto i = 0;i < r.second; ++i)
			{
				tokens->push_back(r.first);
			}
		}
		addUnusedTokens(std::move(tokens));

		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::RoundRobinPolicy)