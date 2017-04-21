#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include <fhiclcpp/ParameterSet.h>
#include <cmath>

namespace artdaq
{
	class CapacityTestPolicy : public RoutingMasterPolicy
	{
	public:
		explicit CapacityTestPolicy(fhicl::ParameterSet ps);

		virtual ~CapacityTestPolicy() { }

		detail::RoutingPacket GetCurrentTable() override;
	private:
		int tokenUsagePercent_;
	};

	CapacityTestPolicy::CapacityTestPolicy(fhicl::ParameterSet ps)
		: RoutingMasterPolicy(ps)
		, tokenUsagePercent_(ps.get<int>("tokens_used_per_table_percent", 50))
	{}

	detail::RoutingPacket CapacityTestPolicy::GetCurrentTable()
	{
		auto tokens = getTokensSnapshot();
		std::map<int, int> table;
		auto tokenCount = 0;
		for (auto token : *tokens)
		{
			table[token]++;
			tokenCount++;
		}
		tokens->clear();

		int tokensToUse = ceil(tokenCount * tokenUsagePercent_ / 100.0);
		auto tokensUsed = 0;

		detail::RoutingPacket output;
		for (auto r : table)
		{
			bool breakCondition = false;
			while (table[r.first] > 0) {
				output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, r.first));
				table[r.first]--;
				tokensUsed++;
				if(tokensUsed >= tokensToUse)
				{
					breakCondition = true;
					break;
				}
			}
			if (breakCondition) break;
		}

		for (auto r : table)
		{
			for (auto i = 0; i < r.second; ++i)
			{
				tokens->push_back(r.first);
			}
		}
		addUnusedTokens(std::move(tokens));

		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::CapacityTestPolicy)