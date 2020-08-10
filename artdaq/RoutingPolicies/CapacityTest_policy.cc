#include <cmath>
#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"

namespace artdaq {
/**
	 * \brief A RoutingManagerPolicy which tries to fully load the first receiver, then the second, and so on
	 */
class CapacityTestPolicy : public RoutingManagerPolicy
{
public:
	/**
		 * \brief CapacityTestPolicy Constructor
		 * \param ps ParameterSet used to configure the CapacityTestPolicy
		 * 
		 * \verbatim
		 * CapacityTestPolicy accepts the following Parameters:
		 * "tokens_used_per_table_percent" (Default: 50): Percentage of available tokens to be used on each iteration.
		 * \endverbatim
		 */
	explicit CapacityTestPolicy(const fhicl::ParameterSet& ps);

	/**
		 * \brief Default virtual Destructor
		 */
	~CapacityTestPolicy() override = default;

	/**
		 * \brief Apply the policy to the current tokens
		 * \return A detail::RoutingPacket containing the Routing Table
		 * 
		 * CapacityTestPolicy will assign available tokens from the first receiver, then the second, and so on
		 * until it has assigned tokens equal to the inital_token_count * tokens_used_per_table_percent / 100.
		 * The idea is that in steady-state, the load on the receivers should reflect the workload relative to
		 * the capacity of the system. (i.e. if you have 5 receivers, and 3 of them are 100% busy, then your load
		 * factor is approximately 60%.)
		 */
	detail::RoutingPacket GetCurrentTable() override;

private:
	CapacityTestPolicy(CapacityTestPolicy const&) = delete;
	CapacityTestPolicy(CapacityTestPolicy&&) = delete;
	CapacityTestPolicy& operator=(CapacityTestPolicy const&) = delete;
	CapacityTestPolicy& operator=(CapacityTestPolicy&&) = delete;

	int tokenUsagePercent_;
};

CapacityTestPolicy::CapacityTestPolicy(const fhicl::ParameterSet& ps)
    : RoutingManagerPolicy(ps)
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
		while (table[r.first] > 0)
		{
			output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, r.first));
			table[r.first]--;
			tokensUsed++;
			if (tokensUsed >= tokensToUse)
			{
				breakCondition = true;
				break;
			}
		}
		if (breakCondition)
		{
			break;
		}
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
}  // namespace artdaq

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::CapacityTestPolicy)