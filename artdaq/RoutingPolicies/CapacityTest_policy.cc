#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

#include "fhiclcpp/ParameterSet.h"

#include <cmath>

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
	 * @brief Add entries to the given RoutingPacket using currently-held tokens
	 * @param output RoutingPacket to add entries to
	 *
	 * CapacityTestPolicy will assign available tokens from the first receiver, then the second, and so on
	 * until it has assigned tokens equal to the inital_token_count * tokens_used_per_table_percent / 100.
	 * The idea is that in steady-state, the load on the receivers should reflect the workload relative to
	 * the capacity of the system. (i.e. if you have 5 receivers, and 3 of them are 100% busy, then your load
	 * factor is approximately 60%.)
	 */
	void CreateRoutingTable(detail::RoutingPacket& output) override;
	/**
	 * @brief Get an artdaq::detail::RoutingPacketEntry for a given sequence ID and rank. Used by RequestBasedEventBuilder and DataFlow RoutingManagerMode
	 * @param seq Sequence Number to get route for
	 * @param requesting_rank Rank to route for
	 * @return artdaq::detail::RoutingPacketEntry connecting sequence ID to destination rank
	 */
	detail::RoutingPacketEntry CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank) override;

private:
	std::pair<size_t, std::map<int, int>> sortTokens_();
	void restoreUnusedTokens_(std::map<int, int> const& sorted_tokens);

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

void CapacityTestPolicy::CreateRoutingTable(detail::RoutingPacket& output)
{
	auto sorted_tokens = sortTokens_();
	size_t tokenCount = sorted_tokens.first;
	auto table = sorted_tokens.second;
	size_t tokensToUse = ceil(tokenCount * tokenUsagePercent_ / 100.0);

	for (auto r : table)
	{
		bool breakCondition = false;
		while (table[r.first] > 0)
		{
			output.emplace_back(next_sequence_id_++, r.first);  // NOLINT(runtime/increment_decrement)
			table[r.first]--;
			tokens_used_since_last_update_++;
			if (tokens_used_since_last_update_ >= tokensToUse)
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

	restoreUnusedTokens_(table);
}

detail::RoutingPacketEntry CapacityTestPolicy::CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int)
{
	detail::RoutingPacketEntry output;
	auto sorted_tokens = sortTokens_();
	if (sorted_tokens.first == 0) return output;  // Trivial case: no tokens
	auto dest = sorted_tokens.second.begin()->first;
	sorted_tokens.second[dest]--;
	tokens_used_since_last_update_++;
	restoreUnusedTokens_(sorted_tokens.second);

	output = detail::RoutingPacketEntry(seq, dest);

	return output;
}

std::pair<size_t, std::map<int, int>> CapacityTestPolicy::sortTokens_()
{
	auto output = std::make_pair(0, std::map<int, int>());
	for (auto token : tokens_)
	{
		output.second[token]++;
		output.first++;
	}
	tokens_.clear();
	return output;
}

void CapacityTestPolicy::restoreUnusedTokens_(std::map<int, int> const& sorted_tokens)
{
	for (auto r : sorted_tokens)
	{
		for (auto i = 0; i < r.second; ++i)
		{
			tokens_.push_back(r.first);
		}
	}
}

}  // namespace artdaq

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::CapacityTestPolicy)
