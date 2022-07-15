#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RoundRobin_policy").c_str()
#include "TRACE/tracemf.h"

#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

#include "fhiclcpp/ParameterSet.h"

namespace artdaq {
/**
 * \brief A RoutingManagerPolicy which evenly distributes Sequence IDs to all receivers.
 * If an uneven number of tokens have been received, extra tokens are stored for the next table update.
 */
class RoundRobinPolicy : public RoutingManagerPolicy
{
public:
	/**
	 * \brief RoundRobinPolicy Constructor
	 * \param ps ParameterSet used to configure RoundRobinPolicy
	 *
	 * RoundRobinPolicy accepts the following Parameter:
	 * "minimum_participants" (Default: 0): Minimum number of receivers to distribute between. Use negative number to indicate how many can be missing from total. If the number of allowed missing receivers is greater than the number that exist, then the minimum number of participants will be set to 1.
	 */
	explicit RoundRobinPolicy(const fhicl::ParameterSet& ps)
	    : RoutingManagerPolicy(ps)
	    , minimum_participants_(ps.get<int>("minimum_participants", 0))
	{
	}

	/**
	 * \brief Default virtual Destructor
	 */
	~RoundRobinPolicy() override = default;

	/**
	 * @brief Add entries to the given RoutingPacket using currently-held tokens
	 * @param output RoutingPacket to add entries to
	 *
	 * RoundRobinPolicy will go through the list of receivers as many times
	 * as it can, until one or more receivers have no tokens. It always does full
	 * "turns" through the recevier list.
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
	RoundRobinPolicy(RoundRobinPolicy const&) = delete;
	RoundRobinPolicy(RoundRobinPolicy&&) = delete;
	RoundRobinPolicy& operator=(RoundRobinPolicy const&) = delete;
	RoundRobinPolicy& operator=(RoundRobinPolicy&&) = delete;

	std::map<int, int> sortTokens_();
	void restoreUnusedTokens_(std::map<int, int> const& sorted_tokens_);
	int calculateMinimum_();

	int minimum_participants_;
	std::set<int> receivers_in_current_round_;
};

void RoundRobinPolicy::CreateRoutingTable(detail::RoutingPacket& output)
{
	TLOG(TLVL_DEBUG + 35) << "RoundRobinPolicy::GetCurrentTable token list size is " << tokens_.size();
	auto table = sortTokens_();
	TLOG(TLVL_DEBUG + 36) << "RoundRobinPolicy::GetCurrentTable table size is " << table.size();

	int minimum = calculateMinimum_();
	bool endCondition = table.size() < static_cast<size_t>(minimum);
	TLOG(TLVL_DEBUG + 37) << "RoundRobinPolicy::GetCurrentTable initial endCondition is " << endCondition << ", minimum is " << minimum;

	while (!endCondition)
	{
		for (auto it = table.begin(); it != table.end();)
		{
			TLOG(TLVL_DEBUG + 38) << "RoundRobinPolicy::GetCurrentTable assigning sequenceID " << next_sequence_id_ << " to rank " << it->first;
			output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, it->first));
			table[it->first]--;

			if (table[it->first] <= 0)
			{
				it = table.erase(it);
			}
			else
			{
				++it;
			}
		}
		endCondition = table.size() < static_cast<size_t>(minimum);
	}

	restoreUnusedTokens_(table);
	TLOG(TLVL_DEBUG + 36) << "RoundRobinPolicy::GetCurrentTable " << tokens_.size() << " unused tokens will be saved for later";

	TLOG(TLVL_DEBUG + 35) << "RoundRobinPolicy::GetCurrentTable return with table size " << output.size();
}
detail::RoutingPacketEntry RoundRobinPolicy::CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int)
{
	detail::RoutingPacketEntry output;

	// Trivial case: We've already started a round
	if (!receivers_in_current_round_.empty())
	{
		output = detail::RoutingPacketEntry(seq, *receivers_in_current_round_.begin());
		receivers_in_current_round_.erase(receivers_in_current_round_.begin());
		return output;
	}

	// We need to set up the next round...
	auto table = sortTokens_();

	// Can't route anything until a full "turn" is available
	int minimum = calculateMinimum_();
	if (table.size() < static_cast<size_t>(minimum))
	{
		TLOG(TLVL_WARNING) << "Do not have tokens from a minimum set of receivers to start a round";
	}
	else
	{
		for (auto& entry : table)
		{
			receivers_in_current_round_.insert(entry.first);
			entry.second--;
		}
		output = detail::RoutingPacketEntry(seq, *receivers_in_current_round_.begin());
		receivers_in_current_round_.erase(receivers_in_current_round_.begin());
	}

	restoreUnusedTokens_(table);
	return output;
}
std::map<int, int> RoundRobinPolicy::sortTokens_()
{
	auto output = std::map<int, int>();
	for (auto token : tokens_)
	{
		output[token]++;
	}
	tokens_.clear();
	return output;
}

void RoundRobinPolicy::restoreUnusedTokens_(std::map<int, int> const& sorted_tokens)
{
	for (auto r : sorted_tokens)
	{
		for (auto i = 0; i < r.second; ++i)
		{
			tokens_.push_back(r.first);
		}
	}
}

int RoundRobinPolicy::calculateMinimum_()
{
	// If 0 or negative, add minimum_participants_ to GetRecevierCount to ensure that it's correct
	// 02-Apr-2019, KAB: changed the declared type of "minimum" from 'auto' to 'int' to avoid the
	// situation in which the compiler chooses a type of 'unsigned int', the minimum_participants_ is
	// a negative number that is larger (in absolute value) to the receiver count, and "minimum"
	// ends up with a large positive value.
	int minimum = minimum_participants_ > 0 ? minimum_participants_ : GetReceiverCount() + minimum_participants_;
	if (minimum < 1)
	{
		minimum = 1;  // Can't go below 1
	}
	return minimum;
}

}  // namespace artdaq

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::RoundRobinPolicy)
