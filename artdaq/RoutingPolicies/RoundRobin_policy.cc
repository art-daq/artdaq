#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RoundRobin_policy").c_str()

#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

#include "fhiclcpp/ParameterSet.h"
#include "tracemf.h"

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
		last_receiver_routed_ = *receiver_ranks_.begin();
	}

	/**
		 * \brief Default virtual Destructor
		 */
	~RoundRobinPolicy() override = default;

	/**
		 * \brief Generate a set of Routing Tables using received tokens
		 * \return A map<int, detail::RoutingPacket> containing the Routing Tables indexed by sender rank
		 * 
		 * RoundRobinPolicy will go through the list of receivers as many times
		 * as it can, until one or more receivers have no tokens. It always does full
		 * "turns" through the recevier list.
		 */
	void CreateRoutingTable(detail::RoutingPacket& output) override;
	detail::RoutingPacketEntry CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank) override;

private:
	RoundRobinPolicy(RoundRobinPolicy const&) = delete;
	RoundRobinPolicy(RoundRobinPolicy&&) = delete;
	RoundRobinPolicy& operator=(RoundRobinPolicy const&) = delete;
	RoundRobinPolicy& operator=(RoundRobinPolicy&&) = delete;

	std::map<int, int> sortTokens_();
	void restoreUnusedTokens_(std::map<int, int>const& sorted_tokens_);
	int calculateMinimum_();

	int minimum_participants_;
	int last_receiver_routed_;
};

void RoundRobinPolicy::CreateRoutingTable(detail::RoutingPacket& output)
{
	TLOG(12) << "RoundRobinPolicy::GetCurrentTable token list size is " << tokens_.size();
	auto table = sortTokens_();
	TLOG(13) << "RoundRobinPolicy::GetCurrentTable table size is " << table.size();

	int minimum = calculateMinimum_();
	bool endCondition = table.size() < static_cast<size_t>(minimum);
	TLOG(15) << "RoundRobinPolicy::GetCurrentTable initial endCondition is " << endCondition << ", minimum is " << minimum;

	while (!endCondition)
	{
		for (auto it = table.begin(); it != table.end();)
		{
			TLOG(16) << "RoundRobinPolicy::GetCurrentTable assigning sequenceID " << next_sequence_id_ << " to rank " << it->first;
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
	TLOG(13) << "RoundRobinPolicy::GetCurrentTable " << tokens_.size() << " unused tokens will be saved for later";

	TLOG(12) << "RoundRobinPolicy::GetCurrentTable return with table size " << output.size();
}
detail::RoutingPacketEntry RoundRobinPolicy::CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int)
{
	auto table = sortTokens_();
	// Trivial case: no tokens
	if (table.empty()) { return detail::RoutingPacketEntry(); }
	auto it = table.find(last_receiver_routed_);
	if (it == table.end())
	{
		TLOG(TLVL_WARNING) << "RoundRobinPolicy::CreateRouteForSequenceID: Could not find token for last receiver routed, resetting to beginning of \"turn\". seq=" << seq;
	}
	else
	{
		++it;
	}

	if (it == table.end()) {
		it = table.begin();
		int minimum = calculateMinimum_();
		if (table.size() < static_cast<size_t>(minimum)) {
			return detail::RoutingPacketEntry();
		}
	}

	auto output = detail::RoutingPacketEntry(seq, it->first);
	last_receiver_routed_ = it->first;
	table[it->first]--;

	restoreUnusedTokens_(table);
	return output;
}
std::map<int, int> RoundRobinPolicy::sortTokens_()
{
	auto output =  std::map<int, int>();
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
	if (minimum > static_cast<int>(GetReceiverCount()))
	{
		minimum = GetReceiverCount();  // Can't go above receiver count
	}
	return minimum;
}

}  // namespace artdaq

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::RoundRobinPolicy)
