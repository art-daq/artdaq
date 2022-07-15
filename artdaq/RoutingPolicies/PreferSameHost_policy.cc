#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_PreferSameHost_policy").c_str()
#include "TRACE/tracemf.h"

#include "artdaq/DAQdata/HostMap.hh"
#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

#include "fhiclcpp/ParameterSet.h"

#include <map>

namespace artdaq {
/**
	 * \brief A RoutingManagerPolicy which tries to keep data on the same host. For EventBuilding mode, performs RoundRobin.
	 */
class PreferSameHostPolicy : public RoutingManagerPolicy
{
public:
	/**
		 * \brief PreferSameHostPolicy Constructor
		 * \param ps ParameterSet used to configure PreferSameHostPolicy
		 * 
		 * PreferSameHostPolicy accepts the following Parameter:
		 * "minimum_participants" (Default: 0): Minimum number of receivers to distribute between. Use negative number to indicate how many can be missing from total. If the number of allowed missing receivers is greater than the number that exist, then the minimum number of participants will be set to 1.
		 */
	explicit PreferSameHostPolicy(const fhicl::ParameterSet& ps)
	    : RoutingManagerPolicy(ps)
	    , minimum_participants_(ps.get<int>("minimum_participants", 0))
	    , host_map_(MakeHostMap(ps))
	{
	}

	/**
		 * \brief Default virtual Destructor
		 */
	~PreferSameHostPolicy() override = default;

	/**
		 * \brief Generate a set of Routing Tables using received tokens
		 * \param output The RoutingPacket to add entries to
		 * 
		 * PreferSameHostPolicy will go through the list of receivers as many times
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
	PreferSameHostPolicy(PreferSameHostPolicy const&) = delete;
	PreferSameHostPolicy(PreferSameHostPolicy&&) = delete;
	PreferSameHostPolicy& operator=(PreferSameHostPolicy const&) = delete;
	PreferSameHostPolicy& operator=(PreferSameHostPolicy&&) = delete;

	std::map<int, int> sortTokens_();
	void restoreUnusedTokens_(std::map<int, int> const& sorted_tokens_);
	int calculateMinimum_();

	int minimum_participants_;
	hostMap_t host_map_;
};

void PreferSameHostPolicy::CreateRoutingTable(detail::RoutingPacket& output)
{
	TLOG(TLVL_DEBUG + 35) << "PreferSameHostPolicy::GetCurrentTable token list size is " << tokens_.size();
	auto table = sortTokens_();
	TLOG(TLVL_DEBUG + 36) << "PreferSameHostPolicy::GetCurrentTable table size is " << table.size();

	int minimum = calculateMinimum_();
	bool endCondition = table.size() < static_cast<size_t>(minimum);
	TLOG(TLVL_DEBUG + 37) << "PreferSameHostPolicy::GetCurrentTable initial endCondition is " << endCondition << ", minimum is " << minimum;

	while (!endCondition)
	{
		for (auto it = table.begin(); it != table.end();)
		{
			TLOG(TLVL_DEBUG + 38) << "PreferSameHostPolicy::GetCurrentTable assigning sequenceID " << next_sequence_id_ << " to rank " << it->first;
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
	TLOG(TLVL_DEBUG + 36) << "PreferSameHostPolicy::GetCurrentTable " << tokens_.size() << " unused tokens will be saved for later";

	TLOG(TLVL_DEBUG + 35) << "PreferSameHostPolicy::GetCurrentTable return with table size " << output.size();
}
detail::RoutingPacketEntry PreferSameHostPolicy::CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank)
{
	detail::RoutingPacketEntry output;
	auto table = sortTokens_();

	// Trivial case: no tokens
	if (table.empty()) return output;

	if (host_map_.count(requesting_rank) == 0)
	{
		TLOG(TLVL_WARNING) << "Received Routing Request from rank " << requesting_rank << ", which is not in my Host Map!";
	}
	auto host = host_map_[requesting_rank];

	// First try to find a match
	std::set<int> matching_ranks_;
	int max_rank = -1;
	int max_rank_tokens = 0;
	for (auto& entry : table)
	{
		if (entry.second == 0) continue;
		if (host_map_.count(entry.first) == 0)
		{
			TLOG(TLVL_WARNING) << "Receiver rank " << entry.first << " is not in the host map! Is this policy configured correctly?!";
		}
		else
		{
			if (host_map_[entry.first] == host)
			{
				matching_ranks_.insert(entry.first);
			}
		}
		if (entry.second > max_rank_tokens)
		{
			max_rank = entry.first;
			max_rank_tokens = entry.second;
		}
	}

	if (matching_ranks_.size() == 0)
	{
		output = detail::RoutingPacketEntry(seq, max_rank);
		table[max_rank]--;
	}
	else if (matching_ranks_.size() == 1)
	{
		output = detail::RoutingPacketEntry(seq, *matching_ranks_.begin());
		table[*matching_ranks_.begin()]--;
	}
	else
	{
		// Find the most tokens in matching_ranks_
		int max = 0;
		int max_rank = -1;
		for (auto& rank : matching_ranks_)
		{
			if (table[rank] > max)
			{
				max = table[rank];
				max_rank = rank;
			}
		}
		output = detail::RoutingPacketEntry(seq, max_rank);
		table[max_rank]--;
	}

	restoreUnusedTokens_(table);
	return output;
}
std::map<int, int> PreferSameHostPolicy::sortTokens_()
{
	auto output = std::map<int, int>();
	for (auto token : tokens_)
	{
		output[token]++;
	}
	tokens_.clear();
	return output;
}

void PreferSameHostPolicy::restoreUnusedTokens_(std::map<int, int> const& sorted_tokens)
{
	for (auto r : sorted_tokens)
	{
		for (auto i = 0; i < r.second; ++i)
		{
			tokens_.push_back(r.first);
		}
	}
}

int PreferSameHostPolicy::calculateMinimum_()
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

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::PreferSameHostPolicy)
