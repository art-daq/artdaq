#ifndef artdaq_Application_Routing_RoutingManagerPolicy_hh
#define artdaq_Application_Routing_RoutingManagerPolicy_hh

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"

#include <deque>
#include <mutex>
#include <unordered_set>
#include "fhiclcpp/fwd.h"

namespace artdaq {
/**
	 * \brief The interface through which RoutingManagerCore obtains Routing Tables using received Routing Tokens
	 */
class RoutingManagerPolicy
{
public:
	/**
		 * \brief RoutingManagerPolicy Constructor
		 * \param ps ParameterSet used to configure the RoutingManagerPolicy
		 * 
		 * \verbatim
		 * RoutingManagerPolicy accepts the following Parameters:
		 * "receiver_ranks" (REQUIRED): A list of integers indicating the ranks that the RoutingManagerPolicy should expect tokens from
		 * \endverbatim
		 */
	explicit RoutingManagerPolicy(const fhicl::ParameterSet& ps);
	/**
		 * \brief Default virtual Destructor
		 */
	virtual ~RoutingManagerPolicy() = default;

	/**
		 * \brief Get the number of configured receivers
		 * \return The size of the receiver_ranks list
		 */
	size_t GetReceiverCount() const { return receiver_ranks_.size(); }

	/**
		 * \brief Get the largest number of tokens that the RoutingManagerPolicy has seen at any one time
		 * \return The largest number of tokens that the RoutingManagerPolicy has seen at any one time
		 */
	size_t GetMaxNumberOfTokens() const { return max_token_count_; }

	size_t GetTokensUsedSinceLastUpdate() const { return tokens_used_since_last_update_; }
	void ResetTokensUsedSinceLastUpdate() { tokens_used_since_last_update_ = 0; }

	/**
		 * \brief Add a token to the token list
		 * \param rank Rank that the token is from
		 * \param new_slots_free Number of slots that are now free (should usually be 1)
		 */
	void AddReceiverToken(int rank, unsigned new_slots_free);

	/**
		 * \brief Reset the policy, setting the next sequence ID to be used to 1, and removing any tokens
		 */
	void Reset();

	// EventBuilder and RequestBasedEventBuilder modes
	detail::RoutingPacket GetCurrentTable();

	// RequestBasedEventBuilder and DataFlow modes
	detail::RoutingPacketEntry GetRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank);

	detail::RoutingManagerMode GetRoutingMode() const { return routing_mode_; }

protected:


	// Building Tables
	virtual void CreateRoutingTable(detail::RoutingPacket& tables) = 0;

	// Route by Request
	virtual detail::RoutingPacketEntry CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank) = 0;


	// Tokens
	std::deque<int> tokens_;
	std::atomic<size_t> tokens_used_since_last_update_;

	// Routing Information
	Fragment::sequence_id_t next_sequence_id_;  ///< The next sequence ID to be assigned
	std::unordered_set<int> receiver_ranks_;
	detail::RoutingManagerMode routing_mode_;


private:
	RoutingManagerPolicy(RoutingManagerPolicy const&) = delete;
	RoutingManagerPolicy(RoutingManagerPolicy&&) = delete;
	RoutingManagerPolicy& operator=(RoutingManagerPolicy const&) = delete;
	RoutingManagerPolicy& operator=(RoutingManagerPolicy&&) = delete;

	void CreateRoutingTableFromCache(detail::RoutingPacket& table);  ///< Cache is used when Routing on Request in EventBuilding mode only, otherwise just creates RoutingTables structure

	struct RoutingCacheEntry
	{
		bool is_valid{false};
		int destination_rank{-1};
		Fragment::sequence_id_t sequence_id{artdaq::Fragment::InvalidSequenceID};
		int requesting_rank{-1};

		RoutingCacheEntry() {}
		RoutingCacheEntry(Fragment::sequence_id_t seq, int dest, int source)
		    : is_valid(true), destination_rank(dest), sequence_id(seq), requesting_rank(source) { }
	};
	std::map<Fragment::sequence_id_t, std::vector<RoutingCacheEntry>> routing_cache_;
	size_t routing_cache_max_size_;
	mutable std::mutex routing_cache_mutex_;
	std::atomic<size_t> max_token_count_;
	mutable std::mutex tokens_mutex_;
};
}  // namespace artdaq

#endif  // artdaq_Application_Routing_RoutingManagerPolicy_hh
