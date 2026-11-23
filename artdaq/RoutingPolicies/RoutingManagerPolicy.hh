#ifndef artdaq_Application_Routing_RoutingManagerPolicy_hh
#define artdaq_Application_Routing_RoutingManagerPolicy_hh

#include "TRACE/tracemf.h"  // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"

#include "artdaq/DAQrate/detail/RoutingPacket.hh"  // No library dependence.

namespace fhicl {
class ParameterSet;
}

#include <deque>
#include <mutex>
#include <unordered_set>

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

	/**
	 * @brief Get the number of tokens that have been used since the last update
	 * @return Current value of the token counter
	 */
	size_t GetTokensUsedSinceLastUpdate() const { return tokens_used_since_last_update_; }
	/**
	 * @brief Reset the number of tokens used
	 */
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

	/**
	 * @brief Get the next sequence ID to be routed
	 * @return Next sequence ID to be routed
	 */
	Fragment::sequence_id_t GetNextSequenceID() const { return next_sequence_id_; }

	/**
	 * @brief Get the number of tokens that are waiting to be used
	 * @return size of the held tokens list
	 */
	size_t GetHeldTokenCount() const
	{
		std::unique_lock<std::mutex> lk(tokens_mutex_);
		return tokens_.size();
	}

	/**
	 * @brief Create a RoutingPacket from currently-owned tokens. Used by EventBuilder and RequestBasedEventBuilder RoutingManagerMode
	 * @return artdaq::detail::RoutingPacket created from tokens held by Routing Manager.
	 */
	detail::RoutingPacket GetCurrentTable();

	/**
	 * @brief Get an artdaq::detail::RoutingPacketEntry for a given sequence ID and rank. Used by RequestBasedEventBuilder and DataFlow RoutingManagerMode
	 * @param seq Sequence Number to get route for
	 * @param requesting_rank Rank to route for
	 * @return artdaq::detail::RoutingPacketEntry connecting sequence ID to destination rank
	 */
	detail::RoutingPacketEntry GetRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank);

	/**
	 * @brief Get the current RoutingManagerMode of this RoutingManager
	 * @return artdaq::detail::RoutingManagerMode value
	 */
	detail::RoutingManagerMode GetRoutingMode() const { return routing_mode_; }

	// For tests
	/**
	 * @brief Get the size of the routing cache. For testing
	 * @return Size of the routing cache
	 */
	size_t GetCacheSize() const { return routing_cache_.size(); }
	/**
	 * @brief Determine whether the routing cache has a route for the given sequence ID. For testing
	 * @param seq Sequence ID to check
	 * @return True if the sequence ID is in the cache
	 */
	bool CacheHasRoute(artdaq::Fragment::sequence_id_t seq) const { return routing_cache_.count(seq) != 0; }

protected:
	/**
	 * \brief Generate entries to add to the given table
	 * \param tables The RoutingPacket to add entries to
	 */
	virtual void CreateRoutingTable(detail::RoutingPacket& tables) = 0;

	/**
	 * @brief Generate a route for the given sequence ID and source rank
	 * @param seq Sequence ID to route
	 * @param requesting_rank Source rank requesting routing information
	 * @return An artdaq::detail::RoutingPacketEntry linking the sequence ID to a destination rank
	 */
	virtual detail::RoutingPacketEntry CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank) = 0;

	// Tokens
	std::deque<int> tokens_;                             ///< The list of tokens which are available for use
	std::atomic<size_t> tokens_used_since_last_update_;  ///< Number of tokens consumed since last metric update

	// Routing Information
	Fragment::sequence_id_t next_sequence_id_;  ///< The next sequence ID to be assigned
	std::unordered_set<int> receiver_ranks_;    ///< Configured receiver (e.g. EventBuilder for BR->EB routing) ranks
	detail::RoutingManagerMode routing_mode_;   ///< Current routing mode

private:
	RoutingManagerPolicy(RoutingManagerPolicy const&) = delete;
	RoutingManagerPolicy(RoutingManagerPolicy&&) = delete;
	RoutingManagerPolicy& operator=(RoutingManagerPolicy const&) = delete;
	RoutingManagerPolicy& operator=(RoutingManagerPolicy&&) = delete;

	void CreateRoutingTableFromCache(detail::RoutingPacket& table);  ///< Cache is used when in EventBuilding modes only, otherwise just creates RoutingTables structure

	void TrimRoutingCache();
	void UpdateCache(detail::RoutingPacket& table);

	struct RoutingCacheEntry
	{
		bool is_valid{false};
		int destination_rank{-1};
		Fragment::sequence_id_t sequence_id{artdaq::Fragment::InvalidSequenceID};
		int requesting_rank{-1};
		bool included_in_table{false};

		RoutingCacheEntry() {}
		RoutingCacheEntry(Fragment::sequence_id_t seq, int dest, int source)
		    : is_valid(true), destination_rank(dest), sequence_id(seq), requesting_rank(source) {}
	};
	std::map<Fragment::sequence_id_t, std::vector<RoutingCacheEntry>> routing_cache_;
	size_t routing_cache_max_size_;
	mutable std::mutex routing_cache_mutex_;
	std::atomic<size_t> max_token_count_;
	mutable std::mutex tokens_mutex_;
};
}  // namespace artdaq

#endif  // artdaq_Application_Routing_RoutingManagerPolicy_hh
