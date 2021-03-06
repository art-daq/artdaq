#define TRACE_NAME "NoOp_policy"
#include "TRACE/tracemf.h"

#include <utility>

#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

#include "fhiclcpp/ParameterSet.h"

namespace artdaq {
/**
 * \brief A RoutingManagerPolicy which simply assigns Sequence IDs to tokens in the order they were received
 */
class NoOpPolicy : public RoutingManagerPolicy
{
public:
	/**
	 * \brief NoOpPolicy Constructor
	 * \param ps ParameterSet used to configure the NoOpPolicy
	 *
	 * NoOpPolicy takes no additional Parameters at this time
	 */
	explicit NoOpPolicy(fhicl::ParameterSet const& ps)
	    : RoutingManagerPolicy(ps)
	{
	}

	/**
	 * \brief Default virtual Destructor
	 */
	~NoOpPolicy() override = default;

	/**
	 * @brief Add entries to the given RoutingPacket using currently-held tokens
	 * @param table RoutingPacket to add entries to
	 *
	 * NoOp_policy will add entries for all tokens in the order that they were received
	 */
	void CreateRoutingTable(detail::RoutingPacket& table) override;
	/**
	 * @brief Get an artdaq::detail::RoutingPacketEntry for a given sequence ID and rank. Used by RequestBasedEventBuilder and DataFlow RoutingManagerMode
	 * @param seq Sequence Number to get route for
	 * @param requesting_rank Rank to route for
	 * @return artdaq::detail::RoutingPacketEntry connecting sequence ID to destination rank
	 */
	detail::RoutingPacketEntry CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank) override;

private:
	NoOpPolicy(NoOpPolicy const&) = delete;
	NoOpPolicy(NoOpPolicy&&) = delete;
	NoOpPolicy& operator=(NoOpPolicy const&) = delete;
	NoOpPolicy& operator=(NoOpPolicy&&) = delete;
};

void NoOpPolicy::CreateRoutingTable(detail::RoutingPacket& table)
{
	while (!tokens_.empty())
	{
		table.emplace_back(next_sequence_id_, tokens_.front());
		next_sequence_id_++;
		tokens_.pop_front();
		tokens_used_since_last_update_++;
	}
}

detail::RoutingPacketEntry NoOpPolicy::CreateRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int)
{
	detail::RoutingPacketEntry output;
	if (!tokens_.empty())
	{
		auto dest = tokens_.front();  // No-Op: Use first token
		output = detail::RoutingPacketEntry(seq, dest);
		tokens_.pop_front();
		tokens_used_since_last_update_++;
	}

	return output;
}

}  // namespace artdaq

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::NoOpPolicy)
