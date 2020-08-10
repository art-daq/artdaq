#include <utility>

#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"
#include "tracemf.h"
#define TRACE_NAME "NoOp_policy"
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
	    : RoutingManagerPolicy(ps) {}

	/**
		 * \brief Default virtual Destructor
		 */
	~NoOpPolicy() override = default;

	/**
		 * \brief Using the tokens received so far, create a Routing Table
		 * \return A detail::RoutingPacket containing the Routing Table
		 */
	detail::RoutingPacket GetCurrentTable() override;

private:
	NoOpPolicy(NoOpPolicy const&) = delete;
	NoOpPolicy(NoOpPolicy&&) = delete;
	NoOpPolicy& operator=(NoOpPolicy const&) = delete;
	NoOpPolicy& operator=(NoOpPolicy&&) = delete;
};

detail::RoutingPacket NoOpPolicy::GetCurrentTable()
{
	TLOG(12) << "NoOpPolicy::GetCurrentTable start";
	auto tokens = getTokensSnapshot();
	detail::RoutingPacket output;
	for (auto token : *tokens)
	{
		output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, token));
	}

	TLOG(12) << "NoOpPolicy::GetCurrentTable return";
	return output;
}

}  // namespace artdaq

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::NoOpPolicy)
