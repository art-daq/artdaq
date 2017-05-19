#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include <fhiclcpp/ParameterSet.h>

namespace artdaq
{
	/**
	 * \brief A RoutingMasterPolicy which evenly distributes Sequence IDs to all receivers.
	 * If an uneven number of tokens have been received, extra tokens are stored for the next table update.
	 */
	class RoundRobinPolicy : public RoutingMasterPolicy
	{
	public:
		/**
		 * \brief RoundRobinPolicy Constructor
		 * \param ps ParameterSet used to configure RoundRobinPolicy
		 * 
		 * RoundRobinPolicy accepts no Parameters at this time.
		 */
		explicit RoundRobinPolicy(fhicl::ParameterSet ps) : RoutingMasterPolicy(ps) {}

		/**
		 * \brief Default virtual Destructor
		 */
		virtual ~RoundRobinPolicy() = default;

		/**
		 * \brief Create a Routing Table using the tokens that have been received
		 * \return A detail::RoutingPacket containing the table update
		 * 
		 * RoundRobinPolicy will go through the list of receivers as many times
		 * as it can, until one or more receivers have no tokens. It always does full
		 * "turns" through the recevier list.
		 */
		detail::RoutingPacket GetCurrentTable() override;
	};

	detail::RoutingPacket RoundRobinPolicy::GetCurrentTable()
	{
		auto tokens = getTokensSnapshot();
		std::map<int, int> table;
		for (auto token : *tokens.get())
		{
			table[token]++;
		}
		tokens->clear();

		detail::RoutingPacket output;
		auto endCondition = table.size() < GetReceiverCount();
		while (!endCondition)
		{
			for (auto r : table)
			{
				output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, r.first));
				if(!endCondition) endCondition = r.second == 1;
				table[r.first]--;
			}
		}

		for(auto r : table)
		{
			for(auto i = 0;i < r.second; ++i)
			{
				tokens->push_back(r.first);
			}
		}
		addUnusedTokens(std::move(tokens));

		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::RoundRobinPolicy)