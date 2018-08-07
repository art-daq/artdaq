#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include "fhiclcpp/ParameterSet.h"

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
		 * RoundRobinPolicy accepts the following Parameter:
		 * "use_all_tokens" (Default: false): Whether to use all tokens or to stop when we can no longer complete a "turn"
		 */
		explicit RoundRobinPolicy(fhicl::ParameterSet ps)
			: RoutingMasterPolicy(ps)
			, use_all_tokens_(ps.get<bool>("use_all_tokens", false))
		{}

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

	private:
		bool use_all_tokens_;
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
		auto endCondition = table.size() < use_all_tokens_ ? 1 : GetReceiverCount();

		while (!endCondition)
		{
			for (auto it = table.begin(); it != table.end();)
			{
				output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, it->first));
				table[it->first]--;

				if (table[it->first] <= 0) it = table.erase(it);
				else ++it;
			}
			endCondition = table.size() < use_all_tokens_ ? 1 : GetReceiverCount();
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