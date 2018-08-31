#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include "fhiclcpp/ParameterSet.h"
#include "tracemf.h"
#define TRACE_NAME "RoundRobin_policy"

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
		 * "minimum_participants" (Default: -1): Minimum number of receivers to distribute between. -1 (default) makes the minumum equal to the number of configured receivers (strict Round-Robin mode)
		 */
		explicit RoundRobinPolicy(fhicl::ParameterSet ps)
			: RoutingMasterPolicy(ps)
			, minimum_participants_(ps.get<int>("minimum_participants", -1))
		{
			if (minimum_participants_ == 0)
			{
				TLOG(TLVL_WARNING) << "minimum_participants == 0 is undefined. Setting to 1. If you wish to instead enable strict Round-Robin mode, set minimum_participants to -1!";
				minimum_participants_ = 1;
		}
		}

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
		int minimum_participants_;
	};

	detail::RoutingPacket RoundRobinPolicy::GetCurrentTable()
	{
		TLOG(12) << "RoundRobinPolicy::GetCurrentTable start";
		auto tokens = getTokensSnapshot();
		TLOG(13) << "RoundRobinPolicy::GetCurrentTable token list size is " << tokens->size();
		std::map<int, int> table;
		for (auto token : *tokens.get())
		{
			TLOG(14) << "RoundRobinPolicy::GetCurrentTable adding token for rank " << token << " to table";
			table[token]++;
		}
		tokens->clear();
		TLOG(13) << "RoundRobinPolicy::GetCurrentTable table size is " << table.size() << ", token list size is " << tokens->size();

		detail::RoutingPacket output;
		auto endCondition = table.size() < (minimum_participants_ > 0 ? minimum_participants_ : GetReceiverCount());
		TLOG(15) << "RoundRobinPolicy::GetCurrentTable initial endCondition is " << endCondition;

		while (!endCondition)
		{
			for (auto it = table.begin(); it != table.end();)
			{
				TLOG(16) << "RoundRobinPolicy::GetCurrentTable assigning sequenceID " << next_sequence_id_ << " to rank " << it->first;
				output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, it->first));
				table[it->first]--;

				if (table[it->first] <= 0) it = table.erase(it);
				else ++it;
			}
			endCondition = table.size() < (minimum_participants_ > 0 ? minimum_participants_ : GetReceiverCount());
		}

		for(auto r : table)
		{
			for(auto i = 0;i < r.second; ++i)
			{
				tokens->push_back(r.first);
			}
		}
		TLOG(13) << "RoundRobinPolicy::GetCurrentTable unused tokens for " << tokens->size() << " ranks will be saved for later";
		addUnusedTokens(std::move(tokens));

		TLOG(12) << "RoundRobinPolicy::GetCurrentTable return with table size " << output.size();
		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::RoundRobinPolicy)
