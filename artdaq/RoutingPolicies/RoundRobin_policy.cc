#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RoundRobin_policy").c_str()

#include "artdaq/RoutingPolicies/RoutingMasterPolicy.hh"
#include "artdaq/RoutingPolicies/PolicyMacros.hh"
#include "fhiclcpp/ParameterSet.h"
#include "tracemf.h"

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
		 * "minimum_participants" (Default: 0): Minimum number of receivers to distribute between. Use negative number to indicate how many can be missing from total. If the number of allowed missing receivers is greater than the number that exist, then the minimum number of participants will be set to 1.
		 */
		explicit RoundRobinPolicy(fhicl::ParameterSet ps)
			: RoutingMasterPolicy(ps)
			, minimum_participants_(ps.get<int>("minimum_participants", 0))
		{
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

		// If 0 or negative, add minimum_participants_ to GetRecevierCount to ensure that it's correct
		// 02-Apr-2019, KAB: changed the declared type of "minimum" from 'auto' to 'int' to avoid the
		// situation in which the compiler chooses a type of 'unsigned int', the minimum_participants_ is
		// a negative number that is larger (in absolute value) to the receiver count, and "minimum"
		// ends up with a large positive value.
		int minimum = minimum_participants_ > 0 ? minimum_participants_ : GetReceiverCount() + minimum_participants_;
		if (minimum < 1) minimum = 1; // Can't go below 1
		if (minimum > static_cast<int>(GetReceiverCount())) minimum = GetReceiverCount(); // Can't go above receiver count

		bool endCondition = table.size() < static_cast<size_t>(minimum);
		TLOG(15) << "RoundRobinPolicy::GetCurrentTable initial endCondition is " << endCondition << ", minimum is " << minimum;

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
			endCondition = table.size() < static_cast<size_t>(minimum);
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
