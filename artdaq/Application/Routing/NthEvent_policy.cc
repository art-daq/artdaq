#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include "artdaq/Application/Routing/PolicyMacros.hh"
#include <fhiclcpp/ParameterSet.h>
#include "trace.h"

namespace artdaq
{
	class NthEventPolicy : public RoutingMasterPolicy
	{
	public:
		explicit NthEventPolicy(fhicl::ParameterSet ps);

		virtual ~NthEventPolicy() { }

		detail::RoutingPacket GetCurrentTable() override;
	private:
		size_t nth_;
		int nth_rank_;
	};

	NthEventPolicy::NthEventPolicy(fhicl::ParameterSet ps)
		: RoutingMasterPolicy(ps)
		, nth_(ps.get<size_t>("nth_event"))
		, nth_rank_(ps.get<int>("target_event_builder"))
	{
		if (nth_ == 0) throw cet::exception("NthEvent_policy") << "nth_event must be greater than 0!";
	}

	detail::RoutingPacket NthEventPolicy::GetCurrentTable()
	{
		auto tokens = getTokensSnapshot();
		std::map<int, int> table;
		for (auto token : *tokens.get())
		{
			table[token]++;
		}
		if (table.count(nth_rank_) == 0) table[nth_rank_] = 0;
		tokens->clear();

		detail::RoutingPacket output;
		TRACE(5, "NthEvent_policy: table[nth_rank_]=%i, Next nth=%zu, max seq=%zu\n", table[nth_rank_], ((next_sequence_id_ / nth_) + 1) * nth_, next_sequence_id_ + table.size() - 1 );
		auto endCondition = table.size() < GetEventBuilderCount() || (table[nth_rank_] <= 0 && (next_sequence_id_ % nth_ == 0 || ((next_sequence_id_ / nth_) + 1) * nth_ < next_sequence_id_ + table.size() - 1));
		while (!endCondition)
		{
			for (auto r : table)
			{
				TRACE(5,"NthEvent_policy: nth_=%zu, nth_rank=%i, r=%i, next_sequence_id=%zu\n", nth_, nth_rank_, r.first, next_sequence_id_);
				if(next_sequence_id_ % nth_ == 0)
				{
					TRACE(5,"NthEvent_policy: Diverting event %zu to EVB %i\n", next_sequence_id_, nth_rank_);
					output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, nth_rank_));
					table[nth_rank_]--;
				}
				if (r.first != nth_rank_) {
					TRACE(5, "NthEvent_policy: Sending event %zu to EVB %i\n", next_sequence_id_, r.first);
					output.emplace_back(detail::RoutingPacketEntry(next_sequence_id_++, r.first));
					if (!endCondition) endCondition = r.second == 1;
					table[r.first]--;
				}
			}
			TRACE(5, "NthEvent_policy: table[nth_rank_]=%i, Next nth=%zu, max seq=%zu\n", table[nth_rank_], ((next_sequence_id_ / nth_) + 1) * nth_, next_sequence_id_ + table.size() - 1 );
			endCondition = endCondition || (table[nth_rank_] <= 0 && (next_sequence_id_ % nth_ == 0 || (next_sequence_id_ / nth_) * nth_ + nth_ < next_sequence_id_ + table.size() - 1));
		}

		for (auto r : table)
		{
			for (auto i = 0; i < r.second; ++i)
			{
				tokens->push_back(r.first);
			}
		}
		addUnusedTokens(std::move(tokens));

		return output;
	}
}

DEFINE_ARTDAQ_ROUTING_POLICY(artdaq::NthEventPolicy)