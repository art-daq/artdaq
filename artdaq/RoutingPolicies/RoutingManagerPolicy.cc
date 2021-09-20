#define TRACE_NAME "RoutingManagerPolicy"

#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"

artdaq::RoutingManagerPolicy::RoutingManagerPolicy(const fhicl::ParameterSet& ps)
    : tokens_used_since_last_update_(0)
    , next_sequence_id_(1)
    , max_token_count_(0)
{
	routing_mode_ = detail::RoutingManagerModeConverter::stringToRoutingManagerMode(ps.get<std::string>("routing_manager_mode", "EventBuilding"));
	routing_cache_max_size_ = ps.get<size_t>("routing_cache_size", 1000);
}

artdaq::detail::RoutingPacket artdaq::RoutingManagerPolicy::GetCurrentTable()
{
	auto table = detail::RoutingPacket();

	if (routing_mode_ == detail::RoutingManagerMode::EventBuilding)
	{
		std::lock_guard<std::mutex> lk(tokens_mutex_);
		CreateRoutingTable(table);
		UpdateCache(table);
		CreateRoutingTableFromCache(table);
	}
	if (routing_mode_ == detail::RoutingManagerMode::RequestBasedEventBuilding)
	{
		CreateRoutingTableFromCache(table);
	}

	return table;
}

void artdaq::RoutingManagerPolicy::AddReceiverToken(int rank, unsigned new_slots_free)
{
	if (receiver_ranks_.count(rank) == 0u)
	{
		TLOG(TLVL_INFO) << "Adding rank " << rank << " to receivers list (initial tokens=" << new_slots_free << ")";
		receiver_ranks_.insert(rank);
	}
	TLOG(10) << "AddReceiverToken BEGIN";
	std::lock_guard<std::mutex> lk(tokens_mutex_);
	if (new_slots_free == 1)
	{
		tokens_.push_back(rank);
	}
	else
	{
		// Randomly distribute multitokens through the token list
		// Only used at start run time, so we can take the performance hit
		for (unsigned i = 0; i < new_slots_free; ++i)
		{
			auto it = tokens_.begin();
			if (!tokens_.empty())
			{
				std::advance(it, rand() % tokens_.size());  // NOLINT(cert-msc50-cpp)
			}
			tokens_.insert(it, rank);
		}
	}
	if (tokens_.size() > max_token_count_)
	{
		max_token_count_ = tokens_.size();
	}
	TLOG(10) << "AddReceiverToken END";
}

void artdaq::RoutingManagerPolicy::Reset()
{
	next_sequence_id_ = 1;
	std::unique_lock<std::mutex> lk(tokens_mutex_);
	tokens_.clear();
	receiver_ranks_.clear();
}

artdaq::detail::RoutingPacketEntry artdaq::RoutingManagerPolicy::GetRouteForSequenceID(artdaq::Fragment::sequence_id_t seq, int requesting_rank)
{
	if (routing_mode_ != detail::RoutingManagerMode::DataFlow)
	{
		std::lock_guard<std::mutex> lk(routing_cache_mutex_);
		if (routing_cache_.count(seq))
		{
			return detail::RoutingPacketEntry(seq, routing_cache_[seq][0].destination_rank);
		}
		else
		{
			std::lock_guard<std::mutex> tlk(tokens_mutex_);
			auto entry = CreateRouteForSequenceID(seq, requesting_rank);
			if (entry.sequence_id == seq)
			{
				routing_cache_[seq].emplace_back(seq, entry.destination_rank, requesting_rank);
			}
			return entry;
		}
	}
	else
	{
		std::lock_guard<std::mutex> lk(routing_cache_mutex_);
		if (routing_cache_.count(seq))
		{
			for (auto& entry : routing_cache_[seq])
			{
				if (entry.requesting_rank == requesting_rank)
				{
					return detail::RoutingPacketEntry(seq, entry.destination_rank);
				}
			}
		}

		std::lock_guard<std::mutex> tlk(tokens_mutex_);
		auto entry = CreateRouteForSequenceID(seq, requesting_rank);
		if (entry.sequence_id == seq)
		{
			routing_cache_[seq].emplace_back(seq, entry.destination_rank, requesting_rank);
		}

		TrimRoutingCache();
		return entry;
	}
	return detail::RoutingPacketEntry();
}

void artdaq::RoutingManagerPolicy::TrimRoutingCache()
{
	while (routing_cache_.size() > routing_cache_max_size_)
	{
		routing_cache_.erase(routing_cache_.begin());
	}
}

void artdaq::RoutingManagerPolicy::UpdateCache(detail::RoutingPacket& table)
{
	std::lock_guard<std::mutex> lk(routing_cache_mutex_);

	for (auto& entry : table)
	{
		if (!routing_cache_.count(entry.sequence_id))
		{
			routing_cache_[entry.sequence_id].emplace_back(entry.sequence_id, entry.destination_rank, my_rank);
		}
	}
}

void artdaq::RoutingManagerPolicy::CreateRoutingTableFromCache(detail::RoutingPacket& table)
{
	std::lock_guard<std::mutex> lk(routing_cache_mutex_);

	if (routing_mode_ == detail::RoutingManagerMode::RequestBasedEventBuilding)
	{
		for (auto& cache_entry : routing_cache_)
		{
			if (!cache_entry.second[0].included_in_table)
			{
				table.push_back(artdaq::detail::RoutingPacketEntry(cache_entry.second[0].sequence_id, cache_entry.second[0].destination_rank));
				cache_entry.second[0].included_in_table = true;
			}
		}

		TrimRoutingCache();
	}
}
