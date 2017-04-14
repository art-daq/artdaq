#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include <fhiclcpp/ParameterSet.h>

artdaq::RoutingMasterPolicy::RoutingMasterPolicy(fhicl::ParameterSet ps)
	: next_sequence_id_(0)
	, last_seq_id_seen_(0)
	, table_()
{
	auto eb_ranks = ps.get<std::vector<int>>("event_builder_ranks");
	auto eb_buffers = ps.get<int>("event_builder_buffer_count");
	for(auto rank : eb_ranks)
	{
		table_[rank] = eb_buffers;
	}
}

void artdaq::RoutingMasterPolicy::AddEventBuilderToken(int rank, int new_slots_free, Fragment::sequence_id_t min_seq_id)
{
	std::unique_lock<std::mutex> lk(table_mutex_);
	table_[rank] += new_slots_free;
	if (min_seq_id > last_seq_id_seen_) last_seq_id_seen_ = min_seq_id;
}

std::unique_ptr<std::map<int,int>> artdaq::RoutingMasterPolicy::getTableSnapshot()
{
	std::unique_lock<std::mutex> lk(table_mutex_);
	return std::make_unique<std::map<int, int>>(table_);
}