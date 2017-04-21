#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include <fhiclcpp/ParameterSet.h>
#include "trace.h"

artdaq::RoutingMasterPolicy::RoutingMasterPolicy(fhicl::ParameterSet ps)
	: next_sequence_id_(0)
	, tokens_()
{
	auto eb_ranks = ps.get<std::vector<int>>("event_builder_ranks");
	eb_count_ = eb_ranks.size();
	auto eb_buffers = ps.get<int>("event_builder_buffer_count");
	for(auto i = 0; i < eb_buffers;++i)
	{
		for (auto rank : eb_ranks) { tokens_.push_back(rank); }
	}
}

void artdaq::RoutingMasterPolicy::AddEventBuilderToken(int rank, unsigned new_slots_free)
{
	TRACE(10, "RoutingMasterPolicy::AddEventBuilderToken BEGIN");
	std::unique_lock<std::mutex> lk(tokens_mutex_);
	for(unsigned i = 0; i < new_slots_free;++i)
	{
		tokens_.push_back(rank);
	}
	TRACE(10, "RoutingMasterPolicy::AddEventBuilderToken END");
}

std::unique_ptr<std::deque<int>> artdaq::RoutingMasterPolicy::getTokensSnapshot()
{
	TRACE(10, "RoutingMasterPolicy::getTokensSnapshot BEGIN");
	std::unique_lock<std::mutex> lk(tokens_mutex_);
	auto out = std::make_unique<std::deque<int>>(tokens_);
	tokens_.clear();
	TRACE(10, "RoutingMasterPolicy::getTokensSnapshot END");
	return out;
}

void artdaq::RoutingMasterPolicy::addUnusedTokens(std::unique_ptr<std::deque<int>> tokens)
{
	std::unique_lock<std::mutex> lk(tokens_mutex_);
	for(auto token = tokens.get()->rbegin();token != tokens.get()->rend();++token)
	{
		tokens_.push_front(*token);
	}
}