#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"
#include <fhiclcpp/ParameterSet.h>

artdaq::RoutingMasterPolicy::RoutingMasterPolicy(fhicl::ParameterSet ps)
	: next_sequence_id_(0)
	, tokens_()
{
	auto receiver_ranks = ps.get<std::vector<int>>("receiver_ranks");
	receiver_ranks_.insert(receiver_ranks.begin(),receiver_ranks.end());
	auto receiver_buffers = ps.get<int>("receiver_buffer_count");
	token_count_ = receiver_ranks_.size() * receiver_buffers;
	for(auto i = 0; i < receiver_buffers;++i)
	{
		for (auto rank : receiver_ranks) { tokens_.push_back(rank); }
	}
}

void artdaq::RoutingMasterPolicy::AddReceiverToken(int rank, unsigned new_slots_free)
{
	if (!receiver_ranks_.count(rank)) return;
	TRACE(10, "RoutingMasterPolicy::AddReceiverToken BEGIN");
	std::unique_lock<std::mutex> lk(tokens_mutex_);
	for(unsigned i = 0; i < new_slots_free;++i)
	{
		tokens_.push_back(rank);
	}
	TRACE(10, "RoutingMasterPolicy::AddReceiverToken END");
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