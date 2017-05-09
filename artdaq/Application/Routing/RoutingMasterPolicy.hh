#ifndef artdaq_Application_Routing_RoutingMasterPolicy_hh
#define artdaq_Application_Routing_RoutingMasterPolicy_hh

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "fhiclcpp/fwd.h"
#include <mutex>
#include <deque>
#include <unordered_set>

namespace artdaq
{
	class RoutingMasterPolicy
	{
	public:
		explicit RoutingMasterPolicy(fhicl::ParameterSet ps);
		virtual ~RoutingMasterPolicy() {};

		virtual detail::RoutingPacket GetCurrentTable() = 0;
		size_t GetReceiverCount() const { return receiver_ranks_.size(); }
		size_t GetMaxNumberOfTokens() const { return max_token_count_; }
		void AddReceiverToken(int rank, unsigned new_slots_free);
		void Reset() { next_sequence_id_ = 0; }
	protected:
		Fragment::sequence_id_t next_sequence_id_;

		std::unique_ptr<std::deque<int>> getTokensSnapshot();
		void addUnusedTokens(std::unique_ptr<std::deque<int>> tokens);
	private:
		mutable std::mutex tokens_mutex_;
		std::unordered_set<int> receiver_ranks_;
		std::deque<int> tokens_;
		size_t max_token_count_;

	};
}


#endif // artdaq_Application_Routing_RoutingMasterPolicy_hh