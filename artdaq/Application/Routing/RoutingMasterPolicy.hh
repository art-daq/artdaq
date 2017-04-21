#ifndef artdaq_Application_Routing_RoutingMasterPolicy_hh
#define artdaq_Application_Routing_RoutingMasterPolicy_hh

#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "fhiclcpp/fwd.h"
#include <mutex>
#include <deque>

namespace artdaq
{
	class RoutingMasterPolicy
	{
	public:
		explicit RoutingMasterPolicy(fhicl::ParameterSet ps);
		virtual ~RoutingMasterPolicy() {};

		virtual detail::RoutingPacket GetCurrentTable() = 0;
		size_t GetEventBuilderCount() const { return eb_count_; }
		virtual void AddEventBuilderToken(int rank, unsigned new_slots_free) final;
		virtual void Reset() final { next_sequence_id_ = 0; }
	protected:
		Fragment::sequence_id_t next_sequence_id_;

		std::unique_ptr<std::deque<int>> getTokensSnapshot();
		void addUnusedTokens(std::unique_ptr<std::deque<int>> tokens);
	private:
		std::mutex tokens_mutex_;
		size_t eb_count_;
		std::deque<int> tokens_;

	};
}


#endif // artdaq_Application_Routing_RoutingMasterPolicy_hh