#ifndef artdaq_Application_Routing_RoutingMasterPolicy_hh
#define artdaq_Application_Routing_RoutingMasterPolicy_hh

#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "fhiclcpp/fwd.h"
#include <mutex>

namespace artdaq
{
	class RoutingMasterPolicy
	{
	public:
		explicit RoutingMasterPolicy(fhicl::ParameterSet ps);
		virtual ~RoutingMasterPolicy() {};

		virtual detail::RoutingPacket GetCurrentTable() = 0;
		virtual void AddEventBuilderToken(int rank, int new_slots_free, Fragment::sequence_id_t min_seq_id) final;
	protected:
		Fragment::sequence_id_t next_sequence_id_;

		std::unique_ptr<std::map<int, int>> getTableSnapshot();

	private:
		Fragment::sequence_id_t last_seq_id_seen_;
		std::mutex table_mutex_;
		std::map<int, int> table_;

	};
}


#endif // artdaq_Application_Routing_RoutingMasterPolicy_hh