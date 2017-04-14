#ifndef artdaq_Application_Routing_RoutingPacket_hh
#define artdaq_Application_Routing_RoutingPacket_hh

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq
{
	namespace detail
	{
		struct RoutingPacketEntry;
		typedef std::vector<RoutingPacketEntry> RoutingPacket;
		struct RoutingAckPacket;
		struct RoutingToken;
	}
}
struct artdaq::detail::RoutingPacketEntry
{
	RoutingPacketEntry() : sequence_id(Fragment::InvalidSequenceID), destination_rank(-1) {}
	RoutingPacketEntry(Fragment::sequence_id_t seq, int rank) : sequence_id(seq), destination_rank(rank) {}
	Fragment::sequence_id_t sequence_id;
	int destination_rank;
};

struct artdaq::detail::RoutingAckPacket
{
	int rank;
	Fragment::sequence_id_t first_sequence_id;
	Fragment::sequence_id_t last_sequence_id;
};

struct artdaq::detail::RoutingToken
{
	int rank;
	int new_slots_free;
	Fragment::sequence_id_t minimum_incomplete_event_number;
};

#endif //artdaq_Application_Routing_RoutingPacket_hh