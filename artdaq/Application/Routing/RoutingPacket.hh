#ifndef artdaq_Application_Routing_RoutingPacket_hh
#define artdaq_Application_Routing_RoutingPacket_hh

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq
{
	namespace detail
	{
		struct RoutingPacketEntry;
		typedef std::vector<RoutingPacketEntry> RoutingPacket;
		struct RoutingPacketHeader;
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

#define ROUTING_MAGIC 0x1337beef
struct artdaq::detail::RoutingPacketHeader
{
	uint32_t header;
	size_t nEntries;

	explicit RoutingPacketHeader(size_t n) : header(ROUTING_MAGIC), nEntries(n) {}
	RoutingPacketHeader() : header(0), nEntries(0) {}
};

struct artdaq::detail::RoutingAckPacket
{
	int rank;
	Fragment::sequence_id_t first_sequence_id;
	Fragment::sequence_id_t last_sequence_id;
};

#define TOKEN_MAGIC 0xbeefcafe
struct artdaq::detail::RoutingToken
{
	uint32_t header;
	int rank;
	unsigned new_slots_free;
};

#endif //artdaq_Application_Routing_RoutingPacket_hh