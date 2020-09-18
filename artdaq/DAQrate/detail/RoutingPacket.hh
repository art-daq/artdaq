#ifndef artdaq_DAQrate_detail_RoutingPacket_hh
#define artdaq_DAQrate_detail_RoutingPacket_hh

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
namespace detail {
struct RoutingPacketEntry;
/**
		 * \brief A RoutingPacket is simply a vector of RoutingPacketEntry objects.
		 * It is not suitable for network transmission, rather a RoutingPacketHeader
		 * should be sent, followed by &RoutingPacket.at(0) (the physical storage of the vector)
		 */
using RoutingPacket = std::vector<RoutingPacketEntry>;
struct RoutingPacketHeader;
struct RoutingConnectHeader;
struct RoutingRequest;
struct RoutingToken;

/**
		 * \brief Mode indicating whether the RoutingManager is routing events by Sequence ID or by Send Count
		 */
enum class RoutingManagerMode : uint8_t
{
	EventBuilding, ///< Multiple sources sending to a single destination
	DataFlow,      ///< One source sending to one destination (i.e. moving around completed events)
	INVALID
};
}  // namespace detail
}  // namespace artdaq
/**
 * \brief A row of the Routing Table
 */
struct artdaq::detail::RoutingPacketEntry
{
	/**
	 * \brief Default Constructor
	 */
	RoutingPacketEntry() {}
	/**
	 * \brief Construct a RoutingPacketEntry with the given sequence ID and destination rank
	 * \param seq The sequence ID of the RoutingPacketEntry
	 * \param rank The destination rank for this sequence ID
	 */
	RoutingPacketEntry(Fragment::sequence_id_t seq, int rank)
	    : sequence_id(seq), destination_rank(rank) {}

	Fragment::sequence_id_t sequence_id{Fragment::InvalidSequenceID};  ///< The sequence ID of the RoutingPacketEntry
	int32_t destination_rank{-1};                                          ///< The destination rank for this sequence ID
};

/**
 * \brief Magic bytes expected in every RoutingPacketHeader
 */
#define ROUTING_MAGIC 0x1337beef
#define REQUEST_MAGIC 0x1337cafe

/**
 * \brief The header of the Routing Table, containing the magic bytes and the number of entries
 */
struct artdaq::detail::RoutingPacketHeader
{
	uint32_t header{0};                                    ///< Magic bytes to make sure the packet wasn't garbled
	uint64_t nEntries{0};                                    ///< The number of RoutingPacketEntries in the RoutingPacket

	/**
	 * \brief Construct a RoutingPacketHeader declaring a given number of entries
	 * \param n The number of RoutingPacketEntries in the associated RoutingPacket
	 */
	explicit RoutingPacketHeader(size_t n)
	    : header(ROUTING_MAGIC),nEntries(n) {}
	/**
	 * \brief Default Constructor
	 */
	RoutingPacketHeader() {}
};

struct artdaq::detail::RoutingConnectHeader
{
	enum class ConnectHeaderMode : uint8_t
	{
	Connect = 0,
	Disconnect = 1,
	Invalid = 255,
	};
	uint32_t header{0};
	int32_t rank{-1};
	ConnectHeaderMode mode{ConnectHeaderMode::Invalid};

	explicit RoutingConnectHeader(int r, ConnectHeaderMode m = ConnectHeaderMode::Connect)
	    : header(ROUTING_MAGIC), rank(r), mode(m) {}

	RoutingConnectHeader() {}
};

struct artdaq::detail::RoutingRequest
{
	uint32_t header{0};
	int32_t rank{-1};                         ///< The rank requesting routing information
	Fragment::sequence_id_t sequence_id{artdaq::Fragment::InvalidSequenceID};  ///< The sequence ID being requested

	RoutingRequest(int r, Fragment::sequence_id_t s)
	    : header(REQUEST_MAGIC), rank(r), sequence_id(s) {}

	RoutingRequest() {}
};

/**
 * \brief Magic bytes expected in every RoutingToken
 */
#define TOKEN_MAGIC 0xbeefcafe

/**
 * \brief The RoutingToken contains the magic bytes, the rank of the token sender, and the number of slots free. This is 
 * a TCP message, so additional verification is not necessary.
 */
struct artdaq::detail::RoutingToken
{
	uint32_t header;          ///< The magic bytes that help validate the RoutingToken
	int rank;                 ///< The rank from which the RoutingToken came
	unsigned new_slots_free;  ///< The number of slots free in the token sender (usually 1)
	unsigned run_number;      ///< The Run with which this token should be associated
};

#endif  //artdaq_Application_Routing_RoutingPacket_hh
