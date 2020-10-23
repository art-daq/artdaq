#ifndef artdaq_DAQrate_detail_RoutingPacket_hh
#define artdaq_DAQrate_detail_RoutingPacket_hh

#include "artdaq-core/Data/Fragment.hh"
#define MAX_ROUTING_TABLE_SIZE 65000

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
struct RoutingAckPacket;
struct RoutingToken;

/**
		 * \brief Mode indicating whether the RoutingManager is routing events by Sequence ID or by Send Count
		 */
enum class RoutingManagerMode : uint8_t
{
	RouteBySequenceID,  ///< Events should be routed by sequence ID (BR -> EB)
	RouteBySendCount,   ///< Events should be routed by send count (EB -> Agg)
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
	int destination_rank{-1};                                          ///< The destination rank for this sequence ID
};

/**
 * \brief Magic bytes expected in every RoutingPacketHeader
 */
#define ROUTING_MAGIC 0x1337beef

/**
 * \brief The header of the Routing Table, containing the magic bytes and the number of entries
 */
struct artdaq::detail::RoutingPacketHeader
{
	uint32_t header{0};                                    ///< Magic bytes to make sure the packet wasn't garbled
	RoutingManagerMode mode{RoutingManagerMode::INVALID};  ///< The current mode of the RoutingManager
	size_t nEntries{0};                                    ///< The number of RoutingPacketEntries in the RoutingPacket
	std::bitset<1024> already_acknowledged_ranks{0};       ///< Bitset of ranks which have already sent valid acknowledgements and therefore do not need to send again

	/**
	 * \brief Construct a RoutingPacketHeader declaring a given number of entries
	 * \param m The RoutingManagerMode that senders are supposed to be operating in
	 * \param n The number of RoutingPacketEntries in the associated RoutingPacket
	 */
	explicit RoutingPacketHeader(RoutingManagerMode m, size_t n)
	    : header(ROUTING_MAGIC), mode(m), nEntries(n) {}
	/**
	 * \brief Default Constructor
	 */
	RoutingPacketHeader() {}
};

/**
 * \brief A RoutingAckPacket contains the rank of the table receiver, plus the first and last sequence IDs in the Routing Table (for verification)
 */
struct artdaq::detail::RoutingAckPacket
{
	int rank;                                   ///< The rank from which the RoutingAckPacket came
	Fragment::sequence_id_t first_sequence_id;  ///< The first sequence ID in the received RoutingPacket
	Fragment::sequence_id_t last_sequence_id;   ///< The last sequence ID in the received RoutingPacket

	/**
	 * @brief Create an EndOfData RoutingAckPacket
	 * @param rank Rank of sender sending EndOfData
	 * @return EndOfData RoutingAckPacket
	*/
	static RoutingAckPacket makeEndOfDataRoutingAckPacket(int rank)
	{
		RoutingAckPacket out;
		out.rank = rank;
		out.first_sequence_id = -3;
		out.last_sequence_id = -2;
		return out;
	}

	/**
	 * @brief Check if a RoutingAckPacket is an EndOfData RoutingAckPacket
	 * @param pkt RoutingAckPacket to check
	 * @return Whether the RoutingAckPacket is an EndOfData RoutingAckPacket
	*/
	static bool isEndOfDataRoutingAckPacket(RoutingAckPacket pkt)
	{
		return pkt.first_sequence_id == static_cast<Fragment::sequence_id_t>(-3) && pkt.last_sequence_id == static_cast<Fragment::sequence_id_t>(-2);
	}
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
