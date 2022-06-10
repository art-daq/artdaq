#ifndef artdaq_DAQrate_detail_RoutingPacket_hh
#define artdaq_DAQrate_detail_RoutingPacket_hh

#include "TRACE/tracemf.h" // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"

#include <string>
#include <vector>

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
	EventBuilding,              ///< Multiple sources sending to a single destination. RoutingManager pushes table updates to all senders
	RequestBasedEventBuilding,  ///< Multiple sources sending to a single destination. Table updates are triggered by senders requesting routing information
	DataFlow,                   ///< One source sending to one destination (i.e. moving around completed events). Uses request-based routing
	INVALID
};
/**
 * @brief Convert RoutingManagerMode to/from strings
 */
class RoutingManagerModeConverter
{
public:
	/**
	 * @brief Convert String to RoutingManagerMode
	 * @param modeString String to convert
	 * @return Resultant RoutingManagerMode, RoutingManagerMode::INVALID if no match
	 */
	static RoutingManagerMode stringToRoutingManagerMode(std::string const& modeString)
	{
		if (modeString == "EventBuilding" || modeString == "eventbuilding") return RoutingManagerMode::EventBuilding;
		if (modeString == "RequestBasedEventBuilding" || modeString == "requestbasedeventbuilding" || modeString == "RequestBased" || modeString == "requestbased") return RoutingManagerMode::RequestBasedEventBuilding;
		if (modeString == "DataFlow" || modeString == "dataflow") return RoutingManagerMode::DataFlow;
		return RoutingManagerMode::INVALID;
	}
	/**
	 * @brief Convert RoutingManagerMode to string
	 * @param mode Mode to convert
	 * @return String representation of mode
	 */
	static std::string routingManagerModeToString(RoutingManagerMode mode)
	{
		switch (mode)
		{
			case RoutingManagerMode::EventBuilding:
				return "EventBuilding";
			case RoutingManagerMode::RequestBasedEventBuilding:
				return "RequestBasedEventBuilding";
			case RoutingManagerMode::DataFlow:
				return "DataFlow";
			case RoutingManagerMode::INVALID:
				return "INVALID";
		}
		return "Unknown Mode";
	}
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
	int32_t destination_rank{-1};                                      ///< The destination rank for this sequence ID
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
	uint32_t header{0};    ///< Magic bytes to make sure the packet wasn't garbled
	uint64_t nEntries{0};  ///< The number of RoutingPacketEntries in the RoutingPacket

	/**
	 * \brief Construct a RoutingPacketHeader declaring a given number of entries
	 * \param n The number of RoutingPacketEntries in the associated RoutingPacket
	 */
	explicit RoutingPacketHeader(size_t n)
	    : header(ROUTING_MAGIC), nEntries(n) {}
	/**
	 * \brief Default Constructor
	 */
	RoutingPacketHeader() {}
};

/**
 * @brief Represents a request sent to the RoutingManager for routing information
*/
struct artdaq::detail::RoutingRequest
{
	/**
	 * @brief The mode of this request, whether Request or Connect/Disconnect control messages
	*/
	enum class RequestMode : uint8_t
	{
		Connect = 0,
		Disconnect = 1,
		Request = 2,
		Invalid = 255,
	};

	/**
	 * @brief Convert a RequestMode enumeration value to string
	 * @param m RequestMode to convert
	 * @return String representation of RequestMode
	*/
	static std::string RequestModeToString(RequestMode m)
	{
		switch (m)
		{
			case RequestMode::Connect:
				return "Connect";
			case RequestMode::Disconnect:
				return "Disconnect";
			case RequestMode::Request:
				return "Request";
			case RequestMode::Invalid:
				return "Invalid";
		}
		return "UNKNOWN";
	}

	uint32_t header{0};                                                        ///< Magic bytes for identifying message type on wire
	int32_t rank{-1};                                                          ///< The rank of the request sender
	Fragment::sequence_id_t sequence_id{artdaq::Fragment::InvalidSequenceID};  ///< The sequence ID being requested in Request mode
	RequestMode mode{RequestMode::Invalid};                                    ///< Mode of the request

	/**
	 * @brief Create a request using the given rank and mode
	 * @param r Rank of the requestor
	 * @param m Mode of this request
	 * 
	 * This constructor is primarily used to sed RequestMode::Connect and RequestMode::Disconnect control messages
	*/
	RoutingRequest(int r, RequestMode m = RequestMode::Connect)
	    : header(ROUTING_MAGIC), rank(r), mode(m) {}

	/**
	 * @brief Create a RoutingRequest using the given rank and sequence ID
	 * @param r Rank of the requestor
	 * @param seq Sequence ID of request
	*/
	RoutingRequest(int r, Fragment::sequence_id_t seq)
	    : header(ROUTING_MAGIC), rank(r), sequence_id(seq), mode(RequestMode::Request) {}

	RoutingRequest() {}  ///< Default constructor
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
