#ifndef artdaq_DAQrate_detail_RequestMessage_hh
#define artdaq_DAQrate_detail_RequestMessage_hh

#include "artdaq-core/Data/Fragment.hh"

namespace artdaq
{
	namespace detail
	{
		struct RequestPacket;
		struct RequestHeader;
		class RequestMessage;

		enum class RequestMessageMode
		{
			Normal = 0,
			EndOfRun = 1,
		};
	}
}

/**
 * \brief The RequestPacket contains information about a single data request
 */
struct artdaq::detail::RequestPacket
{
public:
	/** The magic bytes for the request packet */
	uint32_t header; //TRIG, or 0x54524947
	Fragment::sequence_id_t sequence_id; ///< The sequence ID that responses to this request should use
	Fragment::timestamp_t timestamp; ///< The timestamp of the request

	/**
	 * \brief Default Constructor
	 */
	RequestPacket()
		: header(0)
		, sequence_id(Fragment::InvalidSequenceID)
		, timestamp(Fragment::InvalidTimestamp) {}

	/**
	 * \brief Create a RequestPacket using the given sequence ID and timestmap
	 * \param seq Sequence ID of RequestPacket
	 * \param ts Timestamp of RequestPAcket
	 */
	RequestPacket(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& ts)
		: header(0x54524947)
		, sequence_id(seq)
		, timestamp(ts) {}

	/**
	 * \brief Check the magic bytes of the packet
	 * \return Whether the correct magic bytes were found
	 */
	bool isValid() const { return header == 0x54524947; }
};

/**
 * \brief Header of a RequestMessage. Contains magic bytes for validation and a count of expected RequestPackets
 */
struct artdaq::detail::RequestHeader
{
	/** The magic bytes for the request header */
	uint32_t header; //HEDR, or 0x48454452
	uint32_t packet_count; ///< The number of RequestPackets in this Request message
	RequestMessageMode mode ; ///< Communicates additional information to the Request receiver

	/**
	 * \brief Default Constructor
	 */
	RequestHeader() : header(0x48454452)
	                , packet_count(0)
		, mode(RequestMessageMode::Normal)
	{}

	/**
	* \brief Check the magic bytes of the packet
	* \return Whether the correct magic bytes were found
	*/
	bool isValid() const { return header == 0x48454452; }
};

/**
 * \brief A RequestMessage consists of a RequestHeader and zero or more RequestPackets. They will usually be sent in two calls to send()
 */
class artdaq::detail::RequestMessage
{
public:
	/**
	 * \brief Default Constructor
	 */
	RequestMessage() : header_()
	                 , packets_() { }

	/**
	 * \brief Get a pointer to the RequestHeader, filling in the current size of the message
	 * \return A pointer to the RequestHeader
	 */
	RequestHeader* header()
	{
		header_.packet_count = packets_.size();
		return &header_;
	}

	/**
	 * \brief Get a pointer to the first RequestPacket in contiguous storage
	 * \return Pointer to the first Request Packet
	 */
	RequestPacket* buffer() { return &packets_[0]; }
	/**
	 * \brief Get the number of RequestPackets in the RequestMessage
	 * \return The number of RequestPackets in the RequestMessage
	 */
	size_t size() const { return packets_.size(); }

	/**
	 * \brief Add a request for a sequence ID and timestamp combination
	 * \param seq Sequence ID to request
	 * \param time Timestamp of request
	 */
	void addRequest(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& time)
	{
		packets_.emplace_back(RequestPacket(seq, time));
	}

private:
	RequestHeader header_;
	std::vector<RequestPacket> packets_;
};

#endif // artdaq_DAQrate_detail_RequestMessage
