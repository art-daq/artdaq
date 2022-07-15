#ifndef artdaq_DAQrate_detail_RequestMessage_hh
#define artdaq_DAQrate_detail_RequestMessage_hh

#include "TRACE/tracemf.h" // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"
#define MAX_REQUEST_MESSAGE_SIZE 65000

#include "artdaq/DAQdata/Globals.hh"

#include <iostream>
#include <vector>

namespace artdaq {
namespace detail {
struct RequestPacket;
struct RequestHeader;
class RequestMessage;

/**
		 * \brief Mode used to indicate current run conditions to the request receiver
		 */
enum class RequestMessageMode : uint8_t
{
	Normal = 0,    ///< Normal running
	EndOfRun = 1,  ///< End of Run mode (Used to end request processing on receiver)
};

/**
		 * \brief Converts the RequestMessageMode to a string and sends it to the output stream
		 * \param o Stream to send string to
		 * \param m RequestMessageMode to convert to string
		 * \return o with string sent to it
		 */
inline std::ostream& operator<<(std::ostream& o, RequestMessageMode m)
{
	switch (m)
	{
		case RequestMessageMode::Normal:
			o << "Normal";
			break;
		case RequestMessageMode::EndOfRun:
			o << "EndOfRun";
			break;
	}
	return o;
}

}  // namespace detail
}  // namespace artdaq

/**
 * \brief The RequestPacket contains information about a single data request
 */
struct artdaq::detail::RequestPacket
{
public:
	/** The magic bytes for the request packet */
	uint32_t header{0};                                                //TRIG, or 0x54524947
	Fragment::sequence_id_t sequence_id{Fragment::InvalidSequenceID};  ///< The sequence ID that responses to this request should use
	Fragment::timestamp_t timestamp{Fragment::InvalidTimestamp};       ///< The timestamp of the request

	RequestPacket() = default;

	/**
	 * \brief Create a RequestPacket using the given sequence ID and timestmap
	 * \param seq Sequence ID of RequestPacket
	 * \param ts Timestamp of RequestPAcket
	 */
	RequestPacket(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& ts)
	    : header(0x54524947)
	    , sequence_id(seq)
	    , timestamp(ts)
	{}

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
	uint32_t header{0x48454452};                          //HEDR, or 0x48454452
	uint32_t packet_count{0};                             ///< The number of RequestPackets in this Request message
	int rank{my_rank};                                    ///< Rank of the sender
	uint32_t run_number{0};                               ///< The Run with which this request should be associated
	RequestMessageMode mode{RequestMessageMode::Normal};  ///< Communicates additional information to the Request receiver

	RequestHeader() = default;

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
	RequestMessage()
	    : header_()
	    , packets_()
	{}

	/**
	 * \brief Get the contents of the RequestMessage
	 * \return Vector of bytes corresponding to the full RequestMessage (may span multiple packets)
	 */
	std::vector<uint8_t> GetMessage()
	{
		auto size = sizeof(RequestHeader) + packets_.size() * sizeof(RequestPacket);
		header_.packet_count = packets_.size();
		assert(size < MAX_REQUEST_MESSAGE_SIZE);
		auto output = std::vector<uint8_t>(size);
		memcpy(&output[0], &header_, sizeof(RequestHeader));
		memcpy(&output[sizeof(RequestHeader)], &packets_[0], packets_.size() * sizeof(RequestPacket));

		return output;
	}

	/**
	 * \brief Set the Request Message Mode for this request
	 * \param mode Mode for this Request Message
	 */
	void setMode(RequestMessageMode mode)
	{
		header_.mode = mode;
	}

	/**
	 * \brief Set the rank in the header for this request. This will be the rank from which the request originates.
	 * \param rank Rank for this Request Message
	 */
	void setRank(int rank)
	{
		header_.rank = rank;
	}

	/**
	 * \brief Set the run number in the header for this request.
	 * This will be the Run for which the request is valid.
	 * \param run Run number for this Request Message
	 */
	void setRunNumber(int run)
	{
		header_.run_number = run;
	}

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

	/**
	 * @brief Get the maximum number of requests that can be sent in a single RequestMessage
	 * @return The maximum number of reqeusts that fit in a single UDP datagram
	*/
	static size_t max_request_count()
	{
		return (MAX_REQUEST_MESSAGE_SIZE - sizeof(RequestHeader)) / sizeof(RequestPacket);
	}

private:
	RequestHeader header_;
	std::vector<RequestPacket> packets_;
};

#endif  // artdaq_DAQrate_detail_RequestMessage
