#ifndef artdaq_DAQrate_detail_RequestMessage_hh
#define artdaq_DAQrate_detail_RequestMessage_hh

#include "artdaq/DAQdata/Globals.hh"

#include "artdaq-core/Data/Fragment.hh"
#define MAX_REQUEST_MESSAGE_SIZE 65000

namespace artdaq {
namespace detail {
struct RequestPacket;
struct RequestHeader;
struct RequestAcknowledgement;
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

class VectorBitset
{
public:
	VectorBitset()
	    : data_() {}

	VectorBitset(std::vector<uint8_t> data)
	    : data_(data) {}

	VectorBitset(uint8_t* ptr, size_t size)
	{
		data_ = std::vector<uint8_t>(size);
		memcpy(&data_[0], ptr, size);
	}

	bool at(int index) const
	{
		size_t byte = index / 8;
		size_t bit = index % 8;
		if (byte >= data_.size()) return false;
		return (data_[byte] & (1 << bit)) != 0;
	}

	void set(int index)
	{
		size_t byte = index / 8;
		size_t bit = index % 8;

		if (byte + 1 > data_.size())
		{
			data_.resize(byte + 1);
		}

		data_[byte] |= (1 << bit);
	}

	void clear(int index)
	{
		size_t byte = index / 8;
		size_t bit = index % 8;

		if (byte + 1 > data_.size())
		{
			data_.resize(byte + 1);
		}

		data_[byte] &= ~(1 << bit);
	}

	void resize(size_t size) { data_.resize(size); }
	size_t size() const { return data_.size(); }

	std::vector<uint8_t> getData() const { return data_; }

	bool any() const
	{
		for (auto& data : data_)
		{
			if (data != 0) return true;
		}
		return false;
	}

private:
	std::vector<uint8_t> data_;
};

}  // namespace detail
}  // namespace artdaq

/**
 * \brief The RequestPacket contains information about a single data request
 */
struct artdaq::detail::RequestPacket
{
public:
	/** The magic bytes for the request packet */
	uint32_t header;                      //TRIG, or 0x54524947
	Fragment::sequence_id_t sequence_id;  ///< The sequence ID that responses to this request should use
	Fragment::timestamp_t timestamp;      ///< The timestamp of the request
	int rank;                             ///< Rank of the sender
	VectorBitset activeRanks;
	struct timespec request_time;  ///< Wall-clock time that this request was generated at

	/**
	 * \brief Default Constructor
	 */
	RequestPacket()
	    : header(0)
	    , sequence_id(Fragment::InvalidSequenceID)
	    , timestamp(Fragment::InvalidTimestamp)
	    , activeRanks()
	{
		clock_gettime(CLOCK_REALTIME, &request_time);
	}

	/**
	 * \brief Create a RequestPacket using the given sequence ID and timestmap
	 * \param seq Sequence ID of RequestPacket
	 * \param ts Timestamp of RequestPacket
	 * \param dest_rank Destination Rank for RequestPacket. -1 (default) will be changed to my_rank
	 */
	RequestPacket(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& ts, int dest_rank = -1)
	    : header(0x54524947)
	    , sequence_id(seq)
	    , timestamp(ts)
	    , rank(dest_rank == -1 ? my_rank : dest_rank)
	    , activeRanks()
	{
		clock_gettime(CLOCK_REALTIME, &request_time);
	}

	RequestPacket(uint8_t*& ptr)
	    : header(0)
	    , sequence_id(Fragment::InvalidSequenceID)
	    , timestamp(Fragment::InvalidTimestamp)
	    , rank(0)
	    , activeRanks()
	{
		memcpy(&header, ptr, sizeof(header));
		ptr += sizeof(header);
		memcpy(&sequence_id, ptr, sizeof(sequence_id));
		ptr += sizeof(sequence_id);
		memcpy(&timestamp, ptr, sizeof(timestamp));
		ptr += sizeof(timestamp);
		memcpy(&rank, ptr, sizeof(rank));
		ptr += sizeof(rank);
		size_t activeRanksSize = 0;
		memcpy(&activeRanksSize, ptr, sizeof(size_t));
		ptr += sizeof(size_t);
		activeRanks = VectorBitset(ptr, activeRanksSize);
		ptr += activeRanksSize;
	}

	std::vector<uint8_t> ToByteVector() const
	{
		size_t size = sizeof(header) + sizeof(sequence_id) + sizeof(timestamp) + sizeof(rank) + sizeof(size_t) + activeRanks.size();
		std::vector<uint8_t> output(size);

		memcpy(&output[0], &header, sizeof(header));
		memcpy(&output[sizeof(header)], &sequence_id, sizeof(sequence_id));
		memcpy(&output[sizeof(header) + sizeof(sequence_id)], &timestamp, sizeof(timestamp));
		memcpy(&output[sizeof(header) + sizeof(sequence_id) + sizeof(timestamp)], &rank, sizeof(rank));
		size_t activeRanksSize = activeRanks.size();
		memcpy(&output[sizeof(header) + sizeof(sequence_id) + sizeof(timestamp) + sizeof(rank)], &activeRanksSize, sizeof(size_t));
		memcpy(&output[sizeof(header) + sizeof(sequence_id) + sizeof(timestamp) + sizeof(rank) + sizeof(size_t)], &(activeRanks.getData())[0], activeRanks.size());
		return output;
	}

	/**
	 * \brief Check the magic bytes of the packet
	 * \return Whether the correct magic bytes were found
	 */
	bool isValid() const { return header == 0x54524947; }

	bool isActive() const
	{
		return activeRanks.any();
	}

	bool hasRank(int rank) const
	{
		return activeRanks.at(rank);
	}

	void clearRank(int rank)
	{
		activeRanks.clear(rank);
	}

	void setActiveRanks(std::set<int> const& ranks)
	{
		for (auto& rank : ranks)
		{
			activeRanks.set(rank);
		}
	}
};
inline std::ostream& operator<<(std::ostream& o, const artdaq::detail::RequestPacket r)
{
	return o << "Sequence ID " << r.sequence_id << ", Timestamp " << r.timestamp << ", Rank " << r.rank;
}

/**
 * \brief Header of a RequestMessage. Contains magic bytes for validation and a count of expected RequestPackets
 */
struct artdaq::detail::RequestHeader
{
	/** The magic bytes for the request header */
	uint32_t header;               ///< HEDR, or 0x48454452
	uint32_t packet_count;         ///< The number of RequestPackets in this Request message
	uint32_t run_number;           ///< The Run with which this request should be associated
	RequestMessageMode mode;       ///< Communicates additional information to the Request receiver
	bool request_acknowledgement;  ///< Whether this request should be acknowledged

	/**
	 * \brief Default Constructor
	 */
	RequestHeader()
	    : header(0x48454452)
	    , packet_count(0)
	    , run_number(0)
	    , mode(RequestMessageMode::Normal)
	    , request_acknowledgement(false)
	{
	}

	/**
	* \brief Check the magic bytes of the packet
	* \return Whether the correct magic bytes were found
	*/
	bool isValid() const { return header == 0x48454452; }
};

struct artdaq::detail::RequestAcknowledgement
{
	uint32_t header;  ///< ACCK, or 0x4042424B
	uint32_t packet_count;
	uint32_t run_number;
	int rank;

	RequestAcknowledgement(uint32_t count = 0, uint32_t run = 0)
	    : header(0x4042424B), packet_count(count), run_number(run), rank(my_rank)
	{}

	bool isValid() { return header == 0x4042424B; }
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

	RequestMessage(void* packet, size_t size)
	    : header_(), packets_()
	{
		assert(size > sizeof(RequestHeader));
		memcpy(&header_, packet, sizeof(RequestHeader));
		TLOG(11) << "Request header word: 0x" << std::hex << header_.header << std::dec << ", packet_count: " << header_.packet_count
		         << ", run number: " << header_.run_number;

		if (header_.isValid())
		{
			packets_.reserve(header_.packet_count);

			uint8_t* ptr = reinterpret_cast<uint8_t*>(packet) + sizeof(RequestHeader);
			for (size_t ii = 0; ii < header_.packet_count; ++ii)
			{
				packets_.emplace_back(RequestPacket(ptr));
				if (!packets_.back().isValid()) break;
			}
		}
	}

	/**
	 * \brief Get the contents of the RequestMessage
	 * \return Vector of bytes corresponding to the full RequestMessage (may span multiple packets)
	 */
	std::vector<uint8_t> GetMessage()
	{
		header_.packet_count = packets_.size();

		auto output = std::vector<uint8_t>(sizeof(RequestHeader));
		memcpy(&output[0], &header_, sizeof(RequestHeader));
		for (auto& packet : packets_)
		{
			auto arr = packet.ToByteVector();

			// Do not go over MAX_REQUEST_MESSAGE_SIZE!
			if (output.size() + arr.size() > MAX_REQUEST_MESSAGE_SIZE)
			{
				if (arr.size() > MAX_REQUEST_MESSAGE_SIZE)
				{
					TLOG(TLVL_ERROR) << "Request is too large! sz=" << arr.size() << ", max=" << MAX_REQUEST_MESSAGE_SIZE;
				}
				continue;
			}
			std::move(arr.begin(), arr.end(), std::back_inserter(output));
		}

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
	 * \brief Set the run number in the header for this request.
	 * This will be the Run for which the request is valid.
	 * \param run Run number for this Request Message
	 */
	void setRunNumber(int run)
	{
		header_.run_number = run;
	}

	uint32_t getRunNumber() const { return header_.run_number; }
	RequestMessageMode getMode() const { return header_.mode; }

	/**
	 * \brief Get the number of RequestPackets in the RequestMessage
	 * \return The number of RequestPackets in the RequestMessage
	 */
	size_t size() const
	{
		return packets_.size();
	}

	/**
	 * \brief Add a request for a sequence ID and timestamp combination
	 * \param seq Sequence ID to request
	 * \param time Timestamp of request
	 * \param rank Rank of request destination. Default is -1, which is to use my_rank
	 */
	void addRequest(const Fragment::sequence_id_t& seq, const Fragment::timestamp_t& time, const int& rank = -1)
	{
		packets_.emplace_back(RequestPacket(seq, time, rank));
	}

	void addRequest(const RequestPacket& packet)
	{
		packets_.push_back(packet);
	}

	void setAcknowledge(bool ack)
	{
		header_.request_acknowledgement = ack;
	}

	bool getAcknowledge()
	{
		return header_.request_acknowledgement;
	}

	bool isValid()
	{
		if (!header_.isValid()) return false;

		for (auto& packet : packets_)
		{
			if (!packet.isValid()) return false;
		}

		return true;
	}

	std::vector<RequestPacket> const& getRequests() const { return packets_; }

private:
	RequestHeader header_;
	std::vector<RequestPacket> packets_;
};

#endif  // artdaq_DAQrate_detail_RequestMessage
