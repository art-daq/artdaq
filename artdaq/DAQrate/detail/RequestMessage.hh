#ifndef artdaq_DAQrate_detail_RequestMessage_hh
#define artdaq_DAQrate_detail_RequestMessage_hh

#include "artdaq/DAQdata/Globals.hh"

#include <set>
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
inline std::ostream& operator<<(std::ostream& o, const RequestMessageMode& m)
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
/**
		 * \brief Converts the RequestMessageMode to a string and sends it to the output stream
		 * \param t Stream to send string to
		 * \param m RequestMessageMode to convert to string
		 * \return t with string sent to it
		 */
inline TraceStreamer& operator<<(TraceStreamer& t, const RequestMessageMode& m)
{
	switch (m)
	{
		case artdaq::detail::RequestMessageMode::Normal:
			t << "Normal";
			break;
		case artdaq::detail::RequestMessageMode::EndOfRun:
			t << "EndOfRun";
			break;
	}
	return t;
}

/// <summary>
/// A bitset-like construct using a variable-size backend
/// </summary>
class VectorBitset
{
public:
	/// <summary>
	/// Default Constructor
	/// </summary>
	VectorBitset()
	    : data_() {}

	/// <summary>
	/// Construct a VectorBitset using an existing byte vector
	/// </summary>
	/// <param name="data">Byte vector to use for VectorBitset initial value</param>
	VectorBitset(std::vector<uint8_t> data)
	    : data_(data) {}

	/// <summary>
	/// Construct a VectorBitset using a memory region
	/// </summary>
	/// <param name="ptr">Pointer to the memory</param>
	/// <param name="size">Number of bytes to copy</param>
	VectorBitset(uint8_t* ptr, size_t size)
	{
		data_ = std::vector<uint8_t>(size);
		memcpy(&data_[0], ptr, size);
	}

	/// <summary>
	/// Read the given bit
	/// </summary>
	/// <param name="index">Index of the bit to read</param>
	/// <returns>True if the bit is set, false otherwise</returns>
	bool at(int index) const
	{
		size_t byte = index / 8;
		size_t bit = index % 8;
		if (byte >= data_.size()) return false;
		return (data_[byte] & (1 << bit)) != 0;
	}

	/// <summary>
	/// Set the given bit, resizing the underlying storage if necessary
	/// </summary>
	/// <param name="index">Index of the bit to set</param>
	void set(size_t index)
	{
		size_t byte = index / 8;
		size_t bit = index % 8;

		if (byte + 1 > data_.size())
		{
			data_.resize(byte + 1);
		}

		data_[byte] |= (1 << bit);
	}

	/// <summary>
	/// Clear the given bit
	/// </summary>
	/// <param name="index">Index of the bit to clear</param>
	void clear(size_t index)
	{
		size_t byte = index / 8;
		size_t bit = index % 8;

		if (byte + 1 <= data_.size())
		{
			data_[byte] &= ~(1 << bit);
		}
	}

	/// <summary>
	/// Resize the underlying storage to the given number of bytes
	/// </summary>
	/// <param name="size">Number of bytes (8-bit words) to resize to</param>
	void resize(size_t size) { data_.resize(size); }
	/// <summary>
	/// Get the size of the underlying storage, in bytes
	/// </summary>
	/// <returns>Size, in bytes, of the VectorBitset storage</returns>
	size_t size() const { return data_.size(); }

	/// <summary>
	/// Get a copy of the storage vector
	/// </summary>
	/// <returns>Vector of bytes containing current state of the VectorBitset</returns>
	std::vector<uint8_t> getData() const { return data_; }

	/// <summary>
	/// Determine if any bits are set in the VectorBitset
	/// </summary>
	/// <returns>True if any bits in the VectorBitset are set, false otherwise</returns>
	bool any() const
	{
		for (auto& data : data_)
		{
			if (data != 0) return true;
		}
		return false;
	}

	std::vector<size_t> getSetBits() const
	{
		std::vector<size_t> output;
		for (size_t byte = 0; byte < data_.size(); ++byte)
		{
			for (size_t bit = 0; bit < 8; ++bit)
			{
				if ((data_[byte] & (1 << bit)) != 0)
				{
					output.push_back((8 * byte) + bit);
				}
			}
		}
		return output;
	}

private:
	std::vector<uint8_t> data_;
};
/**
 * \brief Serialize a VectorBitset to a text stream (i.e. cout)
 * \param o Input ostream
 * \param r VectorBitset to serialize
 * \return Output ostream for continued stream operations
 */
inline std::ostream& operator<<(std::ostream& o, const VectorBitset& r)
{
	o << "[ ";

	auto data = r.getSetBits();
	if (data.size())
	{
		o << data[0];
		for (size_t ii = 1; ii < data.size(); ++ii)
		{
			o << ", " << data[ii];
		}
	}

	o << " ]";
	return o;
}
/**
 * \brief Serialize a VectorBitset to a TRACE statement
 * \param o Input TRACE streamer
 * \param r VectorBitset to serialize
 * \return Output TraceStreamer for continued stream operations
 */
inline TraceStreamer& operator<<(TraceStreamer& o, const VectorBitset& r)
{
	o << "[ ";

	auto data = r.getSetBits();
	if (data.size())
	{
		o << data[0];
		for (size_t ii = 1; ii < data.size(); ++ii)
		{
			o << ", " << data[ii];
		}
	}

	o << " ]";
	return o;
}

/**
 * \brief The RequestPacket contains information about a single data request
 */
struct RequestPacket
{
public:
	/** The magic bytes for the request packet */
	uint32_t header;                      //TRIG, or 0x54524947
	Fragment::sequence_id_t sequence_id;  ///< The sequence ID that responses to this request should use
	Fragment::timestamp_t timestamp;      ///< The timestamp of the request
	int rank;                             ///< Rank of the sender
	VectorBitset activeRanks;             ///< VectorBitset representing ranks that are expected to respond to this RequestPacket
	struct timespec request_time;         ///< Wall-clock time that this request was generated at

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

	/**
	 * \brief Deserialize a RequestPacket that has been serialized using ToByteVector
	 * \param ptr Pointer to memory containing RequestPacket data
	 */
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

	/**
	 * \brief Serialize a RequestPacket to a vector of bytes, for sending
	 * \return Vector of bytes representing request
	 */
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

	/**
	 * \brief Determine if any ranks are marked as active for this RequestPacket
	 * \return True if any ranks are in the activeRanks VectorBitset
	 */
	bool isActive() const
	{
		return activeRanks.any();
	}

	/**
	 * \brief Determine if the given rank is present in the active ranks bitset
	 * \param rank Rank to read
	 * \return True if rank is in the active ranks bitset
	 */
	bool hasRank(int rank) const
	{
		return activeRanks.at(rank);
	}

	/**
	 * \brief Clear the given rank from the active ranks bitset
	 * \param rank Rank to clear
	 */
	void clearRank(int rank)
	{
		activeRanks.clear(rank);
	}

	/**
	 * \brief Set the given ranks to active
	 * \param ranks Ranks to set
	 */
	void setActiveRanks(std::set<int> const& ranks)
	{
		for (auto& rank : ranks)
		{
			activeRanks.set(rank);
		}
	}
};
/**
 * \brief Serialize a RequestPacket to a text stream (i.e. cout)
 * \param o Input ostream
 * \param r RequestPacket to serialize
 * \return Output ostream for continued stream operations
 */
inline std::ostream& operator<<(std::ostream& o, const RequestPacket& r)
{
	return o << "Sequence ID " << r.sequence_id << ", Timestamp " << r.timestamp << ", Rank " << r.rank << ", ActiveRanks: " << r.activeRanks;
}
/**
 * \brief Serialize a RequestPacket to a TRACE statement
 * \param t Input TRACE streamer
 * \param r RequestPacket to serialize
 * \return Output TraceStreamer for continued stream operations
 */
inline TraceStreamer& operator<<(TraceStreamer& t, const RequestPacket& r)
{
	return t << "Sequence ID " << r.sequence_id << ", Timestamp " << r.timestamp << ", Rank " << r.rank << ", ActiveRanks: " << r.activeRanks;
}

/**
 * \brief Header of a RequestMessage. Contains magic bytes for validation and a count of expected RequestPackets
 */
struct RequestHeader
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

/**
 * \brief The RequestAcknowledgement class defines the contents of the message that receivers will send in response to a RequestMessage containing new requests
 */
struct RequestAcknowledgement
{
	uint32_t header;        ///< ACCK, or 0x4042424B
	uint32_t packet_count;  ///< Number of request packets being acknowledged (their sequence IDs follow this header)
	uint32_t run_number;    ///< Current Run number
	int rank;               ///< Rank of the acknowledging process

	/// <summary>
	/// Create a RequestAcknowledgement using the given parameters
	/// </summary>
	/// <param name="count">Number of requests being acknowledged</param>
	/// <param name="run">Current Run number</param>
	RequestAcknowledgement(uint32_t count = 0, uint32_t run = 0)
	    : header(0x4042424B), packet_count(count), run_number(run), rank(my_rank)
	{}

	/// <summary>
	/// Check the header word
	/// </summary>
	/// <returns>True if the header word matches expected value</returns>
	bool isValid() { return header == 0x4042424B; }
};

/**
 * \brief A RequestMessage consists of a RequestHeader and zero or more RequestPackets.
 */
class RequestMessage
{
public:
	/**
	 * \brief Default Constructor
	 */
	RequestMessage()
	    : header_()
	    , packets_()
	{}

	/// <summary>
	/// Construct a RequestMessage using the given pointer and size (For deserializing from recvfrom)
	/// </summary>
	/// <param name="packet">Pointer to memory containing serialized RequestMessage</param>
	/// <param name="size">Size of the memory area</param>
	RequestMessage(void* packet, size_t size)
	    : header_(), packets_()
	{
		assert(size >= sizeof(RequestHeader));
		memcpy(&header_, packet, sizeof(RequestHeader));
		TLOG(11) << "Request header word: 0x" << std::hex << header_.header << std::dec << ", packet_count: " << header_.packet_count
		         << ", run number: " << header_.run_number;

		if (header_.isValid())
		{
			packets_.reserve(header_.packet_count);

			uint8_t* ptr = reinterpret_cast<uint8_t*>(packet) + sizeof(RequestHeader);
			uint8_t* end = reinterpret_cast<uint8_t*>(packet) + size;
			for (size_t ii = 0; ii < header_.packet_count; ++ii)
			{
				assert(ptr < end);
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

	/// <summary>
	/// Get the run number in the header for this reqeust
	/// </summary>
	/// <returns>Run number stored in the RequestHeader</returns>
	uint32_t getRunNumber() const { return header_.run_number; }

	/// <summary>
	/// Get the RequestMessageMode of this RequestMessage
	/// </summary>
	/// <returns>RequestMessageMode stored in the RequestHeader</returns>
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

	/// <summary>
	/// Add a RequestPacket to the RequestMessage
	/// </summary>
	/// <param name="packet">RequestPacket to add</param>
	void addRequest(const RequestPacket& packet)
	{
		packets_.push_back(packet);
	}

	/// <summary>
	/// Set the request_acknowledgement bit of the RequestHeader
	/// </summary>
	/// <param name="ack">Value to set</param>
	void setAcknowledge(bool ack)
	{
		header_.request_acknowledgement = ack;
	}

	/// <summary>
	/// Get the request_acknowledgement bit from the RequestHeader
	/// </summary>
	/// <returns>Value of the request_acknowledgement bit</returns>
	bool getAcknowledge()
	{
		return header_.request_acknowledgement;
	}

	/// <summary>
	/// Check the headers of the header and each packet
	/// </summary>
	/// <returns>True if all headers are valid, false otherwise</returns>
	bool isValid()
	{
		if (!header_.isValid()) return false;

		for (auto& packet : packets_)
		{
			if (!packet.isValid()) return false;
		}

		return true;
	}

	/// <summary>
	/// Get the requests in this RequestMessage
	/// </summary>
	/// <returns>Vector of RequestPacket instances</returns>
	std::vector<RequestPacket> const& getRequests() const { return packets_; }

private:
	RequestHeader header_;
	std::vector<RequestPacket> packets_;
};
}  // namespace detail
}  // namespace artdaq

#endif  // artdaq_DAQrate_detail_RequestMessage
