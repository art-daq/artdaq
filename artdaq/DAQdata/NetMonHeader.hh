#ifndef artdaq_DAQdata_NetMonHeader_hh
#define artdaq_DAQdata_NetMonHeader_hh

#include <cstdint>

namespace artdaq {
struct NetMonHeader;
}

/**
 * \brief Header with length information for NetMonTransport messages
 */
struct artdaq::NetMonHeader
{
	enum class MessageType : uint32_t {
		Invalid = 0,
		Init = 1,
		Run = 2,
		Subrun = 3,
		Event = 4,
	};
	uint64_t data_length{0};  ///< The length of the message
};

#endif /* artdaq_DAQdata_NetMonHeader_hh */
