#ifndef ARTDAQ_ARTDAQ_DAQDATA_NETMONHEADER_HH_
#define ARTDAQ_ARTDAQ_DAQDATA_NETMONHEADER_HH_

#include <cstdint>

namespace artdaq {
struct NetMonHeader;
}

/**
 * \brief Header with length information for NetMonTransport messages
 */
struct artdaq::NetMonHeader
{
	enum class MessageType : uint32_t
	{
		Invalid = 0,
		Init = 1,
		Run = 2,
		Subrun = 3,
		Event = 4,
	};
	uint64_t data_length{0};  ///< The length of the message
};

#endif  // ARTDAQ_ARTDAQ_DAQDATA_NETMONHEADER_HH_
