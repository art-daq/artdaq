#ifndef artdaq_ArtModules_NetMonTransportServiceInterface_h
#define artdaq_ArtModules_NetMonTransportServiceInterface_h

#include "art/Framework/Services/Registry/ServiceMacros.h"

class TBufferFile;

class NetMonTransportServiceInterface
{
public:
	virtual void connect() = 0;

	virtual void disconnect() = 0;

	virtual void listen() = 0;

	virtual void sendMessage(uint64_t sequenceId, uint8_t messageType, TBufferFile&) = 0;

	virtual void receiveMessage(TBufferFile*&) = 0;
};

DECLARE_ART_SERVICE_INTERFACE(NetMonTransportServiceInterface, LEGACY)
#endif /* artdaq_ArtModules_NetMonTransportServiceInterface_h */

// Local Variables:
// mode: c++
// End:
