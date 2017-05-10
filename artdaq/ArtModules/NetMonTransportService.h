#ifndef artdaq_ArtModules_NetMonTransportService_h
#define artdaq_ArtModules_NetMonTransportService_h

#include "art/Framework/Services/Registry/ServiceMacros.h"

#include "artdaq/ArtModules/NetMonTransportServiceInterface.h"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq-core/Core/GlobalQueue.hh"

class TBufferFile;

namespace art
{
	class ActivityRegistry;
}

namespace fhicl
{
	class ParameterSet;
}

// ----------------------------------------------------------------------

class NetMonTransportService : public NetMonTransportServiceInterface
{
public:
	virtual ~NetMonTransportService();

	NetMonTransportService(fhicl::ParameterSet const&, art::ActivityRegistry&);

	void connect() override;

	void disconnect() override;

	void listen() override;

	void sendMessage(uint64_t sequenceId, uint8_t messageType, TBufferFile&) override;

	void receiveMessage(TBufferFile*&) override;

	size_t dataReceiverCount() const { return sender_ptr_->destinationCount(); }
private:
	fhicl::ParameterSet data_pset_;

	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_;
	artdaq::RawEventQueue& incoming_events_;
	std::unique_ptr<std::vector<artdaq::Fragment>> recvd_fragments_;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(NetMonTransportService, NetMonTransportServiceInterface, LEGACY)
#endif /* artdaq_ArtModules_NetMonTransportService_h */

// Local Variables:
// mode: c++
// End:
