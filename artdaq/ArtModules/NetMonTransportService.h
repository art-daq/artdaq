#ifndef artdaq_ArtModules_NetMonTransportService_h
#define artdaq_ArtModules_NetMonTransportService_h

#include "art/Framework/Services/Registry/ServiceMacros.h"

#include "artdaq/ArtModules/NetMonTransportServiceInterface.h"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq-core/Core/GlobalQueue.hh"

class TBufferFile;

namespace art {
class ActivityRegistry;
}

namespace fhicl {
class ParameterSet;
}

// ----------------------------------------------------------------------

class NetMonTransportService : public NetMonTransportServiceInterface {
public:
    ~NetMonTransportService();
    NetMonTransportService(fhicl::ParameterSet const&, art::ActivityRegistry&);
    void connect();
    void disconnect();
    void listen();
    void sendMessage(uint64_t sequenceId, uint8_t messageType, TBufferFile &);
    void receiveMessage(TBufferFile *&);
  size_t dataReceiverCount() {return sender_ptr_->destinationCount();}
private:
  fhicl::ParameterSet data_pset_;

    std::unique_ptr<artdaq::DataSenderManager> sender_ptr_;
    artdaq::RawEventQueue &incoming_events_;
    std::unique_ptr<std::vector<artdaq::Fragment> > recvd_fragments_;
  std::vector<artdaq::Fragment>::const_iterator frag_it_;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(NetMonTransportService, NetMonTransportServiceInterface, LEGACY)
#endif /* artdaq_ArtModules_NetMonTransportService_h */

// Local Variables:
// mode: c++
// End:
