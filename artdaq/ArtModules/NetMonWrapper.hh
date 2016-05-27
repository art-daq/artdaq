#ifndef artdaq_ArtModules_NetMonWrapper_hh
#define artdaq_ArtModules_NetMonWrapper_hh

// JCF, May-27-2016

// This class is written with functionality such that it satisfies the
// requirements needed to be a template in the InputArtdaq class

#include "artdaq/ArtModules/NetMonTransportService.h"

#include "art/Framework/Services/Registry/ServiceHandle.h"

#include "TBufferFile.h"

#include <string>
#include <memory>

namespace art {

  class NetMonWrapper {
  public:
  
    NetMonWrapper() {
      ServiceHandle<NetMonTransportService> transport;
      transport->listen();
    }

    ~NetMonWrapper() {
      ServiceHandle<NetMonTransportService> transport;
      transport->disconnect();
    }

    void receiveMessage(std::unique_ptr<TBufferFile>& msg);

  };

}

#endif /* artdaq_ArtModules_NetMonWrapper_hh */
