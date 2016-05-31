#ifndef artdaq_ArtModules_NetMonWrapper_hh
#define artdaq_ArtModules_NetMonWrapper_hh

// JCF, May-27-2016

// This class is written with functionality such that it satisfies the
// requirements needed to be a template in the ArtdaqInput class

#include "artdaq/ArtModules/NetMonTransportService.h"

#include "art/Framework/Services/Registry/ServiceHandle.h"

#include "TBufferFile.h"

#include <string>
#include <memory>

namespace fhicl {
  class ParameterSet;
}

namespace art {

  class NetMonWrapper {
  public:
  
    // JCF, May-31-2016

    // Parameter set constructor argument is unused for now, but
    // needed for this class to implement the interface the
    // ArtdaqInput templatized input source expects

    NetMonWrapper(const fhicl::ParameterSet& ) {
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
