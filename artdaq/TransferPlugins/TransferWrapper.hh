#ifndef artdaq_ArtModules_TransferWrapper_hh
#define artdaq_ArtModules_TransferWrapper_hh

// JCF, May-27-2016

// This is the class through which code that wants to access a
// transfer plugin (e.g., input sources, AggregatorCore, etc.) can do
// so. Its functionality is such that it satisfies the requirements
// needed to be a template in the ArtdaqInput class

#include <string>
#include <memory>
#include <iostream>

#include "artdaq/TransferPlugins/TransferInterface.hh"

#include "TBufferFile.h"

namespace fhicl {
  class ParameterSet;
}

namespace artdaq {

  class Fragment;

  class TransferWrapper {
  public:

    TransferWrapper(const fhicl::ParameterSet& );
    ~TransferWrapper();

    void receiveMessage(std::unique_ptr<TBufferFile>& msg);

  private:

    void extractTBufferFile(const artdaq::Fragment&, std::unique_ptr<TBufferFile>& );

    void checkIntegrity(const artdaq::Fragment& ) const;

    void unregisterMonitor();

    std::size_t timeoutInUsecs_;
    std::unique_ptr<TransferInterface> transfer_;
    const std::string dispatcherHost_;
    const std::string dispatcherPort_;
    const std::string serverUrl_;
    const std::size_t maxEventsBeforeInit_;
    const std::vector<int> allowedFragmentTypes_;
    const bool quitOnFragmentIntegrityProblem_;
    const size_t debugLevel_;
    bool monitorRegistered_;
  };

}

#endif /* artdaq_ArtModules_TransferWrapper_hh */
