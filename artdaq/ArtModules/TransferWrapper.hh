#ifndef artdaq_ArtModules_TransferWrapper_hh
#define artdaq_ArtModules_TransferWrapper_hh

// JCF, May-27-2016

// This is the class through which code that wants to access a
// transfer plugin (e.g., input sources, AggregatorCore, etc.) can do
// so. Its functionality is such that it satisfies the requirements
// needed to be a template in the ArtdaqInput class

#include <string>
#include <memory>

#include "artdaq/ArtModules/TransferInterface.h"
#include "TBufferFile.h"

namespace artdaq {

  class Fragment;

  class TransferWrapper {
  public:
  
    TransferWrapper(const std::string& transferPluginName);

    void receiveMessage(std::unique_ptr<TBufferFile>& msg);

  private:

    void extractTBufferFile(const artdaq::Fragment&, std::unique_ptr<TBufferFile>& );

    std::unique_ptr<TransferInterface> transfer_;
  };

}

#endif /* artdaq_ArtModules_TransferWrapper_hh */
