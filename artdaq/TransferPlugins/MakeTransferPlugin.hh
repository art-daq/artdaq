#ifndef artdaq_TransferPlugins_MakeTransferPlugin_hh
#define artdaq_TransferPlugins_MakeTransferPlugin_hh

// JCF, Sep-6-2016

// MakeTransferPlugin expects the following arguments:

// A FHiCL parameter set which contains within it a table defining a
// transfer plugin

// The name of that table

// The send/receive role of the plugin

#include "TransferInterface.hh"

#include "fhiclcpp/fwd.h"

#include <memory>
#include <string>

namespace artdaq {

  std::unique_ptr<TransferInterface> 
  MakeTransferPlugin(const fhicl::ParameterSet& pset,
		     std::string plugin_label,
		     TransferInterface::Role role);

}

#endif 
