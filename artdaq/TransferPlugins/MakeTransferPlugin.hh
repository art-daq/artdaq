#ifndef artdaq_TransferPlugins_MakeTransferPlugin_hh
#define artdaq_TransferPlugins_MakeTransferPlugin_hh

// JCF, Sep-6-2016

// MakeTransferPlugin expects the following arguments:

// A FHiCL parameter set which contains within it a table defining a
// transfer plugin

// The name of that table

// The send/receive role of the plugin

#include "artdaq/TransferPlugins/TransferInterface.hh"

#include <memory>

namespace artdaq {
/**
	 * \brief Load a TransferInterface plugin
	 * \param pset ParameterSet used to configure the TransferInterface
	 * \param plugin_label Name of the plugin
	 * \param role Whether the TransferInterface should be configured as kSend or kReceive
	 * \return Pointer to the new TransferInterface instance
	 */
std::unique_ptr<TransferInterface>
MakeTransferPlugin(const fhicl::ParameterSet& pset,
                   const std::string& plugin_label,
                   TransferInterface::Role role);
}  // namespace artdaq

#endif
