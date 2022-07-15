
if (${USING_RTIDDS})
include_directories($ENV{NDDSHOME}/include $ENV{NDDSHOME}/include/ndds )
endif()

cet_make_library(SOURCE
  MakeTransferPlugin.cc
  TransferInterface.cc
  detail/Timeout.cc
  LIBRARIES
  PUBLIC
  artdaq::DAQdata
  artdaq_core::artdaq-core_Core
  artdaq_core::artdaq-core_Data
  cetlib::compiler_macros
  fhiclcpp::types
  PRIVATE
  artdaq_core::artdaq-core_Utilities
  fhiclcpp::fhiclcpp
  cetlib::cetlib
  cetlib_except::cetlib_except
)

cet_register_export_set(SET_NAME TPPluginTypes NAMESPACE artdaq_plugin_types)
cet_register_export_set(SET_NAME TPPluginSupport NAMESPACE artdaq_plugin_support)

# Export convenience aliases.
cet_make_alias(NAME transfer EXPORT_SET TPPluginTypes
  TARGET artdaq::TransferPlugins)
cet_make_alias(NAME transferMaker EXPORT_SET TPPluginSupport
  TARGET artdaq::TransferPlugins)

include(BasicPlugin)
cet_write_plugin_builder(artdaq::transfer art::plugin Modules
  SUFFIX transfer
  INSTALL_BUILDER
  LIBRARIES CONDITIONAL artdaq_plugin_types::transfer)

include(artdaq::transfer)

cet_build_plugin(Shmem artdaq::transfer
  IMPL_SOURCE ShmemTransfer.cc
  LIBRARIES PRIVATE
  Boost::headers
)

cet_build_plugin(Null artdaq::transfer)

cet_build_plugin(Multicast artdaq::transfer)

cet_build_plugin(TCPSocket artdaq::transfer
  IMPL_SOURCE TCPSocketTransfer.cc)

cet_build_plugin(Autodetect artdaq::transfer
  LIBRARIES PRIVATE
  artdaq::TransferPlugins_Shmem_transfer
  artdaq::TransferPlugins_TCPSocket_transfer
)

if (${USING_RTIDDS})
  cet_build_plugin(RTIDDS artdaq::transfer
    LIBRARIES PRIVATE artdaq::RTIDDS)
endif()

install_headers(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
install_source(SUBDIRS detail)
