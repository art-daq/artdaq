
cet_make_library(SOURCE
  MakeTransferPlugin.cc
  TransferInterface.cc
  detail/Timeout.cc
  LIBRARIES
  PUBLIC
  artdaq::DAQdata
  artdaq-core::Core
  artdaq-core::Data
  cetlib::compiler_macros
  fhiclcpp::types
  PRIVATE
  artdaq-core::Utilities
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

cet_build_plugin(Null artdaq::transfer)

cet_build_plugin(Multicast artdaq::transfer)

cet_build_plugin(TCPSocket artdaq::transfer
  IMPL_SOURCE TCPSocketTransfer.cc)

cet_build_plugin(TCPSocketWithAcks artdaq::Transfer IMPL_SOURCE TCPSocketTransfer.cc)

cet_build_plugin(Bundle artdaq::transfer
  LIBRARIES PRIVATE
  artdaq::TransferPlugins_TCPSocket_transfer
)

install_headers(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
install_source(SUBDIRS detail)
