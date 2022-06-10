# ======================================================================
#
# Build/install script
#
# ======================================================================

# ----------------------------------------------------------------------
# Build this project's library:

cet_register_export_set(SET_NAME CFGPluginTypes NAMESPACE artdaq_plugin_types)
cet_register_export_set(SET_NAME CFGPluginSupport NAMESPACE artdaq_plugin_support)

cet_make_library(LIBRARY_NAME CommandableFragmentGenerator
  EXPORT_SET CFGPluginTypes
  SOURCE
  CommandableFragmentGenerator.cc
  LIBRARIES
  PUBLIC
  artdaq::DAQrate
  artdaq_core::artdaq-core_Data
  artdaq_plugin_types::generator
  fhiclcpp::types
  Boost::thread
  TRACE::MF
  Threads::Threads
  PRIVATE
  artdaq_core::artdaq-core_Utilities
  fhiclcpp::fhiclcpp
  cetlib_except::cetlib_except
)

cet_make_library(LIBRARY_NAME CFGMaker
  EXPORT_SET CFGPluginSupport
  SOURCE
  makeCommandableFragmentGenerator.cc
  LIBRARIES
  PUBLIC
  artdaq_plugin_types::CommandableFragmentGenerator
  PRIVATE
  cetlib::cetlib
)

cet_write_plugin_builder(artdaq::commandableGenerator artdaq::generator Modules
  INSTALL_BUILDER
  LIBRARIES CONDITIONAL artdaq_plugin_types::CommandableFragmentGenerator)

include(artdaq::commandableGenerator)

cet_build_plugin(CompositeDriver artdaq::commandableGenerator
  LIBRARIES PRIVATE
  artdaq_plugin_support::CFGMaker
  canvas::canvas
  cetlib_except::cetlib_except
  Boost::headers
)

install_headers()
install_source()
