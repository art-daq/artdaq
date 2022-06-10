include(ProcessSmc)

cet_make_library(LIBRARY_NAME_VAR APP_TARGET
  SOURCE
  Commandable.cc
  BoardReaderApp.cc
  BoardReaderCore.cc
  EventBuilderCore.cc
  EventBuilderApp.cc
  DataLoggerApp.cc
  DataLoggerCore.cc
  DataReceiverCore.cc
  DispatcherApp.cc
  DispatcherCore.cc
  RoutingManagerApp.cc
  RoutingManagerCore.cc
  LIBRARIES
  PUBLIC
  artdaq_plugin_types::CommandableFragmentGenerator
  artdaq_plugin_types::policy
  artdaq::DAQrate
  canvas::canvas
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  cetlib::cetlib
  Boost::program_options
  Threads::Threads
  TRACE::MF
  PRIVATE
  artdaq_plugin_support::CFGMaker
  artdaq_plugin_support::policyMaker
  artdaq::DAQdata
  artdaq_core::artdaq-core_Data
  artdaq_core::artdaq-core_Core
  artdaq_core::artdaq-core_Utilities
)

process_smc(${APP_TARGET} OUTPUT_DIR detail detail/Commandable.sm)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl()
