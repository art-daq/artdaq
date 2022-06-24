cet_make_library(SOURCE
  Globals.cc
  PortManager.cc
  TCPConnect.cc
  TCP_listen_fd.cc
  LIBRARIES
  PUBLIC
  artdaq_utilities::artdaq-utilities_Plugins
  fhiclcpp::types
  Threads::Threads
  TRACE::MF
)

cet_make_library(LIBRARY_NAME HostMap INTERFACE
  SOURCE HostMap.hh
  LIBRARIES INTERFACE
  fhiclcpp::types
  fhiclcpp::fhiclcpp
)

include(artdaq::generator)
cet_build_plugin(GenericFragmentSimulator artdaq::generator
  LIBRARIES
  PUBLIC
  artdaq_plugin_types::generator
  artdaq_core::artdaq-core_Data
  fhiclcpp::types
  PRIVATE
  fhiclcpp::fhiclcpp
)

install_headers()
install_source()
