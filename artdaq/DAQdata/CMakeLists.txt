cet_make_library(SOURCE
  Globals.cc
  PortManager.cc
  TCPConnect.cc
  TCP_listen_fd.cc
  LIBRARIES PUBLIC
  artdaq-utilities::Plugins
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

install_headers()
install_source()
