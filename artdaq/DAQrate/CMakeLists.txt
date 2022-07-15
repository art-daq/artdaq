# ======================================================================
#
# Build/install script
#
# ======================================================================

# ----------------------------------------------------------------------
# Build this project's library:

cet_make_library(SOURCE
  DataReceiverManager.cc
  DataSenderManager.cc
  FragmentBuffer.cc
  FragmentReceiverManager.cc
  RequestBuffer.cc
  SharedMemoryEventManager.cc
  StatisticsHelper.cc
  TransferTest.cc
  detail/RequestReceiver.cc
  detail/RequestSender.cc
  detail/TableReceiver.cc
  detail/TokenReceiver.cc
  detail/TokenSender.cc
  LIBRARIES
  PUBLIC
  artdaq::DAQdata
  artdaq_core::artdaq-core_Core
  artdaq_core::artdaq-core_Data
  TRACE::MF
  Boost::thread
  PRIVATE
  canvas::canvas
  fhiclcpp::fhiclcpp
  artdaq::TransferPlugins
  artdaq_plugin_types::policy
  Threads::Threads
)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
