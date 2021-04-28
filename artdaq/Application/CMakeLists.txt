#process_smc(SMC_LIB_SOURCE Commandable.sm)

include_directories("${CMAKE_BINARY_DIR}")

art_make_library( LIBRARY_NAME artdaq_Application
  SOURCE
  detail/Commandable_sm.cpp
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
  artdaq_DAQrate
  artdaq_Generators
  artdaq_core::artdaq-core_Data
  artdaq_core::artdaq-core_Utilities
  artdaq_RoutingPolicies
  MF_MessageLogger
  ${CETLIB}
  ${CETLIB_EXCEPT}
  canvas
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${FHICLCPP}
  )


install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl()
