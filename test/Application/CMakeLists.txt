
cet_test(CommandableFragmentGenerator_t USE_BOOST_UNIT
  LIBRARIES artdaq_Application
  artdaq-core_Data
  )
  
cet_test(RoutingMasterCore_t USE_BOOST_UNIT
  LIBRARIES artdaq_Application
  artdaq_Application_Routing
  )

  add_subdirectory(Routing)