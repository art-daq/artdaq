
cet_test(GenericFragmentSimulator_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq_core::artdaq-core_Plugins
  artdaq_core::artdaq-core_Data
  fhiclcpp::fhiclcpp
  )
  
cet_test(tracemf_t HANDBUILT
  TEST_EXEC tracemf
  TEST_ARGS -csutdl 100000
  )
