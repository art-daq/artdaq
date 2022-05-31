
cet_test(GenericFragmentSimulator_t USE_BOOST_UNIT
  LIBRARIES artdaq_core::artdaq-core_Plugins
  )
  
cet_test(tracemf_t HANDBUILT
  TEST_EXEC tracemf
  TEST_ARGS -csutdl 100000
  )