
cet_test(GenericFragmentSimulator_t USE_BOOST_UNIT INSTALL_BIN
  LIBRARIES PRIVATE
  artdaq-core::Plugins
  artdaq-core::Data
  fhiclcpp::fhiclcpp
  )
  
cet_test(tracemf_t HANDBUILT
  TEST_EXEC tracemf
  TEST_ARGS -csutdl 100000
  )
