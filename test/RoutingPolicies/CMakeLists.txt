
cet_test(RoundRobin_policy_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq_plugin_support::policyMaker
  artdaq_plugin_types::policy
  fhiclcpp::fhiclcpp
)

cet_test(NoOp_policy_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq_plugin_support::policyMaker
  artdaq_plugin_types::policy
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_test(CapacityTest_policy_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq_plugin_support::policyMaker
  artdaq_plugin_types::policy
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_test(PreferSameHost_policy_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq_plugin_support::policyMaker
  artdaq_plugin_types::policy
  fhiclcpp::fhiclcpp
  TRACE::MF
)
