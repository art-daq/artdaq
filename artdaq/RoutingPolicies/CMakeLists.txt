
art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_RoutingPolicies
  NO_PLUGINS
  EXCLUDE RoundRobin_policy.cc NthEvent_policy.cc NoOp_policy.cc CapacityTest_policy.cc
  LIB_LIBRARIES
  artdaq_DAQdata
  artdaq-core_Utilities
  artdaq-core_Data
  cetlib
  ${CETLIB_EXCEPT}
  ${Boost_SYSTEM_LIBRARY}
  fhiclcpp
  MF_MessageLogger
  )

simple_plugin(RoundRobin "policy"
  artdaq_RoutingPolicies
)
simple_plugin(NoOp "policy"
  artdaq_RoutingPolicies
)
simple_plugin(CapacityTest "policy"
artdaq_RoutingPolicies
)
simple_plugin(PreferSameHost "policy"
artdaq_RoutingPolicies
)


install_headers()
install_fhicl(SUBDIRS fcl)
install_source()
