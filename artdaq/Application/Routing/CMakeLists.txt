
art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_Application_Routing
  NO_PLUGINS
  EXCLUDE RoundRobin_policy.cc
  LIB_LIBRARIES
  artdaq-core_Utilities
  artdaq-core_Data
  cetlib
  ${CETLIB_EXCEPT}
  fhiclcpp
  MF_MessageLogger
  MF_Utilities
  )

simple_plugin(RoundRobin "policy"
  artdaq_Application_Routing
)
simple_plugin(NoOp "policy"
  artdaq_Application_Routing
)
