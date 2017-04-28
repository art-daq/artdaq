
cet_test(RoundRobin_policy_t USE_BOOST_UNIT
  LIBRARIES artdaq_Application_Routing
  )
cet_test(NoOp_policy_t USE_BOOST_UNIT
  LIBRARIES artdaq_Application_Routing
  )
cet_test(CapacityTest_policy_t USE_BOOST_UNIT
LIBRARIES artdaq_Application_Routing
)
cet_test(routing_master_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS -hosts localhost -np 3 routing_master -c routing_master_t.fcl
  DATAFILES routing_master_t.fcl
  )
