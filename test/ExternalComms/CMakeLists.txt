
# This is an integration test that exercises SHandles and RHandles,
# making sure they can communicate as expected.
#cet_test(transfer_driver_t HANDBUILT
#  TEST_EXEC mpirun
#  TEST_ARGS -hosts localhost -np 5 transfer_driver_mpi transfer_driver_mpi.fcl
#  DATAFILES fcl/transfer_driver_mpi.fcl
#  TEST_PROPERTIES RUN_SERIAL 1
#  )

#cet_test(transfer_driver_validate_t HANDBUILT
#  TEST_EXEC mpirun
#  TEST_ARGS -hosts localhost -np 5 transfer_driver_mpi transfer_driver_mpi_validate.fcl
#  DATAFILES fcl/transfer_driver_mpi_validate.fcl
#  TEST_PROPERTIES RUN_SERIAL 1
#  )

cet_make_exec(NAME commander_test # NO_INSTALL -- comment out to install
  SOURCE
  commander_test.cc
  LIBRARIES
  artdaq_plugin_support::commanderMaker
  artdaq_plugin_types::commander
  artdaq::Application
  artdaq::DAQdata
  artdaq_core::artdaq-core_Utilities
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  Boost::thread
)

cet_test(commander_test_xmlrpc_t HANDBUILT
	TEST_EXEC commander_test
	TEST_ARGS -c xmlrpc_commander_test.fcl
	DATAFILES fcl/xmlrpc_commander_test.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)
