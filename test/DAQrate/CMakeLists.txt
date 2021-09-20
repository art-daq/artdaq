cet_make_exec(NAME GenToBuffer_t SOURCE GenToBuffer_t.cc 
	LIBRARIES artdaq_DAQrate
	artdaq_Generators
  ${Boost_SYSTEM_LIBRARY}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  fhiclcpp)

cet_test(FragCounter_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate
  )

  # DataSenderManager is tested as part of the TransferTest

  cet_test(DataReceiverManager_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate
  artdaq_TransferPlugins
  artdaq_TransferPlugins_Shmem_transfer
  )

  cet_test(RequestSender_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate
  )

  cet_test(SharedMemoryEventManager_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate
  )

# Generate several fragments and verify they made it into the store.
cet_test(requestsender HANDBUILT
  TEST_EXEC requestSender
  TEST_ARGS -c requestsender.fcl
  DATAFILES requestsender.fcl
  )

  
cet_test(FragmentBuffer_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate 
  artdaq_Generators
  artdaq_core::artdaq-core_Data
  )
