cet_make_exec(NAME GenToBuffer_t SOURCE GenToBuffer_t.cc 
	LIBRARIES PRIVATE
  artdaq::Application
	artdaq_plugin_support::CFGMaker
	artdaq_plugin_types::CommandableFragmentGenerator
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq_core::artdaq-core_Utilities
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  Boost::program_options
  Boost::thread
  TRACE::MF
  Threads::Threads
)

cet_test(FragCounter_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq::DAQrate
  canvas::canvas
)

# DataSenderManager is tested as part of the TransferTest

cet_test(DataReceiverManager_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq::DAQrate
  artdaq::TransferPlugins
  artdaq::TransferPlugins_Shmem_transfer
)

cet_test(RequestSender_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq::DAQrate
  artdaq_core::artdaq-core_Utilities
)

cet_test(SharedMemoryEventManager_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq::DAQrate
  Threads::Threads
)

# Generate several fragments and verify they made it into the store.
cet_test(requestsender HANDBUILT
  TEST_EXEC requestSender
  TEST_ARGS -c requestsender.fcl
  DATAFILES requestsender.fcl
)

cet_test(FragmentBuffer_t USE_BOOST_UNIT
  LIBRARIES PRIVATE
  artdaq::DAQrate
  artdaq_core::artdaq-core_Data
  artdaq_core::artdaq-core_Utilities
  Threads::Threads
)
