include_directories(${MPI_INCLUDE_PATH})

set(CPI_SOURCE ${MPI_INCLUDE_PATH}/../share/examples_graphics/cpi.c)
if (EXISTS ${CPI_SOURCE})
  cet_test(cpi_t SOURCES ${CPI_SOURCE} LIBRARIES ${MPI_C_LIBRARIES})
endif()

string(RANDOM LENGTH 5 ALPHABET 0123456789 key)

cet_test(daqrate_gen_test HANDBUILT
  TEST_EXEC daqrate.py
  DATAFILES fcl/daqrate_gen_test.fcl
  TEST_ARGS 5 1 -- -c daqrate_gen_test.fcl -k ${key}
  #TEST_PROPERTIES RUN_SERIAL true
  )

cet_test(FragCounter_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate)

  # DataSenderManager is tested as part of the TransferTest

  cet_test(DataReceiverManager_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate
  artdaq_TransferPlugins
  artdaq_TransferPlugins_Shmem_transfer)

  cet_test(RequestSender_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate)

  cet_test(SharedMemoryEventManager_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate)