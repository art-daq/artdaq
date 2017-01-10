include_directories(${MPI_INCLUDE_PATH})

set(CPI_SOURCE ${MPI_INCLUDE_PATH}/../share/examples_graphics/cpi.c)
if (EXISTS ${CPI_SOURCE})
  cet_test(cpi_t SOURCES ${CPI_SOURCE} LIBRARIES ${MPI_C_LIBRARIES})
endif()

 art_make_exec(NAME transfer_driver_mpi # NO_INSTALL -- comment out to install
SOURCE
transfer_driver_mpi.cc
TransferTest.cc
 LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
   artdaq_DAQdata
artdaq_TransferPlugins
  ${FHICLCPP}
   )
 art_make_exec(NAME transfer_driver # NO_INSTALL -- comment out to install
SOURCE
transfer_driver.cc
TransferTest.cc
   LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
   artdaq_DAQdata
artdaq_TransferPlugins
  ${FHICLCPP}
   )

# This is an integration test that exercises SHandles and RHandles,
# making sure they can communicate as expected.
cet_test(transfer_driver_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS -hosts localhost -np 5 transfer_driver_mpi transfer_driver_mpi.fcl
  DATAFILES transfer_driver_mpi.fcl
  )

cet_test(daqrate_gen_test HANDBUILT
  TEST_EXEC daqrate
  DATAFILES fcl/daqrate_gen_test.fcl
  TEST_ARGS 5 1 4 11 -- -c daqrate_gen_test.fcl
  TEST_PROPERTIES RUN_SERIAL true
  )

cet_test(FragCounter_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQrate)

cet_test(EventStore_t USE_BOOST_UNIT
  LIBRARIES artdaq_DAQdata artdaq_DAQrate
  )
