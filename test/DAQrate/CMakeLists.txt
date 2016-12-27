include_directories(${MPI_INCLUDE_PATH})

set(CPI_SOURCE ${MPI_INCLUDE_PATH}/../share/examples_graphics/cpi.c)
if (EXISTS ${CPI_SOURCE})
  cet_test(cpi_t SOURCES ${CPI_SOURCE} LIBRARIES ${MPI_C_LIBRARIES})
endif()

 art_make_exec(NAME s_r_transfer_mpi # NO_INSTALL -- comment out to install
   LIBRARIES
  artdaq_DAQrate
   artdaq_DAQdata
artdaq_TransferPlugins
  ${FHICLCPP}
   )
 art_make_exec(NAME s_r_transfer_shmem # NO_INSTALL -- comment out to install
   LIBRARIES
  artdaq_DAQrate
   artdaq_DAQdata
artdaq_TransferPlugins
  ${FHICLCPP}
   )

# This is an integration test that exercises SHandles and RHandles,
# making sure they can communicate as expected.
set(num_sending_ranks 2)
set(total_ranks 5)
# there will be (total_ranks - num_sending_ranks) receiving ranks
cet_test(s_r_transfer_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS -hosts localhost -np ${total_ranks} s_r_transfer_mpi ${num_sending_ranks}
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
