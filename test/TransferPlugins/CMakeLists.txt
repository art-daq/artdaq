
# This is an integration test that exercises SHandles and RHandles,
# making sure they can communicate as expected.
cet_test(transfer_driver_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS -hosts localhost -np 5 transfer_driver_mpi transfer_driver_mpi.fcl
  DATAFILES fcl/transfer_driver_mpi.fcl
  )

