
# This is an integration test that exercises SHandles and RHandles,
# making sure they can communicate as expected.
cet_test(transfer_driver_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS -hosts localhost -np 5 transfer_driver_mpi transfer_driver_mpi.fcl
  DATAFILES fcl/transfer_driver_mpi.fcl
  )

cet_test(transfer_driver_validate_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS -hosts localhost -np 5 transfer_driver_mpi transfer_driver_mpi_validate.fcl
  DATAFILES fcl/transfer_driver_mpi_validate.fcl
  )

  cet_script(runTransferTest.sh)

  cet_test(transfer_driver_tcp_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp.fcl
	DATAFILES fcl/transfer_driver_tcp.fcl
	)
	
  cet_test(transfer_driver_shmem_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem.fcl
	DATAFILES fcl/transfer_driver_shmem.fcl
	)