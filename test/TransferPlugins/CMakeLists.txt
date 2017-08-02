
# This is an integration test that exercises SHandles and RHandles,
# making sure they can communicate as expected.
cet_test(transfer_driver_t HANDBUILT
    TEST_EXEC runMPITransferTest.sh
  TEST_ARGS transfer_driver_mpi.fcl 5
  DATAFILES fcl/transfer_driver_mpi.fcl
  TEST_PROPERTIES RUN_SERIAL 1
  )

cet_test(transfer_driver_validate_t HANDBUILT
    TEST_EXEC runMPITransferTest.sh
  TEST_ARGS transfer_driver_mpi_validate.fcl 5
  DATAFILES fcl/transfer_driver_mpi_validate.fcl
  TEST_PROPERTIES RUN_SERIAL 1
  )

  cet_script(runTransferTest.sh runMPITransferTest.sh)

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
