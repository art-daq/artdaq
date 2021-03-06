
cet_make_exec(NAME transfer_driver # NO_INSTALL -- comment out to install
  SOURCE
  transfer_driver.cc
  LIBRARIES
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq::TransferPlugins
  artdaq_core::artdaq-core_Utilities
)

cet_make_exec(NAME broken_transfer_driver 
  SOURCE broken_transfer_driver.cc BrokenTransferTest.cc 
  LIBRARIES 
  artdaq::DAQrate 
  artdaq::DAQdata 
  artdaq::TransferPlugins 
  artdaq_core::artdaq-core_Utilities
  Boost::program_options
)

cet_script(ALWAYS_COPY runTransferTest.sh runBrokenTransferTest.sh)

cet_test(transfer_driver_tcp_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp.fcl 2
	DATAFILES fcl/transfer_driver_tcp.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_shmem_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem.fcl 2
	DATAFILES fcl/transfer_driver_shmem.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_tcp_validate_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp_validate.fcl 2
	DATAFILES fcl/transfer_driver_tcp_validate.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_shmem_validate_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem_validate.fcl 2
	DATAFILES fcl/transfer_driver_shmem_validate.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_tcp_multi HANDBUILT
  TEST_EXEC runTransferTest.sh
  TEST_ARGS transfer_driver_tcp_multi.fcl 2
  DATAFILES fcl/transfer_driver_tcp_multi.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

# cet_test(transfer_driver_shmem_multi HANDBUILT
#     TEST_EXEC runTransferTest.sh
#     TEST_ARGS transfer_driver_shmem_multi.fcl 2
#     DATAFILES fcl/transfer_driver_shmem_multi.fcl
# #TEST_PROPERTIES RUN_SERIAL 1
#     )

cet_test(transfer_driver_tcp_complex HANDBUILT
  TEST_EXEC runTransferTest.sh
  TEST_ARGS transfer_driver_tcp_complex.fcl 4
  DATAFILES fcl/transfer_driver_tcp_complex.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_shmem_complex HANDBUILT
  TEST_EXEC runTransferTest.sh
  TEST_ARGS transfer_driver_shmem_complex.fcl 4
  DATAFILES fcl/transfer_driver_shmem_complex.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_tcp_empty_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp_empty.fcl 2
	DATAFILES fcl/transfer_driver_tcp_empty.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_shmem_empty_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem_empty.fcl 2
	DATAFILES fcl/transfer_driver_shmem_empty.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_tcp_nonblocking_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp_nonblocking.fcl 2
	DATAFILES fcl/transfer_driver_tcp_nonblocking.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)
  
cet_test(transfer_driver_shmem_nonblocking_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem_nonblocking.fcl 2
	DATAFILES fcl/transfer_driver_shmem_nonblocking.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_tcp_nonblocking_broadcast_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp_nonblocking_broadcast.fcl 4
	DATAFILES fcl/transfer_driver_tcp_nonblocking_broadcast.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_shmem_nonblocking_broadcast_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem_nonblocking_broadcast.fcl 4
	DATAFILES fcl/transfer_driver_shmem_nonblocking_broadcast.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_tcp_broadcast_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_tcp_broadcast.fcl 4
	DATAFILES fcl/transfer_driver_tcp_broadcast.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(transfer_driver_shmem_broadcast_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS transfer_driver_shmem_broadcast.fcl 4
	DATAFILES fcl/transfer_driver_shmem_broadcast.fcl
  #TEST_PROPERTIES RUN_SERIAL 1
)

cet_test(broken_transfer_driver_shmem_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_shmem.fcl
	DATAFILES fcl/broken_transfer_driver_shmem.fcl
)

cet_test(broken_transfer_driver_shmem_equalbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_shmem_equalbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_shmem_equalbuffers.fcl
)

cet_test(broken_transfer_driver_shmem_moretransferbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_shmem_moretransferbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_shmem_moretransferbuffers.fcl
)

cet_test(broken_transfer_driver_shmem_nonblocking_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_shmem_nonblocking.fcl
	DATAFILES fcl/broken_transfer_driver_shmem_nonblocking.fcl
)

cet_test(broken_transfer_driver_shmem_nonblocking_equalbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_shmem_nonblocking_equalbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_shmem_nonblocking_equalbuffers.fcl
)

cet_test(broken_transfer_driver_shmem_nonblocking_moretransferbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_shmem_nonblocking_moretransferbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_shmem_nonblocking_moretransferbuffers.fcl
)

cet_test(broken_transfer_driver_tcp_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_tcp.fcl
	DATAFILES fcl/broken_transfer_driver_tcp.fcl
)

cet_test(broken_transfer_driver_tcp_equalbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_tcp_equalbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_tcp_equalbuffers.fcl
)

cet_test(broken_transfer_driver_tcp_moretransferbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_tcp_moretransferbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_tcp_moretransferbuffers.fcl
)

cet_test(broken_transfer_driver_tcp_nonblocking_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_tcp_nonblocking.fcl
	DATAFILES fcl/broken_transfer_driver_tcp_nonblocking.fcl
)

cet_test(broken_transfer_driver_tcp_nonblocking_equalbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_tcp_nonblocking_equalbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_tcp_nonblocking_equalbuffers.fcl
)

cet_test(broken_transfer_driver_tcp_nonblocking_moretransferbuffers_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS broken_transfer_driver_tcp_nonblocking_moretransferbuffers.fcl
	DATAFILES fcl/broken_transfer_driver_tcp_nonblocking_moretransferbuffers.fcl
)
