
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

if(NOT ${CMAKE_INSTALL_PREFIX} MATCHES /scratch/workspace/artdaq-release-build)

file(GLOB broken_tests "fcl/broken_transfer_driver_*.fcl")
foreach(testfile ${broken_tests})
	get_filename_component(conffile ${testfile} NAME)
	get_filename_component(test_name ${testfile} NAME_WE)
	cet_test(${test_name}_t HANDBUILT
	TEST_EXEC runBrokenTransferTest.sh
	TEST_ARGS ${conffile}
	DATAFILES ${testfile}
	)
endforeach()

file(GLOB transfer_tests "fcl/transfer_driver_*.fcl")
foreach(testfile ${transfer_tests})

	set(procs "0")
	file(STRINGS ${testfile} numprocs REGEX ^num_)
	foreach(line ${numprocs})
		string(REGEX MATCH "[0-9]+" numprocs_d ${line})
		math(EXPR procs "${procs} + ${numprocs_d}")
	endforeach()
	
	get_filename_component(conffile ${testfile} NAME)
	get_filename_component(test_name ${testfile} NAME_WE)
	cet_test(${test_name}_t HANDBUILT
	TEST_EXEC runTransferTest.sh
	TEST_ARGS ${conffile} ${procs}
	DATAFILES ${testfile}
	#TEST_PROPERTIES RUN_SERIAL 1
	)
endforeach()

endif()