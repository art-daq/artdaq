include_directories(${MPI_INCLUDE_PATH})

art_make_exec(StateResponder
  LIBRARIES
  artdaq_Application_MPI2
  artdaq_Application
  artdaq_ExternalComms
  ${MPI_C_LIBRARIES}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  )

art_make_exec(genToArt
  LIBRARIES
  artdaq_Application_MPI2
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq-core_Utilities
  artdaq-core_Generators
  ${ART_FRAMEWORK_ART}
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  )

cet_test(genToArt_t HANDBUILT
  TEST_EXEC genToArt
  TEST_ARGS -c genToArt_t.fcl
  DATAFILES fcl/genToArt_t.fcl
)

cet_test(genToArt_outToBinaryFileOutput_t HANDBUILT
  TEST_EXEC genToArt
  TEST_ARGS -c genToArt_outToBinaryFileOutput_t.fcl
  DATAFILES fcl/genToArt_outToBinaryFileOutput_t.fcl
)

cet_test(genToArt_outToBinaryMPIOutput_t HANDBUILT
  TEST_EXEC mpirun
  TEST_ARGS genToArt -c genToArt_outToBinaryMPIOutput_t.fcl :  s_r_handles 1 
  DATAFILES fcl/genToArt_outToBinaryMPIOutput_t.fcl)
  
install_source()
