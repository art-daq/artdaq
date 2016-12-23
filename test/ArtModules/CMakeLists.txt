include_directories(${MPI_INCLUDE_PATH})

simple_plugin(FragmentSniffer "module"
  artdaq_DAQdata
  art_Framework_Core
  art_Framework_Principal
  art_Persistency_Provenance
  MF_MessageLogger
  fhiclcpp
  )

cet_test(raw_event_queue_reader_t USE_BOOST_UNIT
  LIBRARIES
  artdaq_ArtModules
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq-core_Data_dict
  art_Framework_Core
  art_Framework_Principal
  art_Persistency_Provenance
  ${CANVAS}_Persistency_Common_dict
  ${CANVAS}_Persistency_WrappedStdDictionaries_dict
  art_Utilities
  art_Version
  )

cet_test(daq_flow_t
  LIBRARIES
  art_Framework_Art
  ${MPI_C_LIBRARIES}
  artdaq_DAQdata_GenericFragmentSimulator_generator
  artdaq_Application_MPI2
  artdaq_DAQrate
  artdaq_DAQdata
  MF_MessageLogger
  fhiclcpp
  cetlib
  TEST_ARGS -c daq_flow_t.fcl
  DATAFILES daq_flow_t.fcl
  )

  cet_test(reconfigure_t
  LIBRARIES
  art_Framework_Art
  ${MPI_C_LIBRARIES}
  artdaq_DAQdata_GenericFragmentSimulator_generator
  artdaq_Application_MPI2
  artdaq_DAQrate
  artdaq_DAQdata
  MF_MessageLogger
  fhiclcpp
  cetlib
  TEST_ARGS -c daq_flow_t.fcl
  DATAFILES daq_flow_t.fcl
  )

check_ups_version(art "$ENV{ART_VERSION}" v2_00_00 PRODUCT_OLDER_VAR NO_TEST_ARG_SEPARATER)
if (NO_TEST_ARG_SEPARATER)
   set(TEST_ARG -c)
else (NO_TEST_ARG_SEPARATER)
   set(TEST_ARG -- -c)
endif()

# Wait for an input for a second, then exit.
cet_test(read_files_t HANDBUILT
  TEST_EXEC art_ut
  TEST_ARGS ${TEST_ARG} read_files_t.fcl
  DATAFILES read_files_t.fcl
  )

# Generate several fragments and verify they made it into the store.
cet_test(driver_t HANDBUILT
  TEST_EXEC artdaqDriver
  TEST_ARGS -c driver_t.fcl
  DATAFILES driver_t.fcl
  )

