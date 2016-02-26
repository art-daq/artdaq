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
  art_Persistency_Common_dict
  art_Persistency_WrappedStdDictionaries_dict
  art_Utilities
  ${ART_VERSION}
  )

cet_test(daq_flow_t
  LIBRARIES
  ${ART_FRAMEWORK_ART}
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

# Wait for an input for a second, then exit.
cet_test(read_files_t HANDBUILT
  TEST_EXEC art_ut
  TEST_ARGS -c read_files_t.fcl
  DATAFILES read_files_t.fcl
  )

# Generate several fragments and verify they made it into the store.
cet_test(driver_t HANDBUILT
  TEST_EXEC driver
  TEST_ARGS -c driver_t.fcl
  DATAFILES driver_t.fcl
  )

