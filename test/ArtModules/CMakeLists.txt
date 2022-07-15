cet_build_plugin(FragmentSniffer art::EDAnalyzer
  LIBRARIES PRIVATE
  artdaq::DAQdata
  artdaq_core::artdaq-core_Data
)
  
#get_cmake_property(_variableNames VARIABLES)
#foreach (_variableName ${_variableNames})
#    message(STATUS "${_variableName}=${${_variableName}}")
#endforeach()

cet_test(daq_flow_t
  LIBRARIES PRIVATE
  artdaq::ArtConfig
  artdaq::Application
  artdaq::DAQrate
  artdaq::DAQdata_GenericFragmentSimulator_generator
  artdaq_core::artdaq-core_Data
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  cetlib_except::cetlib_except
  TEST_ARGS -c daq_flow_t.fcl
  DATAFILES daq_flow_t.fcl
)

cet_test(reconfigure_t
  LIBRARIES
  artdaq::ArtConfig
  artdaq::Application
  artdaq::DAQrate
  artdaq::DAQdata_GenericFragmentSimulator_generator
  artdaq_core::artdaq-core_Data
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  TEST_ARGS -c daq_flow_t.fcl
  DATAFILES daq_flow_t.fcl
)

# Wait for an input for a second, then exit.
cet_test(read_files_t HANDBUILT
  TEST_EXEC art_ut
  TEST_ARGS -- -c read_files_t.fcl
  DATAFILES read_files_t.fcl
)

# Generate several fragments and verify they made it into the store.
cet_test(driver_t HANDBUILT
  TEST_EXEC artdaqDriver
  TEST_ARGS -c driver_t.fcl
  DATAFILES driver_t.fcl
)

