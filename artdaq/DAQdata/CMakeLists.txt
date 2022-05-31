cet_make(LIBRARY_NAME artdaq_DAQData  
  EXCLUDE GenericFragmentSimulator_generator.cc
  EXPORT_SET artdaq
  LIBRARIES
  artdaq_utilities::artdaq-utilities_Plugins
  artdaq_core::artdaq-core_Utilities
  )

  include(generator)
  cet_build_plugin(GenericFragmentSimulator generator LIBRARIES REG
  artdaq::artdaq_DAQdata
  artdaq_core::artdaq-core_Data
  )

install_headers()
install_source()
