art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_DAQdata
  EXCLUDE GenericFragmentSimulator_generator.cc
  LIB_LIBRARIES
  artdaq_utilities::artdaq-utilities_Plugins
  artdaq_core::artdaq-core_Utilities
  ${FHICLCPP}
  ${CETLIB}
  ${CETLIB_EXCEPT}
  MF_MessageLogger
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  )

simple_plugin(GenericFragmentSimulator "generator"
  artdaq_DAQdata
  artdaq_core::artdaq-core_Data
  ${CETLIB_EXCEPT}
  )

install_headers()
install_source()
