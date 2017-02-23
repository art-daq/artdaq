art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_DAQdata
  EXCLUDE GenericFragmentSimulator_generator.cc
  LIB_LIBRARIES
  ${ARTDAQ-CORE_DATA}
  ${ARTDAQ-CORE_GENERATORS}
  ${ART_PERSISTENCY_COMMON}
  ${ART_UTILITIES}
  ${FHICLCPP}
  ${CETLIB}
  ${MF_MESSAGELOGGER}
  ${MF_UTILITIES}
  )

simple_plugin(GenericFragmentSimulator "generator"
  artdaq_DAQdata 
  artdaq-core_Data
  ${ARTDAQ-CORE_GENERATORS}
  ${CETLIB_EXCEPT}
  )

install_headers()
install_source()
