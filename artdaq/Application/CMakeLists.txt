process_smc(SMC_LIB_SOURCE Commandable.sm)

include_directories("${CMAKE_BINARY_DIR}")

art_make_library( LIBRARY_NAME artdaq_Application
  SOURCE
  ${SMC_LIB_SOURCE}
  Commandable.cc
  configureMessageFacility
  CommandableFragmentGenerator
  makeCommandableFragmentGenerator
  LIBRARIES
  artdaq_DAQrate
  artdaq-core_Utilities
  ${ART_UTILITIES}
  ${MF_MESSAGELOGGER}
  ${MF_UTILITIES}
  ${CETLIB}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${FHICLCPP}
  ${TRACE}
  )

simple_plugin(CompositeDriver "generator" artdaq_DAQdata artdaq_Application)

install_headers()
install_source()

add_subdirectory(MPI2)
