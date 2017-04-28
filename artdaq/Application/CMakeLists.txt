process_smc(SMC_LIB_SOURCE Commandable.sm)

include_directories("${CMAKE_BINARY_DIR}")

art_make_library( LIBRARY_NAME artdaq_Application
  SOURCE
  ${SMC_LIB_SOURCE}
  Commandable.cc
  CommandableFragmentGenerator
  makeCommandableFragmentGenerator
  LIBRARIES
  artdaq_DAQrate
  artdaq-core_Data
  artdaq-core_Utilities
  art_Utilities
  ${CANVAS_LIB_LIST}
  ${MF_MESSAGELOGGER}
  ${CETLIB}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${FHICLCPP}
  )

simple_plugin(CompositeDriver "generator" artdaq_DAQdata artdaq_Application)

install_headers()
install_source()
install_fhicl()

add_subdirectory(MPI2)
add_subdirectory(Routing)
