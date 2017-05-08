
# Make sure tests have correct environment settings.
include(CetTest)
# If you explicitly include CetTest in a subdirectory, you will need to
# re-initialize the test environment.
cet_test_env("FHICL_FILE_PATH=.")

include_directories(${MPI_INCLUDE_PATH})

# Make sure tests have correct environment settings.
include(CetTest)
# If you explicitly include CetTest in a subdirectory, you will need to
# re-initialize the test environment.
cet_test_env("FHICL_FILE_PATH=.")

cet_test(daqrate PREBUILT NO_AUTO INSTALL_BIN)
cet_test(builder.sh PREBUILT NO_AUTO INSTALL_BIN)

cet_script(numawrap)

# ----------------------------------------------------------------------
# Build this project's executables:

# Nominally a test exec (no MPI) but we want this to be available to other packages.
art_make_exec(NAME artdaqDriver
    SOURCE
    driver.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq-core_Utilities
  artdaq-core_Generators
  art_Framework_Art
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  )

art_make_exec(NAME transfer_plugin_sender # NO_INSTALL -- comment out to install
  LIBRARIES
  artdaq_TransferPlugins
  artdaq-core_Data
  ${CETLIB}
  ${FHICLCPP}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  )

art_make_exec(NAME transfer_plugin_receiver # NO_INSTALL -- comment out to install
  LIBRARIES
  artdaq_TransferPlugins
  artdaq-core_Data
  ${CETLIB}
  ${FHICLCPP}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  )

art_make_exec(perfdump
  LIBRARIES
  artdaq_DAQrate
  )

  art_make_exec(NAME routing_master
  SOURCE
  routing_master.cc
  LIBRARIES
  artdaq_Application
  artdaq_Application_MPI2
  artdaq_Application_Routing
  ${FHICLCPP}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  )
  
 art_make_exec(NAME builder
   SOURCE
   builder.cc
   Config.cc
   LIBRARIES
   art_Framework_Art
  artdaq_Application # needed for messagefacility
   artdaq_DAQrate
   artdaq_DAQdata
   artdaq-core_Utilities
   artdaq-core_Generators
   ${FHICLCPP}
   ${CETLIB}
   ${Boost_PROGRAM_OPTIONS_LIBRARY}
   ${MPI_C_LIBRARIES}
   )

art_make_exec(NAME tracemf
    SOURCE
    tracemf.cc
    LIBRARIES
    artdaq_DAQdata
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    )

cet_test(config_t
  SOURCES
  config_t.cc
  Config.cc
  LIBRARIES artdaq_DAQrate
   ${Boost_PROGRAM_OPTIONS_LIBRARY} )

cet_test(config_with_art_t
  SOURCES
  config_with_art_t.cc
  Config.cc
  LIBRARIES artdaq_DAQrate
   ${Boost_PROGRAM_OPTIONS_LIBRARY}
  TEST_PROPERTIES ENVIRONMENT ARTDAQ_DAQRATE_USE_ART=1
  )

install_fhicl(LIST daqrate_simdata_noart.fcl Multicast.fcl Shmem.fcl)
install_source()
