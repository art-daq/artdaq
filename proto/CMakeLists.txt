
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

cet_test(daqrate.py PREBUILT NO_AUTO INSTALL_BIN)
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

art_make_exec(NAME fhicl_test
    SOURCE
    fhicl_test.cc
    LIBRARIES
    ${FHICLCPP}
    ${CETLIB}
    ${CETLIB_EXCEPT}
    ${MF_MESSAGELOGGER}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    )
	
  art_make_exec(NAME boardreader
    SOURCE
    boardreader.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq-core_Utilities
  artdaq-core_Generators
  art_Framework_Art
  artdaq_ExternalComms
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${XMLRPC_CLIENTPP}
  ${XMLRPCPP}
  ${XMLRPC_UTILPP}
  )

  art_make_exec(NAME eventbuilder
    SOURCE
    eventbuilder.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq-core_Utilities
  artdaq-core_Generators
  art_Framework_Art
  artdaq_ExternalComms
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${XMLRPC_CLIENTPP}
  ${XMLRPCPP}
  ${XMLRPC_UTILPP}
  )

  art_make_exec(NAME datalogger
    SOURCE
    datalogger.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq-core_Utilities
  artdaq-core_Generators
  art_Framework_Art
  artdaq_ExternalComms
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${XMLRPC_CLIENTPP}
  ${XMLRPCPP}
  ${XMLRPC_UTILPP}
  )
  
  art_make_exec(NAME dispatcher
    SOURCE
    dispatcher.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq-core_Utilities
  artdaq-core_Generators
  art_Framework_Art
  artdaq_ExternalComms
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${XMLRPC_CLIENTPP}
  ${XMLRPCPP}
  ${XMLRPC_UTILPP}
  )

art_make_exec(NAME transfer_plugin_sender # NO_INSTALL -- comment out to install
  LIBRARIES
  artdaq_TransferPlugins
  artdaq-core_Data
  ${CETLIB}
  ${FHICLCPP}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  pthread
  )

art_make_exec(NAME transfer_plugin_receiver # NO_INSTALL -- comment out to install
  LIBRARIES
  artdaq_TransferPlugins
  artdaq-core_Data
  ${CETLIB}
  ${FHICLCPP}
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  pthread
  )


 art_make_exec(NAME transfer_driver_mpi # NO_INSTALL -- comment out to install
SOURCE
transfer_driver_mpi.cc
TransferTest.cc
FragmentReceiverManager.cc
 LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
   artdaq_DAQdata
artdaq_TransferPlugins
  ${FHICLCPP}
   )

 art_make_exec(NAME transfer_driver # NO_INSTALL -- comment out to install
SOURCE
transfer_driver.cc
FragmentReceiverManager.cc
TransferTest.cc
   LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
   artdaq_DAQdata
artdaq_TransferPlugins
  ${FHICLCPP}
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
	artdaq-core_Utilities
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    )

	art_make_exec(NAME PrintSharedMemory
	SOURCE PrintSharedMemory.cc
	LIBRARIES
	artdaq_DAQrate
	artdaq-core_Core
	${Boost_PROGRAM_OPTIONS_LIBRARY}
	)
	
install_fhicl(LIST daqrate_simdata_noart.fcl Multicast.fcl Shmem.fcl)
install_source()
