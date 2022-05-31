# ----------------------------------------------------------------------
# Build this project's executables:

# Nominally a test exec (no MPI) but we want this to be available to other packages.
cet_make_exec(NAME artdaqDriver
	SOURCE
	driver.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_Generators
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  )

cet_make_exec(NAME fhicl_test
	SOURCE
	fhicl_test.cc
	LIBRARIES
	)

cet_make_exec(NAME simple_metric_sender
	SOURCE
	simple_metric_sender.cc
	LIBRARIES
  artdaq_Application # needed for messagefacility
  )


cet_make_exec(NAME artdaq
	SOURCE
	artdaq.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  artdaq_ExternalComms
  )

  cet_make_exec(NAME boardreader
	SOURCE
	boardreader.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  artdaq_ExternalComms
  )

  cet_make_exec(NAME eventbuilder
	SOURCE
	eventbuilder.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  artdaq_ExternalComms
  )

  cet_make_exec(NAME datalogger
	SOURCE
	datalogger.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  artdaq_ExternalComms
  )

  cet_make_exec(NAME dispatcher
	SOURCE
	dispatcher.cc
  LIBRARIES
  artdaq_Application # needed for messagefacility
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  artdaq_ExternalComms
  )

  cet_make_exec(NAME requestReceiver
	  SOURCE requestReceiver.cc
	  LIBRARIES
	  artdaq_Application
  )

  cet_make_exec(NAME requestSender
	  SOURCE requestSender.cc
	  LIBRARIES
	  artdaq_Application
  )

  cet_make_exec(NAME routing_manager
  SOURCE
  routing_manager.cc
  LIBRARIES
  artdaq_Application
  artdaq_BuildInfo
  artdaq_ExternalComms
  )

  cet_make_exec(NAME routingReceiver
  SOURCE
  routingReceiver.cc
  LIBRARIES
  artdaq_Application
  artdaq_BuildInfo
  artdaq_ExternalComms
  )

cet_make_exec(NAME transfer_plugin_sender # NO_INSTALL -- comment out to install
  LIBRARIES
  artdaq_TransferPlugins
  artdaq_core::artdaq-core_Data
  )

cet_make_exec(NAME transfer_plugin_receiver # NO_INSTALL -- comment out to install
  LIBRARIES
  artdaq_TransferPlugins
  artdaq_core::artdaq-core_Data
  )

cet_make_exec(NAME tracemf
	SOURCE
	tracemf.cc
	LIBRARIES
	artdaq_DAQdata
	artdaq_core::artdaq-core_Utilities
	)

cet_make_exec(NAME PrintSharedMemory
	SOURCE PrintSharedMemory.cc
	LIBRARIES
	artdaq_DAQrate
	artdaq_core::artdaq-core_Core
	)

install_fhicl(SUBDIRS fcl)
install_source()
