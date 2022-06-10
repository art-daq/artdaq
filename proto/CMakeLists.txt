# ----------------------------------------------------------------------
# Build this project's executables:

# Nominally a test exec (no MPI) but we want this to be available to other packages.
cet_make_exec(NAME artdaqDriver
	SOURCE driver.cc
  LIBRARIES PRIVATE
  artdaq_plugin_support::CFGMaker
  artdaq_plugin_support::generatorMaker
  artdaq::ArtConfig
  artdaq::Application
  artdaq::DAQrate
  artdaq::DAQdata_GenericFragmentSimulator_generator
  artdaq::DAQdata
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
)

cet_make_exec(NAME fhicl_test
	LIBRARIES PRIVATE
  artdaq::Application
  fhiclcpp::fhiclcpp
)

cet_make_exec(NAME simple_metric_sender
	LIBRARIES PRIVATE
  artdaq::Application # needed for messagefacility
)

cet_make_exec(NAME artdaq
  LIBRARIES PRIVATE
  artdaq_plugin_support::commanderMaker
  artdaq::Application # needed for messagefacility
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq::BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
)

cet_make_exec(NAME boardreader
  LIBRARIES PRIVATE
  artdaq_plugin_support::commanderMaker
  artdaq::Application # needed for messagefacility
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq::BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
)

cet_make_exec(NAME eventbuilder
  LIBRARIES PRIVATE
  artdaq_plugin_support::commanderMaker
  artdaq::Application # needed for messagefacility
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq::BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
)

cet_make_exec(NAME datalogger
  LIBRARIES PRIVATE
  artdaq_plugin_support::commanderMaker
  artdaq::Application # needed for messagefacility
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq::BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
)

cet_make_exec(NAME dispatcher
  LIBRARIES PRIVATE
  artdaq_plugin_support::commanderMaker
  artdaq::Application # needed for messagefacility
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq::BuildInfo
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
)

cet_make_exec(NAME requestReceiver
	LIBRARIES PRIVATE
	artdaq::Application
)

cet_make_exec(NAME requestSender
	LIBRARIES PRIVATE
	artdaq::Application
  fhiclcpp::types
  fhiclcpp::fhiclcpp
)

cet_make_exec(NAME routing_manager
  LIBRARIES PRIVATE
  artdaq_plugin_support::commanderMaker
  artdaq::Application
  artdaq::BuildInfo
)

cet_make_exec(NAME routingReceiver
  LIBRARIES PRIVATE
  artdaq::Application
  artdaq::BuildInfo
)

cet_make_exec(NAME transfer_plugin_sender # NO_INSTALL -- comment out to install
  LIBRARIES PRIVATE
  artdaq::TransferPlugins
  artdaq_core::artdaq-core_Data
)

cet_make_exec(NAME transfer_plugin_receiver # NO_INSTALL -- comment out to install
  LIBRARIES PRIVATE
  artdaq::TransferPlugins
  artdaq_core::artdaq-core_Data
)

cet_make_exec(NAME tracemf
	LIBRARIES PRIVATE
	artdaq::DAQdata
	artdaq_core::artdaq-core_Utilities
  Boost::program_options
  TRACE::MF
)

cet_make_exec(NAME PrintSharedMemory
	LIBRARIES PRIVATE
	artdaq::DAQrate
	artdaq_core::artdaq-core_Core
	artdaq_core::artdaq-core_Utilities
  Boost::program_options
)

install_fhicl(SUBDIRS fcl)
install_source()
