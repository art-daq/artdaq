
if (${USING_RTIDDS})
include_directories($ENV{NDDSHOME}/include $ENV{NDDSHOME}/include/ndds )
endif()

cet_make( 
  SUBDIRS detail
  EXCLUDE RTIDDS_transfer.cc Shmem_transfer.cc Multicast_transfer.cc TCPSocket_transfer.cc Autodetect_transfer.cc
  LIBRARIES
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Data
  artdaq::artdaq_DAQdata
  )

  
cet_make_library(LIBRARY_NAME transfer INTERFACE
				 EXPORT_SET PluginTypes
				 SOURCE TransferInterface.hh
				 LIBRARIES INTERFACE
				 art_plugin_support::plugin_config_macros
				 art_plugin_support::support_macros
				 artdaq::artdaq_TransferPlugins)


include(BasicPlugin)
  cet_make_plugin_builder(transfer transfer Modules
    LIBRARIES REG artdaq_plugin_types::transfer)

include(transfer)

cet_build_plugin(Shmem transfer)
cet_build_plugin(Null transfer)
cet_build_plugin(Multicast transfer)
cet_build_plugin(TCPSocket transfer)
cet_build_plugin(Autodetect transfer)

if (${USING_RTIDDS})
  cet_build_plugin(RTIDDS transfer LIBRARIES REG artdaq::artdaq_RTIDDS)
endif()

install_headers(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
install_source(SUBDIRS detail)
