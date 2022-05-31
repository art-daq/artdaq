cet_make( 
  EXCLUDE xmlrpc_commander.cc
	LIBRARIES artdaq_Application
  artdaq_DAQdata
)

cet_make_library(LIBRARY_NAME commander INTERFACE
				 EXPORT_SET PluginTypes
				 SOURCE CommanderInterface.hh
				 LIBRARIES INTERFACE
				 art_plugin_support::plugin_config_macros
				 art_plugin_support::support_macros
				 artdaq::artdaq_ExternalComms)


include(BasicPlugin)
  cet_make_plugin_builder(commander commander Modules
    LIBRARIES REG artdaq_plugin_types::commander)

include(commander)
cet_build_plugin(xmlrpc commander)

install_headers()
install_source()
