
cet_make(
  EXCLUDE CapacityTest_policy.cc NoOp_policy.cc PreferSameHost_policy.cc RoundRobin_policy.cc
  LIBRARIES
  artdaq_DAQdata
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Data
  )
  
cet_make_library(LIBRARY_NAME policy INTERFACE
				 EXPORT_SET PluginTypes
				 SOURCE RoutingManagerPolicy.hh
				 LIBRARIES INTERFACE
				 art_plugin_support::plugin_config_macros
				 art_plugin_support::support_macros
				 artdaq::artdaq_RoutingPolicies)


include(BasicPlugin)
  cet_make_plugin_builder(policy policy Modules
    LIBRARIES REG artdaq_plugin_types::policy)
	
cet_collect_plugin_builders(Modules PolicyPlugins)
include(PolicyPlugins)

cet_build_plugin(RoundRobin policy)
cet_build_plugin(NoOp policy)
cet_build_plugin(CapacityTest policy)
cet_build_plugin(PreferSameHost policy)

install_headers()
install_source()
install_fhicl(SUBDIRS fcl)
