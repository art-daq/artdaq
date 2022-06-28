cet_register_export_set(SET_NAME EPPluginTypes NAMESPACE artdaq_plugin_types)
cet_register_export_set(SET_NAME EPPluginSupport NAMESPACE artdaq_plugin_support)

cet_make_library(LIBRARY_NAME policy
  EXPORT_SET EPPluginTypes
  SOURCE RoutingManagerPolicy.cc
  LIBRARIES
  PUBLIC
  artdaq_core::artdaq-core_Data
  PRIVATE
  artdaq::DAQdata
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_make_library(LIBRARY_NAME policyMaker
  EXPORT_SET EPPluginSupport
  SOURCE makeRoutingManagerPolicy.cc
  LIBRARIES
  PRIVATE
  artdaq_plugin_types::policy
  cetlib::cetlib
)

cet_make_library(LIBRARY_NAME PolicyMacros INTERFACE
  EXPORT_SET EPPluginSupport
  SOURCE PolicyMacros.hh
  LIBRARIES INTERFACE
  artdaq_plugin_types::policy
  cetlib::headers
)

include(BasicPlugin)
cet_write_plugin_builder(artdaq::policy art::plugin Modules
  SUFFIX policy
  INSTALL_BUILDER
  LIBRARIES
  CONDITIONAL artdaq_plugin_types::policy
  REG artdaq_plugin_support::PolicyMacros)
	
cet_collect_plugin_builders(Modules PolicyPlugins LIST artdaq::policy)
include(PolicyPlugins)

cet_build_plugin(RoundRobin artdaq::policy
  LIBRARIES PRIVATE
  artdaq::DAQdata
)

cet_build_plugin(NoOp artdaq::policy
  LIBRARIES PRIVATE
  TRACE::MF
)

cet_build_plugin(CapacityTest artdaq::policy)

cet_build_plugin(PreferSameHost artdaq::policy
  LIBRARIES PRIVATE
  artdaq::DAQdata
)

install_headers()
install_source()
install_fhicl(SUBDIRS fcl)
