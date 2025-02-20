cet_register_export_set(SET_NAME ECPluginTypes NAMESPACE artdaq_plugin_types)
cet_register_export_set(SET_NAME ECPluginSupport NAMESPACE artdaq_plugin_support)

cet_make_library(LIBRARY_NAME commander
  EXPORT_SET ECPluginTypes
  SOURCE CommanderInterface.cc
	LIBRARIES
  PUBLIC
  artdaq::Application
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  cetlib::compiler_macros
)

cet_make_library(LIBRARY_NAME commanderMaker
  EXPORT_SET ECPluginSupport
  SOURCE MakeCommanderPlugin.cc
  LIBRARIES
  PUBLIC
  artdaq_plugin_types::commander
  PRIVATE
  fhiclcpp::fhiclcpp
  cetlib::cetlib
)

include(BasicPlugin)
cet_write_plugin_builder(artdaq::commander art::plugin Modules
  SUFFIX commander INSTALL_BUILDER
  LIBRARIES CONDITIONAL artdaq_plugin_types::commander)

include(artdaq::commander)
cet_build_plugin(xmlrpc artdaq::commander
  LIBRARIES PRIVATE
  artdaq_core::artdaq-core_Utilities
  ${XMLRPC_LIBRARIES}
)
target_include_directories(artdaq_ExternalComms_xmlrpc_commander
  PRIVATE ${XMLRPC_INCLUDE_DIRS})

install_headers()
install_source()
