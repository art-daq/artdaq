#get_cmake_property(_variableNames VARIABLES)
#foreach (_variableName ${_variableNames})
#    message(STATUS "${_variableName}=${${_variableName}}")
#endforeach()

art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_ExternalComms
  LIB_LIBRARIES artdaq_Application
  EXCLUDE xmlrpc_commander.cc
  NO_PLUGINS
  artdaq_DAQdata
  fhiclcpp
  cetlib
)

simple_plugin(xmlrpc "commander"
  artdaq_ExternalComms
  fhiclcpp
  xmlrpc
  xmlrpc++
  xmlrpc_client++
  xmlrpc_server
  xmlrpc_server++
  xmlrpc_server_abyss++
  xmlrpc_util
  xmlrpc_util++
)

install_headers()
install_source()
