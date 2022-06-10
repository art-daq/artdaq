cet_register_export_set(SET_NAME Framework SET_DEFAULT)

add_subdirectory(DAQdata)
add_subdirectory(BuildInfo)

if (${USING_RTIDDS})
add_subdirectory(RTIDDS)
endif()

cet_register_export_set(SET_NAME FrameworkUsers SET_DEFAULT)

add_subdirectory(ExternalComms)
add_subdirectory(TransferPlugins)
add_subdirectory(RoutingPolicies)

cet_register_export_set(SET_NAME PluginUsers SET_DEFAULT)

add_subdirectory(DAQrate)
add_subdirectory(Application)
add_subdirectory(ArtModules)
add_subdirectory(Generators)