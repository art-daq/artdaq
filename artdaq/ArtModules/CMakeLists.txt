include_directories(${MPI_INCLUDE_PATH})

art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_ArtModules
  SUBDIRS detail
  NO_PLUGINS
  DICT_DICTIONARIES
  ${ART_PERSISTENCY_PROVENANCE}
  LIB_LIBRARIES
  artdaq_DAQrate
  artdaq_DAQdata
  ${ART_FRAMEWORK_CORE}
  ${ART_FRAMEWORK_IO_SOURCES}
  ${ART_FRAMEWORK_PRINCIPAL}
  ${ART_PERSISTENCY_PROVENANCE}
  ${ART_UTILITIES}
  ${MF_MESSAGELOGGER}
  ${ROOT_CINTEX}
  ${ROOT_RIO}
  ${ROOT_CORE}
  ${ROOT_NET}
  ${ROOT_REFLEX}
  )

simple_plugin(RawInput "source"
  artdaq_ArtModules
  ${ART_FRAMEWORK_CORE}
  ${ART_FRAMEWORK_IO_SOURCES}
  ${ART_FRAMEWORK_PRINCIPAL}
  ${ART_PERSISTENCY_PROVENANCE}
  ${ART_UTILITIES}
  ${MF_MESSAGELOGGER}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
)

simple_plugin(NetMonInput "source"
  artdaq_ArtModules_NetMonTransportService_service
  artdaq_ArtModules
  ${ART_FRAMEWORK_CORE}
  ${ART_FRAMEWORK_IO_SOURCES}	
  ${ART_FRAMEWORK_SERVICES_REGISTRY}
  ${ART_UTILITIES}
  ${MF_MESSAGELOGGER}
  ${FHICLCPP}
  ${CETLIB}
)

simple_plugin(NetMonOutput "module"
  artdaq_ArtModules_NetMonTransportService_service
  artdaq_ArtModules
  ${ART_FRAMEWORK_SERVICES_REGISTRY}
  ${ART_UTILITIES}
  ${MF_MESSAGELOGGER}
  ${FHICLCPP}
  ${CETLIB}
  ${ROOT_RIO}
)

simple_plugin(NetMonTransportService "service"
  artdaq_DAQrate
  ${ART_FRAMEWORK_SERVICES_REGISTRY}
  ${MF_MESSAGELOGGER}
  ${FHICLCPP}
  ${ROOT_RIO}
  ${ROOT_NET}
  ${ROOT_CORE}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
)

_check_if_version_greater(art ${ART_VERSION} v1_08_00)
if (NOT ${product_version_less})
  simple_plugin(InFlightConfiguration "service"
    ${ART_FRAMEWORK_SERVICES_REGISTRY}
    ${ART_FRAMEWORK_SERVICES_USERINTERACTION}
    ${ART_FRAMEWORK_SERVICES_SYSTEM_PATHSELECTION_SERVICE}
    ${ART_UTILITIES}
    )
endif()

simple_plugin(ArtdaqBuildInfo "module"
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq-core_BuildInfo
  ${MF_MESSAGELOGGER}
  ${FHICLCPP}
  ${CETLIB}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
)


install_headers(SUBDIRS detail)                                                         
install_source(SUBDIRS detail)
