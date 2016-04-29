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
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  MF_MessageLogger
  ${ROOT_CINTEX}
  ${ROOT_RIO}
  ${ROOT_CORE}
  ${ROOT_NET}
  ${ROOT_REFLEX}
  )

simple_plugin(RawInput "source"
  artdaq_ArtModules
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  MF_MessageLogger
)

simple_plugin(RandomDelayFilter "module" artdaq_ArtModules)

simple_plugin(NetMonInput "source"
  artdaq_ArtModules_NetMonTransportService_service
  artdaq_ArtModules
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
)

simple_plugin(RootMPIOutput "module"
  artdaq_ArtModules_NetMonTransportService_service
  artdaq_ArtModules
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${ROOT_RIO}
)

simple_plugin(BinaryMPIOutput "module"
  artdaq_ArtModules
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${ROOT_RIO}
)

simple_plugin(BinaryFileOutput "module"
  artdaq_ArtModules
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${ROOT_RIO}
)

simple_plugin(NetMonTransportService "service"
  artdaq_DAQrate
  art_Framework_Services_Registry
  MF_MessageLogger
  fhiclcpp
  ${ROOT_RIO}
  ${ROOT_NET}
  ${ROOT_CORE}
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
)


install_headers(SUBDIRS detail)                                                         
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
