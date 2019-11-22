

set(TFILE_SERVICE_LIB art_Framework_Services_Optional_TFileService_service art_Framework_Services_Optional)
if ( ${ART_HEX_VERSION} LESS 0x21200)
	# s67 and s73
add_subdirectory(RootDAQOutput)
	set(ROOT_IO_LIBS art_Framework_IO_Root)
elseif ( ${ART_HEX_VERSION} GREATER 0x30200)
	# s82, s87
	add_subdirectory(RootDAQOutput-s81)
	set(ROOT_IO_LIBS art_root_io)
	set(ROOT_IO_TFILE art_root_io_tfile_support)
	set(TFILE_SERVICE_LIB art_root_io_TFileService_service)
else()
	# s85 is art v2_12_00
	add_subdirectory(RootDAQOutput-s85)
	set(ROOT_IO_LIBS art_Framework_IO_Root)
endif()

art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_ArtModules
  SUBDIRS detail
  NO_PLUGINS
  ${ART_PERSISTENCY_PROVENANCE}
  ${CANVAS_PERSISTENCY_PROVENANCE}
  LIB_LIBRARIES
  artdaq-core_Data
  artdaq-core_Data_dict
  artdaq_DAQrate
  artdaq_DAQdata
  artdaq_Application
  artdaq_ExternalComms
  art_Framework_Core
  art_Framework_IO_Sources
  ${ROOT_IO_LIBS}
  ${ROOT_IO_TFILE}
  art_Framework_IO
  art_Framework_Principal
  artdaq_ArtModules_ArtdaqSharedMemoryService_service
  art_Persistency_Provenance
  #  art_Persistency_RootDB
  canvas
  MF_MessageLogger
  ${SQLITE3}
  ${ROOT_CINTEX}
  ${ROOT_RIO}
  ${ROOT_TREE}
  ${ROOT_CORE}
  ${ROOT_NET}
  ${ROOT_REFLEX}
  ${Boost_DATE_TIME_LIBRARY}
  )

simple_plugin(RawInput "source"
  artdaq_ArtModules
  artdaq-core_Data
  artdaq-core_Data_dict
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  MF_MessageLogger
  rt
)

simple_plugin(TransferOutput "module"
  artdaq_TransferPlugins
  artdaq_ArtModules
  ${ROOT_IO_LIBS}
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${ROOT_RIO}
  ${TBB}
  ${HEP_CONCURRENCY}
)

simple_plugin(TransferInput "source"
  artdaq-core_Data
  artdaq-core_Data_dict
  artdaq_TransferPlugins
  artdaq_ArtModules
  ${TBB}
  ${HEP_CONCURRENCY}
)

simple_plugin(RandomDelayFilter "module" artdaq_ArtModules)

simple_plugin(EventDump "module"
  artdaq-core_Data
  artdaq-core_Utilities
  art_Persistency_Provenance
MF_MessageLogger
  artdaq-core_Data
  artdaq-core_Data_dict
  ${CETLIB_EXCEPT})

simple_plugin(MissingDataCheck "module"
  artdaq-core_Data
  artdaq-core_Data_dict
  artdaq-core_Utilities
  MF_MessageLogger
  art_Persistency_Provenance
  art_Framework_Services_Registry
  ${ROOT_IO_LIBS}
  ${ROOT_IO_TFILE}
  ${TFILE_SERVICE_LIB}
  ${ROOT_BASIC_LIB_LIST}
  ${ROOT_CORE}
  ${CETLIB_EXCEPT}
)

simple_plugin(FragmentWatcher "module"
  artdaq_DAQdata
  artdaq-utilities_Plugins
  artdaq-core_Utilities
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
)


simple_plugin(NetMonInput "source"
  artdaq-core_Data
  artdaq-core_Data_dict
  artdaq_ArtModules
  artdaq-core_Utilities
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  ${MESSAGEFACILITY_LIBS}
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${TBB}
  ${HEP_CONCURRENCY}
)

simple_plugin(RootNetOutput "module"
  artdaq_ArtModules
  artdaq-core_Data_dict
  ${ROOT_IO_LIBS}
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${ROOT_RIO}
  ${TBB}
  ${HEP_CONCURRENCY}
)

simple_plugin(BinaryNetOutput "module"
  artdaq_ArtModules
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${ROOT_RIO}
)

simple_plugin(BinaryFileOutput "module"
  artdaq_ArtModules
  art_Framework_IO
  art_Framework_Services_Registry
  art_Utilities
  MF_MessageLogger
  fhiclcpp
  cetlib
  ${CETLIB_EXCEPT}
  ${ROOT_RIO}
)

simple_plugin(ArtdaqSharedMemoryService "service"
  artdaq-core_Data
  artdaq-core_Data_dict
  artdaq_DAQrate
  art_Framework_Services_Registry
  MF_MessageLogger
  fhiclcpp
)

simple_plugin(ArtdaqBuildInfo "module"
  artdaq_DAQdata
  artdaq_BuildInfo
  artdaq-utilities_BuildInfo
  artdaq-core_BuildInfo
  art_Persistency_Provenance
  ${MF_MESSAGELOGGER}
  ${FHICLCPP}
  ${CETLIB}
  ${CETLIB_EXCEPT}
)

simple_plugin(PrintBuildInfo "module"
  MF_MessageLogger
  fhiclcpp
  art_Persistency_Provenance
  ${CETLIB}
  ${CETLIB_EXCEPT}
  )


install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
