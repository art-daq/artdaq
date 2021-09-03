


	add_subdirectory(RootDAQOutput-s81)

cet_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_ArtModules
  SUBDIRS detail
  NO_PLUGINS
  LIB_LIBRARIES
  artdaq_DAQdata
  artdaq_ExternalComms
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Services_Registry
  )


simple_plugin(TransferOutput "module"
  artdaq_TransferPlugins
	hep_concurrency
	art_root_io
	${ROOT_RIO}
)

simple_plugin(TransferInput "source"
	artdaq_ArtModules
  artdaq_TransferPlugins
  art_root_io
  hep_concurrency
  ${ROOT_RIO}
)

simple_plugin(RandomDelayFilter "module" 
artdaq_DAQdata
)

simple_plugin(RequestSender "module" artdaq_ArtModules artdaq_DAQrate)

simple_plugin(EventDump "module"
artdaq_core::artdaq-core_Data
)

simple_plugin(MissingDataCheck "module"
art_root_io
art_root_io_tfile_support
art_root_io_TFileService_service
  ${ROOT_TREE}
)

simple_plugin(FragmentWatcher "module"
artdaq_DAQdata
)

simple_plugin(ArtdaqInput "source"
	artdaq_ArtModules
  art_root_io
  hep_concurrency
  ${ROOT_RIO}
)


simple_plugin(RootNetOutput "module"
artdaq_DAQdata
artdaq_DAQrate
	hep_concurrency
	art_root_io
  ${ROOT_RIO}
)

simple_plugin(BinaryNetOutput "module"
artdaq_DAQdata
artdaq_DAQrate
)

simple_plugin(BinaryFileOutput "module"
artdaq_DAQdata
art_Framework_IO
)

simple_plugin(ArtdaqSharedMemoryService "service"
artdaq_DAQdata
artdaq_DAQrate
)

simple_plugin(ArtdaqFragmentNamingService "service"
  MF_MessageLogger
)

simple_plugin(ArtdaqBuildInfo "module"
artdaq_BuildInfo
artdaq_core::artdaq-core_BuildInfo
artdaq_utilities::artdaq-utilities_BuildInfo
)

simple_plugin(PrintBuildInfo "module"
	MF_MessageLogger
  )


install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
