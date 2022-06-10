


	add_subdirectory(RootDAQOutput-s81)

cet_make(
  SUBDIRS detail
  NO_PLUGINS
  LIBRARIES
  TRACE::TRACE
  )


cet_build_plugin(TransferOutput art::module LIBRARIES REG
  artdaq_TransferPlugins
)

cet_build_plugin(TransferInput art::source LIBRARIES REG
	artdaq_ArtModules
  artdaq_TransferPlugins
)

cet_build_plugin(RandomDelayFilter art::module  LIBRARIES REG
artdaq_DAQdata
)

cet_build_plugin(RequestSender art::module LIBRARIES REG artdaq_ArtModules artdaq_DAQrate)

cet_build_plugin(EventDump art::module LIBRARIES REG
artdaq_core::artdaq-core_Data
)

cet_build_plugin(MissingDataCheck art::module)

cet_build_plugin(FragmentWatcher art::module LIBRARIES REG
artdaq_DAQdata
)

cet_build_plugin(ArtdaqInput art::source LIBRARIES REG
	artdaq_ArtModules
)


cet_build_plugin(RootNetOutput art::module LIBRARIES REG
artdaq_DAQdata
artdaq_DAQrate
)

cet_build_plugin(BinaryNetOutput art::module LIBRARIES REG
artdaq_DAQdata
artdaq_DAQrate
)

cet_build_plugin(BinaryFileOutput art::module LIBRARIES REG
artdaq_DAQdata
)

cet_build_plugin(ArtdaqSharedMemoryService art::service LIBRARIES REG
artdaq_DAQdata
artdaq_DAQrate
)

cet_build_plugin(ArtdaqFragmentNamingService art::service)

cet_build_plugin(ArtdaqBuildInfo art::module LIBRARIES REG
artdaq_BuildInfo
artdaq_core::artdaq-core_BuildInfo
artdaq_utilities::artdaq-utilities_BuildInfo
)

cet_build_plugin(PrintBuildInfo art::module  )


install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
