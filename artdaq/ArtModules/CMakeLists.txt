
if(${ART_HEX_VERSION} LESS 0x31200)
	add_subdirectory(RootDAQOutput-s81)
else()
	add_subdirectory(RootDAQOutput-s124)
endif()

cet_register_export_set(SET_NAME AMPluginTypes NAMESPACE artdaq_plugin_types)

cet_make_library(LIBRARY_NAME ArtConfig INTERFACE
  SOURCE detail/ArtConfig.hh
  LIBRARIES INTERFACE
  artdaq-utilities::Plugins
  art::Framework_Core
  art::Framework_IO
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  )

cet_make_library(LIBRARY_NAME ArtdaqSharedMemoryService INTERFACE
  EXPORT_SET AMPluginTypes
  SOURCE ArtdaqSharedMemoryServiceInterface.h
  LIBRARIES INTERFACE
  artdaq-core::Data
  art_plugin_types::serviceDeclaration
  TRACE::MF
)

cet_register_export_set(SET_NAME AMPUsers SET_DEFAULT)

cet_build_plugin(ArtdaqSharedMemoryService art::service
  LIBRARIES PRIVATE
  artdaq_plugin_types::ArtdaqSharedMemoryService
  artdaq::DAQdata
  artdaq-core::Core
  artdaq-core::Utilities
  fhiclcpp::types
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_build_plugin(ArtdaqGlobalsService art::service
  LIBRARIES PRIVATE
  artdaq_plugin_types::ArtdaqSharedMemoryService
  artdaq::DAQdata
  artdaq::DAQrate
)

cet_make_library(SOURCE
  detail/ShmemWrapper.cc
  detail/TransferWrapper.cc
  detail/ListenTransferWrapper.cc
  LIBRARIES
  PUBLIC
  artdaq_plugin_types::commander
  artdaq_plugin_types::transfer
  artdaq-core::Data
  fhiclcpp::fhiclcpp
  TRACE::MF
  PRIVATE
  artdaq_plugin_support::transferMaker
  artdaq_plugin_support::commanderMaker
  artdaq_plugin_types::ArtdaqSharedMemoryService
  artdaq::DAQdata
  art::Framework_Services_Registry
)

cet_make_library(LIBRARY_NAME ArtdaqFragmentNamingService INTERFACE
  EXPORT_SET AMPluginTypes
  SOURCE ArtdaqFragmentNamingService.h
  LIBRARIES INTERFACE
  artdaq-core::Plugins
  artdaq-core::Data
  art_plugin_types::serviceDeclaration
)

cet_build_plugin(ArtdaqFragmentNamingService art::service
  LIBRARIES PRIVATE
  artdaq_plugin_types::ArtdaqFragmentNamingService
)

cet_make_library(LIBRARY_NAME ArtdaqBuildInfo INTERFACE
  EXPORT_SET AMPluginTypes
  SOURCE ArtdaqBuildInfo_module.hh
  LIBRARIES INTERFACE
  artdaq-core::headers
  art_plugin_types::EDProducer
)

cet_build_plugin(ArtdaqBuildInfo art::EDProducer
  LIBRARIES PRIVATE
  artdaq-core::BuildInfo
  artdaq-utilities::BuildInfo
  artdaq::BuildInfo
)

cet_build_plugin(EventDump art::EDAnalyzer
  LIBRARIES PRIVATE
artdaq-core::Data
)

cet_build_plugin(FragmentWatcher art::EDAnalyzer
  LIBRARIES PRIVATE
  artdaq::DAQdata
  artdaq-core::Data
  TRACE::MF
)

cet_build_plugin(MissingDataCheck art::EDAnalyzer
  LIBRARIES PRIVATE
  artdaq-core::Data
  art_root_io::TFileService_service
  ROOT::Tree
)

cet_build_plugin(PrintBuildInfo art::EDAnalyzer
  LIBRARIES PRIVATE
  artdaq-core::headers
  TRACE::MF
)

cet_build_plugin(RandomDelayFilter art::EDFilter
  LIBRARIES PRIVATE
  artdaq-core::Utilities
  TRACE::MF
)

cet_build_plugin(RequestSender art::EDAnalyzer
  LIBRARIES PRIVATE
  artdaq::DAQrate
  artdaq-core::Data
  TRACE::MF
)

include(art::SourceT OPTIONAL)
if (COMMAND art::SourceT AND TARGET art_plugin_types::SourceT)
  set(stype art::SourceT)
else()
  set(stype art::source)
endif()

cet_make_library(LIBRARY_NAME InputUtilities INTERFACE
  SOURCE InputUtilities.hh
  LIBRARIES INTERFACE
  canvas::canvas
  ROOT::Core
  TRACE::MF
)

cet_make_library(LIBRARY_NAME ArtdaqInputHelper INTERFACE
  SOURCE ArtdaqInputHelper.hh
  LIBRARIES INTERFACE
  artdaq_plugin_types::ArtdaqFragmentNamingService
  artdaq_plugin_types::ArtdaqSharedMemoryService
  artdaq::DAQdata
  artdaq::InputUtilities
  artdaq-core::Data
  artdaq-core::Utilities
  art_root_io::art_root_io
  art::Framework_Core
  art::Framework_IO_Sources
  art::Framework_Principal
  art::Framework_Services_Registry
  canvas::canvas
  fhiclcpp::fhiclcpp
  ROOT::Core
)

cet_build_plugin(TransferInput ${stype}
  LIBRARIES PRIVATE
  artdaq::ArtModules
  artdaq::ArtdaqInputHelper
)

cet_build_plugin(TransferListenerInput ${stype}
  LIBRARIES PRIVATE
  artdaq::ArtModules
  artdaq::ArtdaqInputHelper
)

cet_build_plugin(ArtdaqInput ${stype}
  LIBRARIES PRIVATE
  artdaq::ArtModules
  artdaq::ArtdaqInputHelper
)

cet_make_library(LIBRARY_NAME ArtdaqOutput INTERFACE
  SOURCE ArtdaqOutput.hh
  LIBRARIES INTERFACE
  artdaq::DAQdata
  artdaq-core::Data
  art_root_io::art_root_io
  art::Framework_Core
  art::Framework_Principal
  art::Persistency_Provenance
  canvas::canvas
  fhiclcpp::fhiclcpp
  ROOT::Core
  TRACE::MF
  )

# Use art::module due to issue with art::Output in v3_09_03
cet_build_plugin(RootNetOutput art::module
  LIBRARIES PRIVATE
  artdaq::ArtdaqOutput
  artdaq_plugin_types::ArtdaqSharedMemoryService
  artdaq::DAQrate
  artdaq::DAQdata
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_build_plugin(BinaryNetOutput art::module
  LIBRARIES PRIVATE
  artdaq::ArtdaqOutput
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq-core::Data
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_build_plugin(BinaryFileOutput art::module
  LIBRARIES PRIVATE
  artdaq::ArtdaqOutput
  artdaq::DAQrate
  artdaq::DAQdata
  artdaq-core::Data
  fhiclcpp::fhiclcpp
  TRACE::MF
)

cet_build_plugin(TransferOutput art::module
  LIBRARIES PRIVATE
  artdaq::ArtdaqOutput
  artdaq::DAQdata
  artdaq_plugin_support::transferMaker
  TRACE::MF
)

cet_build_plugin(TransferOutputReliable art::module
  LIBRARIES PRIVATE
  artdaq::ArtdaqOutput
  artdaq::DAQdata
  artdaq_plugin_support::transferMaker
  TRACE::MF
)

cet_build_plugin(OffsetPrescale art::EDFilter
  LIBRARIES PRIVATE
  artdaq_plugin_types::ArtdaqSharedMemoryService
)

include(mfPlugin)
mfPlugin( ArtdaqMetric LIBRARIES REG artdaq::DAQdata )

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
