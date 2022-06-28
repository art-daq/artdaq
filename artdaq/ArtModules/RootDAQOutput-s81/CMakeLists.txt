cet_make_library(LIBRARY_NAME RootDAQOutput USE_PROJECT_NAME NO_EXPORT
  SOURCE
  RootDAQOutFile.cc
  LIBRARIES
  PUBLIC
  art_root_io::art_root_io
  art::Framework_IO
  art::Framework_Services_System_FileCatalogMetadata_service
  canvas::canvas
  cetlib::sqlite
  ROOT::Core
  ROOT::RIO
  PRIVATE
  artdaq::DAQdata
  art_root_io::RootDB
  art_root_io::detail
  canvas_root_io::canvas_root_io
  art::Framework_Core
  art::Framework_Principal
  art::Framework_Services_Registry
  art::Persistency_Provenance
  fhiclcpp::fhiclcpp
  cetlib::cetlib
  cetlib::container_algorithms
  ROOT::Core
  ROOT::RIO
  ROOT::Tree
  TRACE::MF
  Boost::date_time
)

# Use art::module due to issue with art::Output in v3_09_03
cet_build_plugin(RootDAQOut art::module 
  LIBRARIES PRIVATE
  artdaq::RootDAQOutput
  artdaq::ArtModules
  art_root_io::detail
  art::Framework_IO_detail
  TRACE::MF
)


