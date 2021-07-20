
art_make( BASENAME_ONLY
    LIBRARY_NAME artdaq_ArtModules_RootDAQOutput
  NO_PLUGINS
  LIB_LIBRARIES
  artdaq_DAQdata
  art_Framework_Core
  art_Framework_IO
  art_Framework_Principal
  art_Framework_Services_Registry
  art_Utilities
  art_root_io
  art_root_io_RootDB
  art_root_io_detail
  canvas_root_io
  canvas
  ${SQLITE3}
  cetlib
  hep_concurrency
  ${ROOT_RIO}
  ${ROOT_TREE}
  )

simple_plugin(RootDAQOut "module"
  artdaq_ArtModules_RootDAQOutput
  art_Framework_IO_detail
)


