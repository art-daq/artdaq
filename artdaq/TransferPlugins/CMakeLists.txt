
if (${USING_RTIDDS})
include_directories(${MPI_INCLUDE_PATH} $ENV{NDDSHOME}/include $ENV{NDDSHOME}/include/ndds )
else()
include_directories(${MPI_INCLUDE_PATH} )
endif()

art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_TransferPlugins
  NO_PLUGINS
  EXCLUDE RTIDDS_transfer.cc shmem_transfer.cc
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

simple_plugin(TransferInput "source"
#  artdaq_ArtModules
  artdaq_TransferPlugins
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  MF_MessageLogger
  MF_Utilities
  fhiclcpp
  cetlib
)

simple_plugin(shmem "transfer"
  artdaq-core_Utilities
  artdaq_DAQrate
  MF_MessageLogger
  fhiclcpp
  ${ROOT_RIO}
  ${ROOT_NET}
  ${ROOT_CORE}
)

if (${USING_RTIDDS})

simple_plugin(RTIDDS "transfer"
  artdaq-core_Utilities
  artdaq_RTIDDS
  artdaq_DAQrate
  MF_MessageLogger
  fhiclcpp
  ${ROOT_RIO}
  ${ROOT_NET}
  ${ROOT_CORE}
)

endif()

