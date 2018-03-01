
if (${USING_RTIDDS})
include_directories($ENV{NDDSHOME}/include $ENV{NDDSHOME}/include/ndds )
endif()

art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_TransferPlugins
  NO_PLUGINS
  SUBDIRS detail
  EXCLUDE RTIDDS_transfer.cc Shmem_transfer.cc Multicast_transfer.cc TCPSocket_transfer.cc Autodetect_transfer.cc
  DICT_DICTIONARIES
  ${ART_PERSISTENCY_PROVENANCE}
  LIB_LIBRARIES
  artdaq-core_Utilities
  artdaq-core_Data
  artdaq_DAQdata
  art_Framework_Core
  art_Framework_IO_Sources
  art_Framework_Principal
  art_Persistency_Provenance
  art_Utilities
  cetlib
  ${CETLIB_EXCEPT}
  fhiclcpp
  MF_MessageLogger
  MF_Utilities
  ${ROOT_CINTEX}
  ${ROOT_RIO}
  ${ROOT_CORE}
  ${ROOT_NET}
  ${ROOT_REFLEX}
  ${Boost_THREAD_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${TBB}
  )
  

simple_plugin(Shmem "transfer"
  artdaq_TransferPlugins
  artdaq-core_Core
)

simple_plugin(Null "transfer"
  artdaq_TransferPlugins
  )

simple_plugin(Multicast "transfer"
  artdaq_TransferPlugins
  ${Boost_SYSTEM_LIBRARY}
  pthread
)

simple_plugin(TCPSocket "transfer"
  artdaq_TransferPlugins
  pthread
)

simple_plugin(Autodetect "transfer"
  artdaq_TransferPlugins
  artdaq_TransferPlugins_Shmem_transfer
  artdaq_TransferPlugins_TCPSocket_transfer
)

if (${USING_RTIDDS})

simple_plugin(RTIDDS "transfer"
  artdaq_TransferPlugins
  artdaq_RTIDDS
)

endif()

install_headers(SUBDIRS detail)
install_fhicl(SUBDIRS fcl)
install_source(SUBDIRS detail)
