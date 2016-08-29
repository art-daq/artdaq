
if (${USING_RTIDDS})
include_directories(${MPI_INCLUDE_PATH} $ENV{NDDSHOME}/include $ENV{NDDSHOME}/include/ndds )
else()
include_directories(${MPI_INCLUDE_PATH} )
endif()

art_make( BASENAME_ONLY
  LIBRARY_NAME artdaq_TransferPlugins
  NO_PLUGINS
  EXCLUDE RTIDDS_transfer.cc shmem_transfer.cc multicast_transfer.cc
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
  ${XMLRPC_CLIENTPP}
  ${XMLRPCPP}
  ${XMLRPC_UTILPP}
  )

simple_plugin(TransferInput "source"
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
  ${XMLRPC_SERVER_ABYSSPP}
  ${XMLRPC_SERVER_ABYSS}
  ${XMLRPC_SERVERPP}
  ${XMLRPC_SERVER}
  ${XMLRPC_ABYSS}
  ${XMLRPCPP}
  ${XMLRPC}
  ${XMLRPC_UTIL}
  ${XMLRPC_UTILPP}
  ${XMLRPC_XMLPARSE}
  ${XMLRPC_XMLTOK}
  ${XMLRPC_CLIENTPP}
#  ${XMLRPCPP}
  ${XMLRPC_UTILPP}
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

simple_plugin(multicast "transfer"
  artdaq_DAQrate
  artdaq-core_Data			
  artdaq-core_Utilities
  MF_MessageLogger
  cetlib
  fhiclcpp
  ${Boost_SYSTEM_LIBRARY}
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

