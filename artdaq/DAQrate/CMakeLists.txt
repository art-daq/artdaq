# ======================================================================
#
# Build/install script
#
# ======================================================================

# ----------------------------------------------------------------------
# Build this project's library:

cet_make(BASENAME_ONLY
  LIBRARY_NAME artdaq_DAQrate
  SUBDIRS detail
  LIB_LIBRARIES
  artdaq_DAQdata
  artdaq_TransferPlugins
  artdaq_utilities::artdaq-utilities_Plugins
  artdaq_core::artdaq-core_Core
  artdaq_core::artdaq-core_Data
  artdaq_core::artdaq-core_Utilities
  artdaq_RoutingPolicies
  fhiclcpp
  MF_MessageLogger
  ${Boost_THREAD_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  pthread
  )

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
