# ======================================================================
#
# Build/install script
#
# ======================================================================

# ----------------------------------------------------------------------
# Build this project's library:

art_make(BASENAME_ONLY
  LIBRARY_NAME artdaq_DAQrate
  SUBDIRS detail
  LIB_LIBRARIES
  artdaq_DAQdata
  artdaq_TransferPlugins
  artdaq-utilities_Plugins
  artdaq-core_Core
  artdaq-core_Data
  artdaq-core_Utilities
  ${FHICLCPP}
  ${MF_MESSAGELOGGER}
  ${MF_UTILITIES}
  ${CLHEP}
  ${Boost_THREAD_LIBRARY}
  ${Boost_SYSTEM_LIBRARY} 
  pthread
  )

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
