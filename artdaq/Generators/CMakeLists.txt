# ======================================================================
#
# Build/install script
#
# ======================================================================

# ----------------------------------------------------------------------
# Build this project's library:

art_make(BASENAME_ONLY
  LIBRARY_NAME artdaq_Generators
  NO_PLUGINS
  LIB_LIBRARIES
  artdaq_DAQdata
  artdaq_DAQrate
  artdaq_core::artdaq-core_Data
  )
simple_plugin(CompositeDriver "generator" artdaq_DAQdata artdaq_Generators)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
