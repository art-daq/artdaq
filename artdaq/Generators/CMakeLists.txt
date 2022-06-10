# ======================================================================
#
# Build/install script
#
# ======================================================================

# ----------------------------------------------------------------------
# Build this project's library:

cet_make(
	EXCLUDE CompositeDriver_generator.cc
  LIBRARIES
  artdaq_DAQdata
  artdaq_DAQrate
  artdaq_core::artdaq-core_Data
  )

  include(generator)

cet_build_plugin(CompositeDriver generator LIBRARIES REG artdaq_DAQdata artdaq_Generators)

install_headers(SUBDIRS detail)
install_source(SUBDIRS detail)
