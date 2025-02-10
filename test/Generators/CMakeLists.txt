cet_test(CommandableFragmentGenerator_t USE_BOOST_UNIT INSTALL_BIN
  LIBRARIES PRIVATE
  artdaq_plugin_types::CommandableFragmentGenerator
  artdaq::DAQdata
  artdaq-core::Data
  artdaq-core::Utilities
  TRACE::MF
)
