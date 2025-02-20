# Make sure tests have correct environment settings.
include(CetTest)
# If you explicitly include CetTest in a subdirectory, you will need to
# re-initialize the test environment.
cet_test_env("FHICL_FILE_PATH=.")

cet_script(ALWAYS_COPY installMessageViewer.sh installEpicsPlugin.sh installGangliaPlugin.sh disk_stats.sh)

cet_make_exec(NAME periodic_cmd_stats )

cet_make_exec(NAME StateResponder
  LIBRARIES
  artdaq_plugin_support::commanderMaker
  artdaq::Application
  Boost::program_options
)

cet_make_exec(NAME genToArt
  LIBRARIES
  artdaq_plugin_support::CFGMaker
  artdaq::DAQrate
  artdaq::DAQdata_GenericFragmentSimulator_generator
  artdaq_core::artdaq-core_Utilities
  artdaq_core::artdaq-core_Plugins
  canvas::canvas
  fhiclcpp::fhiclcpp
  cetlib::cetlib
  cetlib_except::cetlib_except
  Boost::program_options
)

cet_script(ALWAYS_COPY setupMpichPlugin.sh)

# Make sure tests have correct environment settings.
include(CetTest)
# If you explicitly include CetTest in a subdirectory, you will need to
# re-initialize the test environment.
cet_test_env("FHICL_FILE_PATH=.")

cet_test(genToArt_t HANDBUILT
  TEST_EXEC genToArt
  TEST_ARGS -c genToArt_t.fcl
  DATAFILES fcl/genToArt_t.fcl
)

cet_test(genToArt_outToBinaryFileOutput_t HANDBUILT
  TEST_EXEC genToArt
  TEST_ARGS -c genToArt_outToBinaryFileOutput_t.fcl
  DATAFILES fcl/genToArt_outToBinaryFileOutput_t.fcl
)

# Make Modules
include(PythonAddon)

cet_make_library(LIBRARY_NAME swig_artdaq
  SOURCE swig_artdaq.cc
  LIBRARIES
  PUBLIC
  TRACE::MF
  PRIVATE
  artdaq::Application
  artdaq::DAQdata
)

create_python_addon (ADDON_NAME "python_artdaq" LIBRARIES swig_artdaq)

#cet_test(genToArt_outToBinaryNetOutput_t HANDBUILT
#  TEST_EXEC mpirun
#  TEST_ARGS genToArt -c genToArt_outToBinaryNetOutput_t.fcl :  s_r_handles 1
#  DATAFILES fcl/genToArt_outToBinaryNetOutput_t.fcl)

install_headers()
install_source()
install_fhicl()
