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
  artdaq-core::Utilities
  artdaq-core::Plugins
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

# PAM module and vncpasswd utility for VNC password authentication
find_package(PAM QUIET)
if(PAM_FOUND)
  # PAM authentication module
  add_library(pam_vncpasswd MODULE pam_vncpasswd.c)
  target_include_directories(pam_vncpasswd PRIVATE
    ${PAM_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(pam_vncpasswd PRIVATE ${PAM_LIBRARIES})
  set_target_properties(pam_vncpasswd PROPERTIES PREFIX "")
  install(TARGETS pam_vncpasswd LIBRARY DESTINATION lib/security)

  # vncpasswd utility
  add_executable(vncpasswd vncpasswd.c)
  target_include_directories(vncpasswd PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  install(TARGETS vncpasswd RUNTIME DESTINATION bin)

  # Self-test
  add_executable(test_vncpasswd test_vncpasswd.c)
  target_include_directories(test_vncpasswd PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME test_vncpasswd COMMAND test_vncpasswd)

  # Man pages
  install(FILES vncpasswd.1 DESTINATION share/man/man1)
  install(FILES pam_vncpasswd.8 DESTINATION share/man/man8)

  # System-wide default configuration template
  install(FILES vncpasswd.conf DESTINATION share/vnc)

  # Optional: clang-format check (does not modify files)
  find_program(CLANG_FORMAT clang-format)
  if(CLANG_FORMAT)
    add_custom_target(format-check-vnc
      COMMAND ${CLANG_FORMAT} --dry-run --Werror
        ${CMAKE_CURRENT_SOURCE_DIR}/pam_vncpasswd.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vncpasswd.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vnc_config.h
        ${CMAKE_CURRENT_SOURCE_DIR}/test_vncpasswd.c
      COMMENT "Checking VNC tool formatting with clang-format"
    )
    add_custom_target(format-vnc
      COMMAND ${CLANG_FORMAT} -i
        ${CMAKE_CURRENT_SOURCE_DIR}/pam_vncpasswd.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vncpasswd.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vnc_config.h
        ${CMAKE_CURRENT_SOURCE_DIR}/test_vncpasswd.c
      COMMENT "Formatting VNC tool sources with clang-format"
    )
  endif()

  # Optional: cppcheck static analysis
  find_program(CPPCHECK cppcheck)
  if(CPPCHECK)
    add_custom_target(cppcheck-vnc
      COMMAND ${CPPCHECK}
        --enable=warning,performance,portability
        --error-exitcode=1
        --suppress=missingIncludeSystem
        -I ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/pam_vncpasswd.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vncpasswd.c
        ${CMAKE_CURRENT_SOURCE_DIR}/test_vncpasswd.c
      COMMENT "Running cppcheck static analysis on VNC tools"
    )
  endif()
else()
  message(STATUS "PAM not found; pam_vncpasswd and vncpasswd will not be built")
endif()

install_headers()
install_source()
install_fhicl()
