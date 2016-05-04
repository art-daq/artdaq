# ======================================================================
#  artdaq main build file
#
#  cd .../path/to/build/directory
#  source .../path/to/artdaq/ups/setup_for_development
#  cmake [-DCMAKE_INSTALL_PREFIX=/install/path]
#        -Dqualifier:STRING=$CETPKG_QUAL
#        -DCMAKE_BUILD_TYPE=$CETPKG_TYPE
#        $CETPKG_SOURCE
#  make
#  make test
#  make install
#  make package
# ======================================================================

# use cmake 2.8 or later
cmake_minimum_required (VERSION 2.8)

project(artdaq)
# cetbuildtools contains our cmake modules
SET ( CETBUILDTOOLS_VERSION $ENV{CETBUILDTOOLS_VERSION} )
IF (NOT CETBUILDTOOLS_VERSION)
    MESSAGE (FATAL_ERROR "ERROR: setup cetbuildtools to get the cmake modules")
ENDIF()

set(CMAKE_MODULE_PATH $ENV{ART_DIR}/Modules
		      $ENV{CETBUILDTOOLS_DIR}/Modules
		      ${CMAKE_MODULE_PATH})

include(CetCMakeEnv)
cet_cmake_env()

cet_set_compiler_flags(DIAGS VIGILANT
  WERROR
  NO_UNDEFINED
  EXTRA_FLAGS -pedantic
)

string(TOUPPER ${CMAKE_BUILD_TYPE} BTYPE_UC )
#if( ${BTYPE_UC} MATCHES "DEBUG" )
#  cet_add_compiler_flags(-fsanitize=address)
#endif()

cet_report_compiler_flags()

# these are minimum required versions, not the actual product versions
find_ups_boost(v1_53_0)
find_ups_product (cetbuildtools v4_09_02)
find_ups_product( TRACE v3_03_00 )
find_ups_product( art v1_08_10 )
find_ups_product( artdaq_core v1_03_01 )
find_ups_product( artdaq_utilities v1_00_00)
find_ups_product( cetlib v1_08_00 )
find_ups_product( cetpkgsupport v1_08_03 )
find_ups_product( clhep v2_2_0_3 )
find_ups_product( fhiclcpp v3_04_00 )
find_ups_product( messagefacility v1_14_01 )
find_ups_product( sqlite v3_08_05_00 )
find_ups_product( tbb v4_1 )
find_ups_product( xmlrpc_c v1_25_18 )
find_ups_root( v5_34_20 )

# Include at the top level to make sure include directory is visible
# everywhere.
include(ProcessSmc)

check_ups_version(art "$ENV{ART_VERSION}" v1_11_00 PRODUCT_OLDER_VAR have_old_art)
if (have_old_art)
  cmake_policy(SET CMP0046 OLD)
endif()
unset(have_old_art)

check_ups_version(art "$ENV{ART_VERSION}" v1_14_00 PRODUCT_OLDER_VAR EVENTID_HAS_EXPLICIT_RUNID)
if (EVENTID_HAS_EXPLICIT_RUNID)
  add_definitions(-DARTDAQ_ART_EVENTID_HAS_EXPLICIT_RUNID)
endif()

string(REGEX MATCHALL "[0-9]+" ART_VERSION_LIST "$ENV{ART_VERSION}")
list(GET ART_VERSION_LIST 0 ART_MAJOR_VERSION)
list(GET ART_VERSION_LIST 1 ART_MINOR_VERSION)
list(GET ART_VERSION_LIST 2 ART_PATCH_VERSION)
add_definitions(-DART_MAJOR_VERSION=${ART_MAJOR_VERSION} -DART_MINOR_VERSION=${ART_MINOR_VERSION} -DART_PATCH_VERSION=${ART_PATCH_VERSION})

find_library( XMLRPC_SERVER_ABYSSPP NAMES xmlrpc_server_abyss++ PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_SERVER_ABYSS NAMES xmlrpc_server_abyss PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_SERVERPP NAMES xmlrpc_server++ PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_SERVER NAMES xmlrpc_server PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_ABYSS NAMES xmlrpc_abyss PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_CLIENTPP NAMES xmlrpc_client++ PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPCPP NAMES xmlrpc++ PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC NAMES xmlrpc PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_UTIL NAMES xmlrpc_util PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_XMLPARSE NAMES xmlrpc_xmlparse PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( XMLRPC_XMLTOK NAMES xmlrpc_xmltok PATHS $ENV{XMLRPC_C_LIBRARY} )
find_library( TRACE trace PATHS $ENV{TRACE_LIB} )

# Debug streamer.
string(TOUPPER ${CMAKE_BUILD_TYPE} BTYPE_UC)
if(BTYPE_UC STREQUAL DEBUG)
  add_definitions(-DDEBUGME)
endif()

find_package(MPI QUIET)

# macros for artdaq_dictionary and simple_plugin
include(ArtDictionary)
include(ArtMake)
include(BuildPlugins)

# testing
add_subdirectory(test)

# source
add_subdirectory(artdaq)

# Prototype stuff (kept around for tests)
add_subdirectory(proto)

# Useful tools and scripts.
add_subdirectory(tools)

# ups - table and config files
add_subdirectory(ups)

# packaging utility
include(UseCPack)
