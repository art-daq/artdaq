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

# use cmake 3.17 or later
cmake_minimum_required (VERSION 3.19 FATAL_ERROR)

find_package(cetmodules 3.16.00 REQUIRED)

project(artdaq VERSION 3.12.01)

include(CetCMakeEnv)
cet_cmake_env()

# Find our own CMake modules, and tell everyone else where to find them.
cet_cmake_module_directories(Modules BINARY)

cet_set_compiler_flags(DIAGS VIGILANT
  WERROR
  NO_UNDEFINED
  EXTRA_FLAGS -pedantic -DRTI_UNIX -DRTI_LINUX -DRTI_64BIT -m64 
)

string(TOUPPER ${CMAKE_BUILD_TYPE} BTYPE_UC)
if(BTYPE_UC STREQUAL DEBUG)
  add_definitions(-DDEBUGME)
	if(DEFINED ENV{USE_ASAN})
		if($ENV{USE_ASAN} GREATER 0)
			message("!!! Address Sanitizer Activated!!!")
			message("!!! Note: To run, add 'export ASAN_OPTIONS=verify_asan_link_order=0' to setup !!!")
			add_compile_options(-fsanitize=address -Wno-unused-command-line-argument)
			add_link_options(-lasan)
		endif()
	endif()
	if(DEFINED ENV{USE_TSAN})
		if($ENV{USE_TSAN} GREATER 0)
			message("!!! Thread Sanitizer Activated!!!")
			add_compile_options(-fsanitize=thread -Wno-unused-command-line-argument -Wno-tsan)
			add_link_options(-ltsan)
		endif()
	endif()
	if(DEFINED ENV{USE_UBSAN})
		if($ENV{USE_UBSAN} GREATER 0)
			message("!!! Undefined Sanitizer Activated!!!")
			add_compile_options(-fsanitize=undefined -Wno-unused-command-line-argument)
			add_link_options(-lubsan)
		endif()
	endif()
	if(DEFINED ENV{USE_GCOV})
		add_compile_options(-fprofile-arcs -ftest-coverage --coverage)
		add_link_options(--coverage)
	endif()
endif()

#exeption stack trace
if(DEFINED ENV{USE_EXCEPTIONSTACKTRACE})	
	if($ENV{USE_EXCEPTIONSTACKTRACE} GREATER 0)
		message("!!! Exception StackTrace Activated!!!")
		add_definitions(-D EXCEPTIONSTACKTRACE)
		set( EXCEPTIONSTACKTRACE_LIB  artdaq-core_Utilities_ExceptionStackTrace )
		add_compile_options( -rdynamic -Wno-unused-command-line-argument )
		add_link_options( -rdynamic )
	endif()
endif()


#cet_report_compiler_flags()
find_package(TRACE 3.17.04 REQUIRED EXPORT)
find_package(artdaq_core 3.08.04 REQUIRED EXPORT)
find_package(artdaq_utilities 1.07.02 REQUIRED EXPORT)

find_package(art_root_io 1.08.03 REQUIRED EXPORT)
find_package(art 3.09.03 REQUIRED EXPORT)
find_package(canvas_root_io REQUIRED EXPORT)
find_package(canvas REQUIRED EXPORT)
find_package(messagefacility REQUIRED EXPORT)
find_package(fhiclcpp REQUIRED EXPORT)
find_package(cetlib REQUIRED EXPORT)
find_package(cetlib_except REQUIRED EXPORT)
find_package(Boost COMPONENTS date_time program_options thread REQUIRED EXPORT)
find_package(XMLRPC COMPONENTS c++2 client abyss-server REQUIRED EXPORT)

include(BuildPlugins)

string(REGEX MATCHALL "[0-9][0-9]?[0-9]?" ART_VERSION_LIST "${art_VERSION}")
list(GET ART_VERSION_LIST 0 ART_MAJOR_VERSION)
list(GET ART_VERSION_LIST 1 ART_MINOR_VERSION)
list(GET ART_VERSION_LIST 2 ART_PATCH_VERSION)

message("-DART_HEX_VERSION=0x${ART_MAJOR_VERSION}${ART_MINOR_VERSION}${ART_PATCH_VERSION}")
set(ART_HEX_VERSION 0x${ART_MAJOR_VERSION}${ART_MINOR_VERSION}${ART_PATCH_VERSION})
add_definitions(-DART_HEX_VERSION=0x${ART_MAJOR_VERSION}${ART_MINOR_VERSION}${ART_PATCH_VERSION})

# Debug streamer.
string(TOUPPER ${CMAKE_BUILD_TYPE} BTYPE_UC)
if(BTYPE_UC STREQUAL DEBUG)
  add_definitions(-DDEBUGME)
endif()

if (DEFINED ENV{NDDSHOME})
   set( USING_RTIDDS TRUE )
   message("\"NDDSHOME\" environment variable defined; will build RTI-DDS code")
else()
   set( USING_RTIDDS FALSE )
   message("\"NDDSHOME\" environment variable not defined; will ignore RTI-DDS code")
endif()

# source
add_subdirectory(artdaq)
cet_register_export_set(SET_NAME artdaqUsers SET_DEFAULT)

# testing
add_subdirectory(test)

# Prototype stuff (kept around for tests)
add_subdirectory(proto)

# Useful tools and scripts.
add_subdirectory(tools)

if (NOT DEFINED ENV{DISABLE_DOXYGEN})
add_subdirectory(doc)
endif()

cet_cmake_config()
