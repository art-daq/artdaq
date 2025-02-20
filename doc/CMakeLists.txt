include(artdaq_doxygen)

create_doxygen_documentation(EXCLUDE
	artdaq/ArtModules/RootDAQOutput-s81/RootDAQOutFile.h
	artdaq/ArtModules/RootDAQOutput-s81/RootDAQOutFile.cc
	artdaq/ArtModules/RootDAQOutput-s81/RootDAQOut_module.cc
	artdaq/Application/detail/statemap.h
	artdaq/Application/detail/Commandable_sm.h
	artdaq/Application/detail/Commandable_sm.cpp
	DEPENDS
	artdaq-core
	artdaq-utilities
    artdaq-mfextensions
)
