include(artdaq_doxygen)

create_doxygen_documentation(EXCLUDE 
	artdaq/ArtModules/RootDAQOutFile.h 
	artdaq/ArtModules/RootDAQOutFile.cc
	artdaq/ArtModules/RootDAQOut_module.cc
	artdaq/ArtModules/detail/KeptProvenance.h
	artdaq/ArtModules/detail/KeptProvenance.cc
)
