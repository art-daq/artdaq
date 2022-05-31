
cet_make(
  EXCLUDE RootDAQOut_module.cc
  LIBRARIES
  artdaq_DAQdata
  )

 cet_build_plugin(RootDAQOut art::module LIBRARIES REG
  artdaq_ArtModules_RootDAQOutput
)


