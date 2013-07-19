#ifndef artdaq_Application_configureMessageFacility_hh
#define artdaq_Application_configureMessageFacility_hh

namespace artdaq
{
  // Configure and start the message facility. Provide the program
  // name so that messages will be appropriately tagged.
  void configureMessageFacility(char const* progname);
}

#endif /* artdaq_Application_configureMessageFacility_hh */