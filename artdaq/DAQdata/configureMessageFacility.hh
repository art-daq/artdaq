#ifndef artdaq_Application_configureMessageFacility_hh
#define artdaq_Application_configureMessageFacility_hh

#include <string>

namespace artdaq
{
	// Configure and start the message facility. Provide the program
	// name so that messages will be appropriately tagged.
	void configureMessageFacility(char const* progname, bool useConsole = true);

	// Set the message facility application name using the specified
	// application type and port number
	void setMsgFacAppName(const std::string& appType, unsigned short port);
}

#endif /* artdaq_Application_configureMessageFacility_hh */
