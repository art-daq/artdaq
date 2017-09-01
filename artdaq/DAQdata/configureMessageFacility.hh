#ifndef artdaq_Application_configureMessageFacility_hh
#define artdaq_Application_configureMessageFacility_hh

#include <string>

namespace artdaq
{
	/**
	* \brief Create the MessageFacility configuration Fhicl string
	* \param progname The name of the program
	* \param useConsole Should console output be activated? Default = true
	* \return Fhicl string with generated MessageFacility configuration
	* \throw cet::exception if log path or ARTDAQ_LOG_FHICL do not exist
	*/
	std::string generateMessageFacilityConfiguration(char const* progname, bool useConsole = true);

	/**
	 * \brief Configure and start the message facility. Provide the program name so that messages will be appropriately tagged.
	 * \param progname The name of the program
	 * \param useConsole Should console output be activated? Default = true
	 */
	void configureMessageFacility(char const* progname, bool useConsole = true);

	/**
	 * \brief Set the message facility application name using the specified application type and port number
	 * \param appType Application name
	 * \param port XMLRPC port of this application instance
	 */
	void setMsgFacAppName(const std::string& appType, unsigned short port);
}

#endif /* artdaq_Application_configureMessageFacility_hh */
