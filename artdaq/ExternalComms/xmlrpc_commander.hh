/* DarkSide 50 DAQ program
 * This file add the xmlrpc commander as a client to the SC
 * Author: Alessandro Razeto <Alessandro.Razeto@ge.infn.it>
 */
#ifndef artdaq_ExternalComms_xmlrpc_commander_hh
#define artdaq_ExternalComms_xmlrpc_commander_hh

#include <mutex>
#include "artdaq/Application/Commandable.hh"

namespace artdaq
{
	class xmlrpc_commander;
}

/**
 * \brief The xmlrpc_commander class serves as the XMLRPC server run in each artdaq application
 */
class artdaq::xmlrpc_commander
{
public:
	/**
	 * \brief xmlrpc_commander Constructor
	 * \param port Port to listen on
	 * \param commandable artdaq::Commandable object to send transition commands to
	 */
	xmlrpc_commander(int port, artdaq::Commandable& commandable);

	/**
	 * \brief Run the XMLRPC server
	 */
	void run();

private:
	xmlrpc_commander(const xmlrpc_commander&) = delete;

	xmlrpc_commander(xmlrpc_commander&&) = delete;

	int _port;

public:
	artdaq::Commandable& _commandable; ///< The artdaq::Commandable object that this xmlrpc_commander sends commands to
	std::mutex mutex_; ///< XMLRPC mutex
};

#endif /* artdaq_ExternalComms_xmlrpc_commander_hh */
