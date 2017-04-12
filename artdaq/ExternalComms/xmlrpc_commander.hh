/* DarkSide 50 DAQ program
 * This file add the xmlrpc commander as a client to the SC
 * Author: Alessandro Razeto <Alessandro.Razeto@ge.infn.it>
 */
#ifndef artdaq_ExternalComms_xmlrpc_commander_hh
#define artdaq_ExternalComms_xmlrpc_commander_hh

#include <mutex>
#include "artdaq/Application/Commandable.hh"

class xmlrpc_commander
{
public:
	xmlrpc_commander(int port, artdaq::Commandable& commandable);

	void run();

private:
	xmlrpc_commander(const xmlrpc_commander&) = delete;

	xmlrpc_commander(xmlrpc_commander&&) = delete;

	int _port;

public:
	artdaq::Commandable& _commandable;
	std::mutex mutex_;
};

#endif /* artdaq_ExternalComms_xmlrpc_commander_hh */
