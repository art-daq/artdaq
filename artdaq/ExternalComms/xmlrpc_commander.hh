/* DarkSide 50 DAQ program
 * This file add the xmlrpc commander as a client to the SC
 * Author: Alessandro Razeto <Alessandro.Razeto@ge.infn.it>
 */
#ifndef artdaq_ExternalComms_xmlrpc_commander_hh
#define artdaq_ExternalComms_xmlrpc_commander_hh

#include <mutex>
#include "artdaq/ExternalComms/CommanderInterface.hh"

namespace artdaq
{

/**
 * \brief The xmlrpc_commander class serves as the XMLRPC server run in each artdaq application
 */
class xmlrpc_commander : public CommanderInterface
{
public:
	/**
	 * \brief xmlrpc_commander Constructor
	 * \param ps ParameterSet used for configuring xmlrpc_commander
	 * \param commandable artdaq::Commandable object to send transition commands to
	 *
	 * \verbatim
	  xmlrpc_commander accepts the following Parameters:
	   id: For XMLRPC, the ID should be the port to listen on
	   server_url: When sending, location of XMLRPC server
	 * \endverbatim
	 */
	xmlrpc_commander(fhicl::ParameterSet ps, artdaq::Commandable& commandable);

	/**
	 * \brief Run the XMLRPC server
	 */
	void run_server() override;

	std::string send_register_monitor(std::string monitor_fhicl) override;

	std::string send_unregister_monitor(std::string monitor_label) override;

private:
	xmlrpc_commander(const xmlrpc_commander&) = delete;

	xmlrpc_commander(xmlrpc_commander&&) = delete;

	int port_;
	std::string serverUrl_;

public:
	std::mutex mutex_; ///< XMLRPC mutex
};

}

#endif /* artdaq_ExternalComms_xmlrpc_commander_hh */
