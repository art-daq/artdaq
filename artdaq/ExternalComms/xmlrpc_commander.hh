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

	/// <summary>
	/// Send a register_monitor command over XMLRPC
	/// </summary>
	/// <param name="monitor_fhicl">FHiCL string contianing monitor configuration</param>
	/// <returns>Return status from XMLRPC</returns>
	std::string send_register_monitor(std::string monitor_fhicl) override;

	/// <summary>
	/// Send an unregister_monitor command over XMLRPC
	/// </summary>
	/// <param name="monitor_label">Label of the monitor to unregister</param>
	/// <returns>Return status from XMLRPC</returns>
	std::string send_unregister_monitor(std::string monitor_label) override;

private:
	xmlrpc_commander(const xmlrpc_commander&) = delete;

	xmlrpc_commander(xmlrpc_commander&&) = delete;

	int port_;
	std::string serverUrl_;

public:
	std::timed_mutex mutex_; ///< XMLRPC mutex
	std::unique_ptr<xmlrpc_c::serverAbyss> server; ///< XMLRPC server
};

}

#endif /* artdaq_ExternalComms_xmlrpc_commander_hh */
