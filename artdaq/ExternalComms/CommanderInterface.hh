#ifndef artdaq_ExternalComms_CommanderInterface_hh
#define artdaq_ExternalComms_CommanderInterface_hh

#include "fhiclcpp/ParameterSet.h"
#include "artdaq/Application/Commandable.hh"


namespace artdaq
{
	/**
	 * \brief This interface defines the functions used to transfer data between artdaq applications.
	 */
	class CommanderInterface
	{
	public:


		/**
		 * \brief CommanderInterface Constructor
		 * \param ps ParameterSet used for configuring the CommanderInterface
		 * \param commandable artdaq::Commandable object to send transition commands to
		 *
		 * \verbatim
		 * CommanderInterface accepts the following Parameters:
		   "commanderPluginType": The type of Commander plugin to load
		   "id": Integer ID number of this Commandable. May be constrained by plugin types (i.e. XMLRPC port number).
		 * \endverbatim
		 */
		CommanderInterface(const fhicl::ParameterSet& ps, artdaq::Commandable& commandable)
			: _commandable(commandable)
			, _id(ps.get<int>("id", 0))
		{}

		/**
		 * \brief Copy Constructor is deleted
		 */
		CommanderInterface(const CommanderInterface&) = delete;

		/**
		 * \brief Copy Assignment operator is deleted
		 * \return CommanderInterface Copy
		 */
		CommanderInterface& operator=(const CommanderInterface&) = delete;

		/**
		 * \brief Default virtual Destructor
		 */
		virtual ~CommanderInterface() = default;

		/// <summary>
		/// run_server is the main work loop for the Commander.
		/// 
		/// This function is expected to block and persist for the entire run of the application.
		/// It should accept and handle the following commands (subject to state-machine constraints, see Commandable::legal_commands()):
		/// init
		/// soft_init
		/// reinit
		/// start
		/// pause
		/// resume
		/// stop
		/// shutdown
		/// status
		/// report
		/// legal_commands
		/// register_monitor
		/// unregister_monitor
		/// 
		/// See the send_* functions for more details on each command. Not all commands are valid for all applications/states.
		/// run_server should return a string indicating success or failure to the transport mechanism when it is done processing a command.
		/// </summary>
		virtual void run_server() = 0;

		/// <summary>
		/// Using the transport mechanism, send an init command
		/// 
		/// The init command is accepted by all artdaq processes that are in the booted state.
		/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_init(fhicl::ParameterSet, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_init!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a soft_init command
		/// 
		/// The soft_init command is accepted by all artdaq processes that are in the booted state.
		/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_soft_init(fhicl::ParameterSet, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_soft_init!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a reinit command
		/// 
		/// The reinit command is accepted by all artdaq processes.
		/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_reinit(fhicl::ParameterSet, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_reinit!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a start command
		/// 
		/// The start command starts a Run using the given run number.
		/// This command also accepts a timeout parameter and a timestamp parameter.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_start(art::RunID, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_start!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a pause command
		/// 
		/// The pause command pauses a Run. When the run resumes, the subrun number will be incremented.
		/// This command accepts a timeout parameter and a timestamp parameter.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_pause(uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_pause!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a resume command
		/// 
		/// The resume command resumes a paused Run. When the run resumes, the subrun number will be incremented.
		/// This command accepts a timeout parameter and a timestamp parameter.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_resume(uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_resume!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a stop command
		/// 
		/// The stop command stops the current Run.
		/// This command accepts a timeout parameter and a timestamp parameter.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_stop(uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_stop!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a shutdown command
		/// 
		/// The shutdown command shuts down the artdaq process.
		/// This command accepts a timeout parameter.
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_shutdown(uint64_t)
		{
#pragma message "Using default implementation of send_shutdown!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a status command
		/// 
		/// The status command returns the current status of the artdaq process.
		/// </summary>
		/// <returns>Command result: current status of the artdaq process</returns>
		virtual std::string send_status()
		{
#pragma message "Using default implementation of send_status!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a report command
		/// 
		/// The report command returns the current value of the requested reportable quantity.
		/// </summary>
		/// <returns>Command result: current value of the requested reportable quantity</returns>
		virtual std::string send_report(std::string)
		{
#pragma message "Using default implementation of send_report!"
			return "NOT IMPLEMENTED";
		}
		/// <summary>
		/// Using the transport mechanism, send a legal_commands command
		/// 
		/// This will query the artdaq process, and it will return the list of allowed transition commands from its current state.
		/// </summary>
		/// <returns>Command result: a list of allowed transition commands from its current state</returns>
		virtual std::string send_legal_commands()
		{
#pragma message "Using default implementation of send_legal_commands!"
			return "NOT IMPLEMENTED";
		}

		/// <summary>
		/// Using the transport mechanism, send a register_monitor command
		/// 
		/// This will cause a Dispatcher to start an art process with the given FHiCL configuration string
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_register_monitor(std::string)
		{
#pragma message "Using default implementation of send_register_monitor!"
			return "NOT IMPLEMENTED";
		}

		/// <summary>
		/// Using the transport mechanism, send an unregister_monitor command
		/// 
		/// This will cause a Dispatcher to stop sending data to the monitor identified by the given label
		/// </summary>
		/// <returns>Command result: "SUCCESS" if succeeded</returns>
		virtual std::string send_unregister_monitor(std::string)
		{
#pragma message "Using default implementation of send_unregister_monitor!"
			return "NOT IMPLEMENTED";
		}


	private:

	public:
		/// <summary>
		/// Reference to the Commandable that this Commander Commands.
		/// </summary>
		artdaq::Commandable& _commandable;

	protected:
		int _id; ///< ID Number of this Commander
	};
}

#define DEFINE_ARTDAQ_COMMANDER(klass)                                \
  extern "C" std::unique_ptr<artdaq::CommanderInterface> make(fhicl::ParameterSet const & ps, \
								 artdaq::Commandable& commandable) { \
	return std::unique_ptr<artdaq::CommanderInterface>(new klass(ps, commandable)); \
}


#endif /* artdaq_ExternalComms_CommanderInterface.hh */

// Local Variables:
// mode: c++
// End:
