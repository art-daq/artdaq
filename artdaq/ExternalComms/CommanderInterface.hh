#ifndef artdaq_ExternalComms_CommanderInterface_hh
#define artdaq_ExternalComms_CommanderInterface_hh

#include "artdaq/Application/Commandable.hh"

#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Comment.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/Name.h"

#include "cetlib/compiler_macros.h"

#include <atomic>
#include <memory>
#include <string>

namespace artdaq {
/**
 * \brief This interface defines the functions used to transfer data between artdaq applications.
 */
class CommanderInterface
{
public:
	/// <summary>
	/// Configuration of the CommanderInterface. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "id" (Default: 0): Integer ID number of this Commandable.May be constrained by plugin types(i.e.XMLRPC port number).
		fhicl::Atom<int> id{fhicl::Name{"id"}, fhicl::Comment{"The unique ID associated with this Commander plugin. (ex. XMLRPC Port number)"}, 0};
		/// "commanderPluginType" (REQUIRED): The type of Commander plugin to load
		fhicl::Atom<std::string> commanderPluginType{fhicl::Name{"commanderPluginType"}, fhicl::Comment{"String identifying the name of the CommanderInterface plugin to load"}};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
	 * \brief CommanderInterface Constructor
	 * \param ps ParameterSet used for configuring the CommanderInterface. See artdaq::CommanderInterface::Config
	 * \param commandable artdaq::Commandable object to send transition commands to
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
	virtual ~CommanderInterface();

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
	/// trace_get
	/// trace_set
	/// meta_command
	/// rollover_subrun
	/// add_config_archive_entry
	/// clear_config_archive
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
	/// <param name="ps">ParameterSet for the command</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_init(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a soft_init command
	///
	/// The soft_init command is accepted by all artdaq processes that are in the booted state.
	/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
	/// </summary>
	/// <param name="ps">ParameterSet for the command</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_soft_init(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a reinit command
	///
	/// The reinit command is accepted by all artdaq processes.
	/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
	/// </summary>
	/// <param name="ps">ParameterSet for the command</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_reinit(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a start command
	///
	/// The start command starts a Run using the given run number.
	/// This command also accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="runNumber">Run number of the new run</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_start(art::RunID runNumber, uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a pause command
	///
	/// The pause command pauses a Run. When the run resumes, the subrun number will be incremented.
	/// This command accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_pause(uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a resume command
	///
	/// The resume command resumes a paused Run. When the run resumes, the subrun number will be incremented.
	/// This command accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_resume(uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a stop command
	///
	/// The stop command stops the current Run.
	/// This command accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_stop(uint64_t timeout, uint64_t timestamp);

	/// <summary>
	/// Using the transport mechanism, send a shutdown command
	///
	/// The shutdown command shuts down the artdaq process.
	/// This command accepts a timeout parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_shutdown(uint64_t timeout);

	/// <summary>
	/// Using the transport mechanism, send a status command
	///
	/// The status command returns the current status of the artdaq process.
	/// </summary>
	/// <returns>Command result: current status of the artdaq process</returns>
	virtual std::string send_status();

	/// <summary>
	/// Using the transport mechanism, send a report command
	///
	/// The report command returns the current value of the requested reportable quantity.
	/// </summary>
	/// <param name="which">Reportable quantity to request</param>
	/// <returns>Command result: current value of the requested reportable quantity</returns>
	virtual std::string send_report(std::string const& which);

	/// <summary>
	/// Using the transport mechanism, send a legal_commands command
	///
	/// This will query the artdaq process, and it will return the list of allowed transition commands from its current state.
	/// </summary>
	/// <returns>Command result: a list of allowed transition commands from its current state</returns>
	virtual std::string send_legal_commands();

	/// <summary>
	/// Using the transport mechanism, send a register_monitor command
	///
	/// This will cause a Dispatcher to start an art process with the given FHiCL configuration string
	/// </summary>
	/// <param name="monitor_fhicl">FHiCL code used to configure the art process that the Dispatcher starts</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_register_monitor(std::string const& monitor_fhicl);

	/// <summary>
	/// Using the transport mechanism, send an unregister_monitor command
	///
	/// This will cause a Dispatcher to stop sending data to the monitor identified by the given label
	/// </summary>
	/// <param name="label">Label of the monitor to unregister</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_unregister_monitor(std::string const& label);

	/// <summary>
	/// Using the transport mechanism, send an send_trace_get command
	///
	/// This will cause the receiver to get the TRACE level masks for the given name
	/// Use name == "ALL" to get ALL names
	/// </summary>
	/// <param name="name">TRACE name to get the mask for ("ALL" to get all names)</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_trace_get(std::string const& name);

	/// <summary>
	/// Using the transport mechanism, send an send_trace_msgfacility_set command
	///
	/// This will cause the receiver to set the given TRACE level mask for the given name to the given mask.
	/// Only the first character of the mask selection will be parsed, dial 'M' for Memory, or 'S' for Slow.
	/// Use name == "ALL" to set ALL names
	///
	/// EXAMPLE: xmlrpc http://localhost:5235/RPC2 daq.trace_set s/M s/ALL s/0x12345
	///
	/// </summary>
	/// <param name="name">TRACE name to set ("ALL" for all TRACE names)</param>
	/// <param name="type">Type of mask to set ('M', 'S', or 'T')</param>
	/// <param name="mask">64-bit mask, in string form</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_trace_set(std::string const& name, std::string const& type, std::string const& mask);

	/// <summary>
	/// Using the transport mechanism, send an send_meta_command command
	///
	/// This will cause the receiver to run the given command with the given argument in user code
	/// </summary>
	/// <param name="command">Command name to send</param>
	/// <param name="argument">Argument for command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_meta_command(std::string const& command, std::string const& argument);

	/// <summary>
	/// Using the transport mechanism, send a send_rollover_subrun command
	///
	/// This will cause the receiver to rollover the subrun number at the given event. (Event with seqID == boundary will be in new subrun.)
	/// Should be sent to all EventBuilders before the given event is processed.
	/// </summary>
	/// <param name="seq">Sequence ID of new subrun</param>
	/// <param name="subrunNumber">Subrun number of the new subrun</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string send_rollover_subrun(uint64_t seq, uint32_t subrunNumber);

	/// <summary>
	/// Determine whether the Commander plugin is ready to accept commands
	/// </summary>
	/// <returns>True if running, false otherwise</returns>
	bool GetStatus() { return running_.load(); }

	/// <summary>
	/// Using the transport mechanism, send an add_config_archive_entry command
	///
	/// This will cause the receiver to add the specified key and value to its list of
	/// configuration archive information, which is stored in the art/ROOT files that are
	/// written by various processes in the system.
	/// This command accepts configuration key and value strings.
	///
	/// EXAMPLE: xmlrpc http://localhost:5235/RPC2 daq.add_config_archive_entry "EventBuilder1" "daq: {verbose: true}"
	/// </summary>
	/// <param name="key">Key in the archive</param>
	/// <param name="value">Value to store in the archive</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string add_config_archive_entry(std::string const& key, std::string const& value);

	/// <summary>
	/// Using the transport mechanism, send a clear_config_archive command
	///
	/// This will cause the receiver to clear its list of configuration archive information.
	///
	/// EXAMPLE: xmlrpc http://localhost:5235/RPC2 daq.clear_config_archive
	/// </summary>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	virtual std::string clear_config_archive();

private:
	CommanderInterface(CommanderInterface&&) = delete;
	CommanderInterface& operator=(CommanderInterface&&) = delete;

public:
	/// <summary>
	/// Reference to the Commandable that this Commander Commands.
	/// </summary>
	artdaq::Commandable& _commandable;

protected:
	int _id;                     ///< ID Number of this Commander
	std::atomic<bool> running_;  ///< Whether the server is running and able to respond to requests
};
}  // namespace artdaq

#ifndef EXTERN_C_FUNC_DECLARE_START
#define EXTERN_C_FUNC_DECLARE_START extern "C" {
#endif

#define DEFINE_ARTDAQ_COMMANDER(klass)                                                  \
	EXTERN_C_FUNC_DECLARE_START                                                         \
	std::unique_ptr<artdaq::CommanderInterface> make(fhicl::ParameterSet const& ps,     \
	                                                 artdaq::Commandable& commandable)  \
	{                                                                                   \
		return std::unique_ptr<artdaq::CommanderInterface>(new klass(ps, commandable)); \
	}                                                                                   \
	}

#endif /* artdaq_ExternalComms_CommanderInterface.hh */

// Local Variables:
// mode: c++
// End:
