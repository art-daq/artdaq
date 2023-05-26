/* DarkSide 50 DAQ program
 * This file add the xmlrpc commander as a client to the SC
 * Author: Alessandro Razeto <Alessandro.Razeto@ge.infn.it>
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#define _LIBCPP_ENABLE_CXX17_REMOVED_FEATURES 1
#include <memory>
#include <xmlrpc-c/base.hpp>
#include <xmlrpc-c/client_simple.hpp>
#include <xmlrpc-c/girerr.hpp>
#include <xmlrpc-c/registry.hpp>
#include <xmlrpc-c/server_abyss.hpp>
#undef _LIBCPP_ENABLE_CXX17_REMOVED_FEATURES
#pragma GCC diagnostic pop

#include "TRACE/tracemf.h"
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_xmlrpc_commander").c_str()

#include "artdaq/ExternalComms/CommanderInterface.hh"

#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include <netinet/in.h>
#include <sys/socket.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace artdaq {

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
	xmlrpc_commander(const fhicl::ParameterSet& ps, artdaq::Commandable& commandable);

	/**
	 * \brief Run the XMLRPC server
	 */
	void run_server() override;

	/// <summary>
	/// Send a register_monitor command over XMLRPC
	/// </summary>
	/// <param name="monitor_fhicl">FHiCL string contianing monitor configuration</param>
	/// <returns>Return status from XMLRPC</returns>
	std::string send_register_monitor(std::string const& monitor_fhicl) override;

	/// <summary>
	/// Send an unregister_monitor command over XMLRPC
	/// </summary>
	/// <param name="monitor_label">Label of the monitor to unregister</param>
	/// <returns>Return status from XMLRPC</returns>
	std::string send_unregister_monitor(std::string const& monitor_label) override;

	/// <summary>
	/// Send an init command over XMLRPC
	///
	/// The init command is accepted by all artdaq processes that are in the booted state.
	/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
	/// </summary>
	/// <param name="ps">ParameterSet received with the init command</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_init(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a soft_init command over XMLRPC
	///
	/// The soft_init command is accepted by all artdaq processes that are in the booted state.
	/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
	/// </summary>
	/// <param name="ps">ParameterSet received with the soft_init command</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_soft_init(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a reinit command over XMLRPC
	///
	/// The reinit command is accepted by all artdaq processes.
	/// It expects a ParameterSet for configuration, a timeout, and a timestamp.
	/// </summary>
	/// <param name="ps">ParameterSet received with the reinit command</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_reinit(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a start command over XMLRPC
	///
	/// The start command starts a Run using the given run number.
	/// This command also accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="runNumber">Run number of the new run</param>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_start(art::RunID runNumber, uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a pause command over XMLRPC
	///
	/// The pause command pauses a Run. When the run resumes, the subrun number will be incremented.
	/// This command accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_pause(uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a resume command over XMLRPC
	///
	/// The resume command resumes a paused Run. When the run resumes, the subrun number will be incremented.
	/// This command accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_resume(uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a stop command over XMLRPC
	///
	/// The stop command stops the current Run.
	/// This command accepts a timeout parameter and a timestamp parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <param name="timestamp">Timestamp of the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_stop(uint64_t timeout, uint64_t timestamp) override;

	/// <summary>
	/// Send a shutdown command over XMLRPC
	///
	/// The shutdown command shuts down the artdaq process.
	/// This command accepts a timeout parameter.
	/// </summary>
	/// <param name="timeout">Timeout for the command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_shutdown(uint64_t timeout) override;

	/// <summary>
	/// Send a status command over XMLRPC
	///
	/// The status command returns the current status of the artdaq process.
	/// </summary>
	/// <returns>Command result: current status of the artdaq process</returns>
	std::string send_status() override;

	/// <summary>
	/// Send a report command over XMLRPC
	///
	/// The report command returns the current value of the requested reportable quantity.
	/// </summary>
	/// <param name="what">Reportable quantity to request</param>
	/// <returns>Command result: current value of the requested reportable quantity</returns>
	std::string send_report(std::string const& what) override;

	/// <summary>
	/// Send a legal_commands command over XMLRPC
	///
	/// This will query the artdaq process, and it will return the list of allowed transition commands from its current state.
	/// </summary>
	/// <returns>Command result: a list of allowed transition commands from its current state</returns>
	std::string send_legal_commands() override;

	/// <summary>
	/// Send an send_trace_get command over XMLRPC
	///
	/// This will cause the receiver to get the TRACE level masks for the given name
	/// Use name == "ALL" to get ALL names
	/// </summary>
	/// <param name="name">TRACE name to get the mask for ("ALL" to get all names)</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_trace_get(std::string const& name) override;

	/// <summary>
	/// Send an send_trace_msgfacility_set command over XMLRPC
	///
	/// This will cause the receiver to set the given TRACE level mask for the given name to the given mask.
	/// Only the first character of the mask selection will be parsed, dial 'M' for Memory, or 'S' for Slow.
	/// Use name == "ALL" to set ALL names
	///
	/// EXAMPLE: xmlrpc http://localhost:5235/RPC2 daq.trace_msgfacility_set TraceLock i8/$((0x1234)) # Use Bash to convert hex to dec
	/// </summary>
	/// <param name="name">TRACE name to set ("ALL" for all TRACE names)</param>
	/// <param name="type">Type of mask to set ('M', 'S', or 'T')</param>
	/// <param name="mask">64-bit mask, in string form</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_trace_set(std::string const& name, std::string const& type, std::string const& mask) override;

	/// <summary>
	/// Send an send_meta_command command over XMLRPC
	///
	/// This will cause the receiver to run the given command with the given argument in user code
	/// </summary>
	/// <param name="command">Command name to send</param>
	/// <param name="argument">Argument for command</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_meta_command(std::string const& command, std::string const& argument) override;

	/// <summary>
	/// Send a send_rollover_subrun command over XMLRPC
	///
	/// This will cause the receiver to rollover the subrun number at the given event. (Event with seqID == boundary will be in new subrun.)
	/// Should be sent to all EventBuilders before the given event is processed.
	/// </summary>
	/// <param name="when">Sequence ID of new subrun</param>
	/// <param name="sr">Subrun number of the new subrun</param>
	/// <returns>Command result: "SUCCESS" if succeeded</returns>
	std::string send_rollover_subrun(uint64_t when, uint32_t sr) override;

private:
	xmlrpc_commander(const xmlrpc_commander&) = delete;

	xmlrpc_commander(xmlrpc_commander&&) = delete;
	xmlrpc_commander& operator=(xmlrpc_commander const&) = delete;
	xmlrpc_commander& operator=(xmlrpc_commander&&) = delete;

	int port_;
	std::string serverUrl_;

	std::string send_command_(const std::string& command);
	std::string send_command_(const std::string& command, const std::string& arg);
	std::string send_command_(const std::string& command, const fhicl::ParameterSet& pset, uint64_t timestamp, uint64_t timeout);
	std::string send_command_(const std::string& command, uint64_t a, uint64_t b);
	std::string send_command_(const std::string& command, uint64_t a, uint32_t b);
	std::string send_command_(const std::string& command, art::RunID r, uint64_t a, uint64_t b);
	std::string send_command_(const std::string&, uint64_t);
	std::string send_command_(const std::string&, const std::string&, const std::string&);
	std::string send_command_(const std::string&, const std::string&, const std::string&, const std::string&);

public:
	std::timed_mutex mutex_;                        ///< XMLRPC mutex
	std::unique_ptr<xmlrpc_c::serverAbyss> server;  ///< XMLRPC server
};

}  // namespace artdaq

namespace {
/// <summary>
/// Wrapper for XMLRPC environment construction/destruction
/// </summary>
class env_wrap
{
public:
	env_wrap() { xmlrpc_env_init(&this->env_c); };
	~env_wrap() { xmlrpc_env_clean(&this->env_c); };
	xmlrpc_env env_c;  ///< XMLRPC Environment
private:
	env_wrap(env_wrap const&) = delete;
	env_wrap(env_wrap&&) = delete;
	env_wrap& operator=(env_wrap const&) = delete;
	env_wrap& operator=(env_wrap&&) = delete;
};
}  // namespace
static xmlrpc_c::paramList
pListFromXmlrpcArray(xmlrpc_value* const arrayP)
{
	env_wrap env;
	XMLRPC_ASSERT_ARRAY_OK(arrayP);
	unsigned int const arraySize = xmlrpc_array_size(&env.env_c, arrayP);
	assert(!env.env_c.fault_occurred);
	xmlrpc_c::paramList paramList(arraySize);
	for (unsigned int i = 0; i < arraySize; ++i)
	{
		xmlrpc_value* arrayItemP;
		xmlrpc_array_read_item(&env.env_c, arrayP, i, &arrayItemP);
		assert(!env.env_c.fault_occurred);
		paramList.add(xmlrpc_c::value(arrayItemP));
		xmlrpc_DECREF(arrayItemP);
	}
	return paramList;
}
static xmlrpc_value*
c_executeMethod(xmlrpc_env* const envP,
                xmlrpc_value* const paramArrayP,
                void* const methodPtr,
                void* const callInfoPtr)
{
	auto* const methodP(static_cast<xmlrpc_c::method*>(methodPtr));
	xmlrpc_c::paramList const paramList(pListFromXmlrpcArray(paramArrayP));
	auto* const callInfoP(static_cast<xmlrpc_c::callInfo*>(callInfoPtr));
	xmlrpc_value* retval;
	retval = nullptr;  // silence used-before-set warning
	try
	{
		xmlrpc_c::value result;
		try
		{
			auto* const method2P(dynamic_cast<xmlrpc_c::method2*>(methodP));
			if (method2P != nullptr)
			{
				method2P->execute(paramList, callInfoP, &result);
			}
			else
			{
				methodP->execute(paramList, &result);
			}
		}
		catch (xmlrpc_c::fault const& fault)
		{
			xmlrpc_env_set_fault(envP, fault.getCode(),
			                     fault.getDescription().c_str());
		}
		if (envP->fault_occurred == 0)
		{
			if (result.isInstantiated())
			{
				retval = result.cValue();
			}
			else
			{
				girerr::throwf(
				    "Xmlrpc-c user's xmlrpc_c::method object's "
				    "'execute method' failed to set the RPC result "
				    "value.");
			}
		}
	}
	catch (std::exception const& e)
	{
		xmlrpc_faultf(envP,
		              "Unexpected error executing code for "
		              "particular method, detected by Xmlrpc-c "
		              "method registry code.  Method did not "
		              "fail; rather, it did not complete at all.  %s",
		              e.what());
	}
	catch (...)
	{
		xmlrpc_env_set_fault(envP, XMLRPC_INTERNAL_ERROR,
		                     "Unexpected error executing code for "
		                     "particular method, detected by Xmlrpc-c "
		                     "method registry code.  Method did not "
		                     "fail; rather, it did not complete at all.");
	}
	return retval;
}

namespace artdaq {
/**
 * \brief Write an exception message
 * \param er A std::runtime_error to print
 * \param helpText Additional information about the exception context. Default: "execute request"
 * \return Exception message
 */
std::string exception_msg(const std::runtime_error& er,
                          const std::string& helpText = "execute request")
{
	std::string msg("Exception when trying to ");
	msg.append(helpText);
	msg.append(": ");
	msg.append(er.what());  // std::string(er.what ()).substr (2);
	if (msg[msg.size() - 1] == '\n')
	{
		msg.erase(msg.size() - 1);
	}
	return msg;
}

/**
 * \brief Write an exception message
 * \param er An art::Exception to print
 * \param helpText Additional information abou the exception context
 * \return Exception message
 */
std::string exception_msg(const art::Exception& er,
                          const std::string& helpText)
{
	std::string msg("Exception when trying to ");
	msg.append(helpText);
	msg.append(": ");
	msg.append(er.what());
	if (msg[msg.size() - 1] == '\n')
	{
		msg.erase(msg.size() - 1);
	}
	return msg;
}

/**
 * \brief Write an exception message
 * \param er A cet::exception to print
 * \param helpText Additional information abou the exception context
 * \return Exception message
 */
std::string exception_msg(const cet::exception& er,
                          const std::string& helpText)
{
	std::string msg("Exception when trying to ");
	msg.append(helpText);
	msg.append(": ");
	msg.append(er.what());
	if (msg[msg.size() - 1] == '\n')
	{
		msg.erase(msg.size() - 1);
	}
	return msg;
}

/**
 * \brief Write an exception message
 * \param er A boost::exception to print
 * \param helpText Additional information abou the exception context
 * \return Exception message
 */
std::string exception_msg(const boost::exception& er,
                          const std::string& helpText)
{
	std::string msg("Exception when trying to ");
	msg.append(helpText);
	msg.append(": ");
	msg.append(boost::diagnostic_information(er));
	if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
	return msg;
}
/**
 * \brief Write an exception message
 * \param er A std::exception to print
 * \param helpText Additional information abou the exception context
 * \return Exception message
 */
std::string exception_msg(const std::exception& er,
                          const std::string& helpText)
{
	std::string msg("Exception when trying to ");
	msg.append(helpText);
	msg.append(": ");
	msg.append(er.what());
	if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
	return msg;
}

/**
 * \brief Write an exception message
 * \param erText A std::string to print
 * \param helpText Additional information abou the exception context
 * \return Exception message
 */
std::string exception_msg(const std::string& erText,
                          const std::string& helpText)
{
	std::string msg("Exception when trying to ");
	msg.append(helpText);
	msg.append(": ");
	msg.append(erText);
	if (msg[msg.size() - 1] == '\n')
	{
		msg.erase(msg.size() - 1);
	}
	return msg;
}

/**
 * \brief The "cmd_" class serves as the base class for all artdaq's XML-RPC commands.
 *
 * JCF, 9/5/14
 *
 * The "cmd_" class serves as the base class for all artdaq's
 * XML-RPC commands, all of which use the code in the "execute()"
 * function; each specific command type deriving from cmd_ is
 * implemented in the execute_() function which execute() calls
 * (notice the underscore), and optionally sets the retvalP
 * parameter
 *
 * cmd_ contains a set of template functions, getParam<T>(), which
 * are designed to prevent implementors of derived classes from
 * having to worry about interfacing directly with xmlrpc_c's
 * parameter-getting functionality
 */
class cmd_ : public xmlrpc_c::method
{
public:
	// Can't seem to initialize "_signature" and "_help" in the initialization list...
	/**
	 * \brief cmd_ Constructor
	 * \param c xmlrpc_commander instance
	 * \param signature  Signature of the command
	 * \param description Description of the command
	 */
	cmd_(xmlrpc_commander& c, const std::string& signature, const std::string& description)
	    : _c(c)
	{
		_signature = signature;
		_help = description;
	}

	/**
	 * \brief Execute trhe command with the given parameters
	 * \param paramList List of parameters for the command (i.e. a fhicl string for init transitions)
	 * \param retvalP Pointer to the return value (usually a string describing result of command)
	 */
	void execute(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* retvalP) final;

protected:
	xmlrpc_commander& _c;  ///< The xmlrpc_commander instance that the command will be sent to

	/**
	 * \brief "execute_" is a wrapper function around the call to the commandable object's function
	 * \param retvalP Pointer to the return value (usually a string describing result of command)
	 * \return Whether the command succeeded
	 */
	virtual bool execute_(const xmlrpc_c::paramList&, xmlrpc_c::value* retvalP) = 0;

	/**
	 * \brief Get a parameter from the parameter list
	 * \tparam T Type of the parameter
	 * \param paramList The parameter list
	 * \param index Index of the parameter in the parameter list
	 * \return The requested parameter
	 *
	 * Template specilization is used to provide valid overloads
	 */
	template<typename T>
	T getParam(const xmlrpc_c::paramList& paramList, int index);

	/**
	 * \brief Get a parameter from the parameter list, returning a default value if not found at specified location
	 * \tparam T Type of the parameter
	 * \param paramList The parameter list
	 * \param index Index of the parameter in the parameter list
	 * \param default_value Default value to return if exception retrieving parameter
	 * \return The requested parameter, or the default value if there was an exception retrieving the parameter
	 *
	 * JCF, 9/5/14
	 *
	 * Here, if getParam throws an exception due to a lack of an
	 * existing parameter, swallow the exception and return the
	 * default value passed to the function
	 *
	 * Surprisingly, if an invalid index is supplied, although getParam
	 * throws an exception that exception is neither xmlrpc_c's
	 * girerr:error nor boost::bad_lexical_cast. Although it's less than
	 * ideal, we'll swallow almost all exceptions in the call to
	 * getParam, as an invalid index value simply means the user wishes
	 * to employ the default_value. I say "almost" because the only
	 * exception we don't swallow here is if an invalid parameter type
	 * "T" was supplied
	 */
	template<typename T>
	T getParam(const xmlrpc_c::paramList& paramList, int index, T default_value);
};

// Users are only allowed to call getParam for predefined types; see
// template specializations below this default function

template<typename T>
T cmd_::getParam(const xmlrpc_c::paramList& /*unused*/, int /*unused*/)
{
	throw cet::exception("cmd_") << "Error in cmd_::getParam(): value type not supported" << std::endl;  // NOLINT(cert-err60-cpp)
}

/**
 * \brief Get a parameter from the parameter list
 * \param paramList The parameter list
 * \param index Index of the parameter in the parameter list
 * \return The requested parameter
 *
 * This specialized cmd_getParam for the uint64_t type
 */
template<>
uint64_t cmd_::getParam<uint64_t>(const xmlrpc_c::paramList& paramList, int index)
{
	TLOG(TLVL_DEBUG + 33) << "Getting parameter " << index << " from list as uint64_t.";
	try
	{
		TLOG(TLVL_DEBUG + 33) << "Param value: " << paramList.getI8(index);
		return static_cast<uint64_t>(paramList.getI8(index));
	}
	catch (...)
	{
		TLOG(TLVL_DEBUG + 33) << "Param value (int): " << paramList.getInt(index);
		return static_cast<uint64_t>(paramList.getInt(index));
	}
}

/**
 * \brief Get a parameter from the parameter list
 * \param paramList The parameter list
 * \param index Index of the parameter in the parameter list
 * \return The requested parameter
 *
 * This specialized cmd_getParam for the uint64_t type
 */
template<>
uint32_t cmd_::getParam<uint32_t>(const xmlrpc_c::paramList& paramList, int index)
{
	TLOG(TLVL_DEBUG + 33) << "Getting parameter " << index << " from list as uint32_t.";
	TLOG(TLVL_DEBUG + 33) << "Param value: " << paramList.getInt(index);
	return static_cast<uint32_t>(paramList.getInt(index));
}

/**
 * \brief Get a parameter from the parameter list
 * \param paramList The parameter list
 * \param index Index of the parameter in the parameter list
 * \return The requested parameter
 *
 * This specialized cmd_getParam for the std::string type
 */
template<>
std::string cmd_::getParam<std::string>(const xmlrpc_c::paramList& paramList, int index)
{
	TLOG(TLVL_DEBUG + 33) << "Getting parameter " << index << " from list as string.";
	TLOG(TLVL_DEBUG + 33) << "Param value: " << paramList.getString(index);
	return static_cast<std::string>(paramList.getString(index));
}

/**
 * \brief Get a parameter from the parameter list
 * \param paramList The parameter list
 * \param index Index of the parameter in the parameter list
 * \return The requested parameter
 *
 * This specialized cmd_getParam for the art::RunID type
 */
template<>
art::RunID cmd_::getParam<art::RunID>(const xmlrpc_c::paramList& paramList, int index)
{
	TLOG(TLVL_DEBUG + 33) << "Getting parameter " << index << " from list as Run Number.";
	art::RunNumber_t run_number;
	try
	{
		TLOG(TLVL_DEBUG + 33) << "Param value: " << paramList.getInt(index);
		run_number = art::RunNumber_t(paramList.getInt(index));
	}
	catch (...)
	{
		TLOG(TLVL_DEBUG + 33) << "Parameter is not an int. Trying string...";

		auto runNumber = paramList.getString(index);
		TLOG(TLVL_DEBUG + 33) << "Got run number string " << runNumber;
		run_number = art::RunNumber_t(std::stoi(runNumber));
	}

	art::RunID run_id(run_number);
	return run_id;
}

/**
 * \brief Get a parameter from the parameter list
 * \param paramList The parameter list
 * \param index Index of the parameter in the parameter list
 * \return The requested parameter
 *
 * This specialized cmd_getParam for the fhicl::ParameterSet type
 */
template<>
fhicl::ParameterSet cmd_::getParam<fhicl::ParameterSet>(const xmlrpc_c::paramList& paramList, int index)
{
	TLOG(TLVL_DEBUG + 33) << "Getting parameter " << index << " from list as ParameterSet.";
	TLOG(TLVL_DEBUG + 33) << "Param value: " << paramList.getString(index);
	std::string configString = std::string(paramList.getString(index));
	TLOG(TLVL_DEBUG + 32) << "Loading Parameter Set from string: " << configString << std::endl;
	fhicl::ParameterSet pset;

	try
	{
		pset = fhicl::ParameterSet::make(configString);
	}
	catch (const fhicl::exception& e)
	{
		if (getenv("FHICL_FILE_PATH") == nullptr)
		{
			TLOG(TLVL_ERROR) << "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"";
			std::cerr << "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"" << std::endl;
			setenv("FHICL_FILE_PATH", ".", 0);
		}
		cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
		pset = fhicl::ParameterSet::make(configString, lookup_policy);
	}

	TLOG(TLVL_INFO) << "Parameter Set Loaded." << std::endl;
	return pset;
}

template<typename T>
T cmd_::getParam(const xmlrpc_c::paramList& paramList, int index,
                 T default_value)
{
	T val = default_value;

	if (static_cast<unsigned>(index) < paramList.size())
	{
		val = getParam<T>(paramList, index);
	}
	return val;
}

void cmd_::execute(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const retvalP)
{
	TLOG(TLVL_DEBUG + 33) << "Received Request to " << _help << ", attempting to get lock";
	std::unique_lock<std::timed_mutex> lk(_c.mutex_, std::defer_lock);
	auto ret = lk.try_lock_for(std::chrono::milliseconds(250));

	if (ret && lk.owns_lock())
	{
		try
		{
			// JCF, 9/4/14

			// Assuming the execute_ function returns true, then if the
			// retvalP argument was untouched, assign it the string
			// "Success"

			// See
			// http://xmlrpc-c.sourceforge.net/doc/libxmlrpc++.html#isinstantiated
			// for more on the concept of instantiation in xmlrpc_c::value objects

			if (execute_(paramList, retvalP))
			{
				if (!retvalP->isInstantiated())
				{
					*retvalP = xmlrpc_c::value_string("Success");
				}
			}
			else
			{
				std::string problemReport = _c._commandable.report("transition_status");
				*retvalP = xmlrpc_c::value_string(problemReport);
			}
		}
		catch (std::runtime_error& er)
		{
			std::string msg = exception_msg(er, _help);
			*retvalP = xmlrpc_c::value_string(msg);
			TLOG(TLVL_ERROR) << msg;
		}
		catch (art::Exception& er)
		{
			std::string msg = exception_msg(er, _help);
			*retvalP = xmlrpc_c::value_string(msg);
			TLOG(TLVL_ERROR) << msg;
		}
		catch (cet::exception& er)
		{
			std::string msg = exception_msg(er, _help);
			*retvalP = xmlrpc_c::value_string(msg);
			TLOG(TLVL_ERROR) << msg;
		}
		catch (boost::exception& er)
		{
			std::string msg = exception_msg(er, _help);
			*retvalP = xmlrpc_c::value_string(msg);
			TLOG(TLVL_ERROR) << msg;
		}
		catch (std::exception& er)
		{
			std::string msg = exception_msg(er, _help);
			*retvalP = xmlrpc_c::value_string(msg);
			TLOG(TLVL_ERROR) << msg;
		}
		catch (...)
		{
			std::string msg = exception_msg("Unknown exception (not from std, boost, cet, or art)", _help);
			*retvalP = xmlrpc_c::value_string(msg);
			TLOG(TLVL_ERROR) << msg;
		}

		lk.unlock();
	}
	else
	{
		TLOG(TLVL_ERROR) << "Unable to get lock while trying to " << _help << ", returning busy";
		*retvalP = xmlrpc_c::value_string("busy");
	}
}

//////////////////////////////////////////////////////////////////////

// JCF, 9/5/14

// The three "init" transitions all take a FHiCL parameter list, and
// optionally a timeout and a timestamp; thus we can kill three birds
// with one stone in the GENERATE_INIT_TRANSITION macro

#define GENERATE_INIT_TRANSITION(NAME, CALL, DESCRIPTION)                                                                                                                 \
	/** Command class representing an init transition */                                                                                                                  \
	class NAME##_ : public cmd_                                                                                                                                           \
	{                                                                                                                                                                     \
	public:                                                                                                                                                               \
		/** Command class Constructor                                                                                                                                     \
		 * \param c xmlrpc_commander to send parsed command to                                                                                                            \
		 */                                                                                                                                                               \
		explicit NAME##_(xmlrpc_commander& c) : cmd_(c, "s:sII", DESCRIPTION) {}                                                                                          \
                                                                                                                                                                          \
		/** Default timeout for command */                                                                                                                                \
		static const uint64_t defaultTimeout = 45;                                                                                                                        \
		/** Default timestamp for Command */                                                                                                                              \
		static const uint64_t defaultTimestamp = std::numeric_limits<const uint64_t>::max();                                                                              \
                                                                                                                                                                          \
	private:                                                                                                                                                              \
		bool execute_(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const retvalP)                                                                               \
		{                                                                                                                                                                 \
			fhicl::ParameterSet ps;                                                                                                                                       \
			try                                                                                                                                                           \
			{                                                                                                                                                             \
				ps = getParam<fhicl::ParameterSet>(paramList, 0);                                                                                                         \
			}                                                                                                                                                             \
			catch (...)                                                                                                                                                   \
			{                                                                                                                                                             \
				*retvalP = xmlrpc_c::value_string("Error: The " #NAME " message requires a single argument that is a string containing the initialization ParameterSet"); \
				return true;                                                                                                                                              \
			}                                                                                                                                                             \
                                                                                                                                                                          \
			return _c._commandable.CALL(ps,                                                                                                                               \
			                            getParam<uint64_t>(paramList, 1, defaultTimeout),                                                                                 \
			                            getParam<uint64_t>(paramList, 2, defaultTimestamp));                                                                              \
		}                                                                                                                                                                 \
	};

GENERATE_INIT_TRANSITION(init, initialize, "initialize the program")

GENERATE_INIT_TRANSITION(soft_init, soft_initialize, "initialize software components in the program")

GENERATE_INIT_TRANSITION(reinit, reinitialize, "re-initialize the program")

#undef GENERATE_INIT_TRANSITION

//////////////////////////////////////////////////////////////////////

/**
 * \brief Command class representing a start transition
 */
class start_ : public cmd_
{
public:
	/**
	 * \brief start_ Command (cmd_ derived class) Constructor
	 * \param c xmlrpc_commander instance to command
	 */
	explicit start_(xmlrpc_commander& c)
	    : cmd_(c, "s:iII", "start the run")
	{}

	/** Default timeout for command */
	static const uint64_t defaultTimeout = 45;
	/** Default timestamp for Command */
	static const uint64_t defaultTimestamp = std::numeric_limits<const uint64_t>::max();

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<art::RunID>(paramList, 0);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The start message requires the run number as an argument.");
			return true;
		}

		return _c._commandable.start(getParam<art::RunID>(paramList, 0),
		                             getParam<uint64_t>(paramList, 1, defaultTimeout),
		                             getParam<uint64_t>(paramList, 2, defaultTimestamp));
	}
};

//////////////////////////////////////////////////////////////////////

// JCF, 9/5/14

// "pause", "resume" and "stop" all take an optional timeout and
// timestamp parameter, so we can generate them all with the
// GENERATE_TIMEOUT_TIMESTAMP_TRANSITION macro

#define GENERATE_TIMEOUT_TIMESTAMP_TRANSITION(NAME, CALL, DESCRIPTION, TIMEOUT)              \
	/** NAME ## _ Command class */                                                           \
	class NAME##_ : public cmd_                                                              \
	{                                                                                        \
	public:                                                                                  \
		/** NAME ## _ Constructor                                                            \
		    \param c xmlrpc_commander to send transition commands to */                      \
		NAME##_(xmlrpc_commander& c) : cmd_(c, "s:II", DESCRIPTION) {}                       \
                                                                                             \
		/** Default timeout for command */                                                   \
		static const uint64_t defaultTimeout = TIMEOUT;                                      \
		/** Default timestamp for Command */                                                 \
		static const uint64_t defaultTimestamp = std::numeric_limits<const uint64_t>::max(); \
                                                                                             \
	private:                                                                                 \
		bool execute_(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const)          \
		{                                                                                    \
			return _c._commandable.CALL(getParam<uint64_t>(paramList, 0, defaultTimeout),    \
			                            getParam<uint64_t>(paramList, 1, defaultTimestamp)); \
		}                                                                                    \
	};

GENERATE_TIMEOUT_TIMESTAMP_TRANSITION(pause, pause, "pause the program", 45)

GENERATE_TIMEOUT_TIMESTAMP_TRANSITION(resume, resume, "resume the program", 45)

GENERATE_TIMEOUT_TIMESTAMP_TRANSITION(stop, stop, "stop the program", 45)

#undef GENERATE_TIMEOUT_TIMESTAMP_TRANSITION

/**
 * \brief shutdown_ Command class
 */
class shutdown_ : public cmd_
{
public:
	/**
	 * \brief shutdown_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	shutdown_(xmlrpc_commander& c)
	    : cmd_(c, "s:i", "shutdown the program")
	{}

	/** Default timeout for command */
	static const uint64_t defaultTimeout = 45;

private:
	bool execute_(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const /*retvalP*/) override
	{
		auto ret = _c._commandable.shutdown(getParam<uint64_t>(paramList, 0, defaultTimeout));

#if 1
		if (_c.server)
		{
			_c.server->terminate();
		}
#endif

		return ret;
	}
};

/**
 * \brief status_ Command class
 */
class status_ : public cmd_
{
public:
	/**
	 * \brief status_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	status_(xmlrpc_commander& c)
	    : cmd_(c, "s:n", "report the current state")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& /*unused*/, xmlrpc_c::value* const retvalP) override
	{
		*retvalP = xmlrpc_c::value_string(_c._commandable.status());
		return true;
	}
};

/**
 * \brief report_ Command class
 */
class report_ : public cmd_
{
public:
	/**
	 * \brief report_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	report_(xmlrpc_commander& c)
	    : cmd_(c, "s:s", "report statistics")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<std::string>(paramList, 0);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The report message requires a single argument that selects the type of statistics to be reported.");
			return true;
		}

		*retvalP = xmlrpc_c::value_string(_c._commandable.report(getParam<std::string>(paramList, 0)));
		return true;
	}
};

/**
 * \brief legal_commands_ Command class
 */
class legal_commands_ : public cmd_
{
public:
	/**
	 * \brief legal_commands_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	legal_commands_(xmlrpc_commander& c)
	    : cmd_(c, "s:n", "return the currently legal commands")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& /*unused*/, xmlrpc_c::value* const retvalP) override
	{
		std::vector<std::string> cmdList = _c._commandable.legal_commands();
		std::string resultString;

		for (auto& cmd : cmdList)
		{
			resultString.append(cmd + " ");
			if (cmd == "shutdown")
			{
				resultString.append(" reset");
			}
		}
		*retvalP = xmlrpc_c::value_string(resultString);

		return true;
	}
};

/**
 * \brief register_monitor_ Command class
 */
class register_monitor_ : public cmd_
{
public:
	/**
	 * \brief register_monitor_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	register_monitor_(xmlrpc_commander& c)
	    : cmd_(c, "s:s", "Get notified of a new monitor")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<fhicl::ParameterSet>(paramList, 0);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The register_monitor command expects a string representing the FHiCL definition of a Transfer plugin");
			return true;
		}

		*retvalP = xmlrpc_c::value_string(_c._commandable.register_monitor(getParam<fhicl::ParameterSet>(paramList, 0)));
		return true;
	}
};

/**
 * \brief unregister_monitor_ Command class
 */
class unregister_monitor_ : public cmd_
{
public:
	/**
	 * \brief unregister_monitor_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	unregister_monitor_(xmlrpc_commander& c)
	    : cmd_(c, "s:s", "Remove a monitor")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<std::string>(paramList, 0);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The unregister_monitor command expects a string representing the label of the monitor to be removed");
			return true;
		}

		*retvalP = xmlrpc_c::value_string(_c._commandable.unregister_monitor(getParam<std::string>(paramList, 0)));
		return true;
	}
};

/**
 * \brief trace_set_ Command class
 */
class trace_set_ : public cmd_
{
public:
	/**
	 * \brief unregister_monitor_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	trace_set_(xmlrpc_commander& c)
	    : cmd_(c, "s:ssI", "Set TRACE mask")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<std::string>(paramList, 0);
			getParam<std::string>(paramList, 1);
			getParam<std::string>(paramList, 2);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The trace_set command expects a name (ALL for all), a mask type (M, S , or T), and a mask");
			return true;
		}

		return _c._commandable.do_trace_set(getParam<std::string>(paramList, 0), getParam<std::string>(paramList, 1), getParam<std::string>(paramList, 2));
	}
};

/**
 * \brief trace_get_ Command class
 */
class trace_get_ : public cmd_
{
public:
	/**
	 * \brief trace_msgfacility_set_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	trace_get_(xmlrpc_commander& c)
	    : cmd_(c, "s:s", "Get TRACE mask")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<std::string>(paramList, 0);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The trace_get command expects a name (ALL for all)");
			return true;
		}

		*retvalP = xmlrpc_c::value_string(_c._commandable.do_trace_get(getParam<std::string>(paramList, 0)));
		return true;
	}
};

/**
 * \brief meta_command_ Command class
 */
class meta_command_ : public cmd_
{
public:
	/**
	 * \brief meta_command_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	meta_command_(xmlrpc_commander& c)
	    : cmd_(c, "s:ss", "Run custom command")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<std::string>(paramList, 0);
			getParam<std::string>(paramList, 1);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The meta_command command expects a string command and a string argument");
			return true;
		}

		return _c._commandable.do_meta_command(getParam<std::string>(paramList, 0), getParam<std::string>(paramList, 1));
	}
};

/**
 * \brief rollover_subrun_ Command class
 */
class rollover_subrun_ : public cmd_
{
public:
	/**
	 * \brief shutdown_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	rollover_subrun_(xmlrpc_commander& c)
	    : cmd_(c, "s:Ii", "create a new subrun")
	{}

	static const uint64_t defaultSequenceID = 0xFFFFFFFFFFFFFFFF;  ///< Default Sequence ID for command
	static const uint32_t defaultSubrunNumber = 1;                 ///< Default subrun number for command

private:
	bool execute_(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const /*retvalP*/) override
	{
		auto ret = _c._commandable.do_rollover_subrun(getParam<uint64_t>(paramList, 0, defaultSequenceID), getParam<uint32_t>(paramList, 1, defaultSubrunNumber));
		return ret;
	}
};

/**
 * \brief add_config_archive_entry_ Command class
 */
class add_config_archive_entry_ : public cmd_
{
public:
	/**
	 * \brief add_config_archive_entry_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	add_config_archive_entry_(xmlrpc_commander& c)
	    : cmd_(c, "s:ss", "Add an entry to the configuration archive list")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP) override
	{
		try
		{
			getParam<std::string>(paramList, 0);
			getParam<std::string>(paramList, 1);
		}
		catch (...)
		{
			*retvalP = xmlrpc_c::value_string("Error: The add_config_archive_entry command expects a string key and a string value");
			return true;
		}

		return _c._commandable.do_add_config_archive_entry(getParam<std::string>(paramList, 0), getParam<std::string>(paramList, 1));
	}
};

/**
 * \brief clear_config_archive_ Command class
 */
class clear_config_archive_ : public cmd_
{
public:
	/**
	 * \brief clear_config_archive_ Constructor
	 * \param c xmlrpc_commander to send transition commands to
	 */
	clear_config_archive_(xmlrpc_commander& c)
	    : cmd_(c, "s:n", "Clear the configuration archive list")
	{}

private:
	bool execute_(xmlrpc_c::paramList const& /*unused*/, xmlrpc_c::value* const /*retvalP*/) override
	{
		return _c._commandable.do_clear_config_archive();
	}
};

// JCF, 9/4/14

// Not sure if anyone was planning to resurrect this code by changing
// the preprocessor decision; as such, I'll leave it in for now...

#if 0
	class shutdown_ : public xmlrpc_c::registry::shutdown
	{
	public:
		shutdown_(xmlrpc_c::serverAbyss *server) : _server(server) {}

		virtual void doit(const std::string& paramString, void*) const
		{
			TLOG(TLVL_INFO) << "A shutdown command was sent "
				<< "with parameter "
				<< paramString << "\"";
			_server->terminate();
		}
	private:
		xmlrpc_c::serverAbyss *_server;
	};
#endif

xmlrpc_commander::xmlrpc_commander(const fhicl::ParameterSet& ps, artdaq::Commandable& commandable)
    : CommanderInterface(ps, commandable)
    , port_(ps.get<int>("id", 0))
    , serverUrl_(ps.get<std::string>("server_url", ""))
    , server(nullptr)
{
	if (serverUrl_.empty())
	{
		char hostname[HOST_NAME_MAX];
		gethostname(hostname, HOST_NAME_MAX);
		serverUrl_ = std::string(hostname);
	}
	if (serverUrl_.find("http") == std::string::npos)
	{
		serverUrl_ = "http://" + serverUrl_;
	}
	if (serverUrl_.find(std::to_string(port_)) == std::string::npos && serverUrl_.find(':', 7) == std::string::npos)
	{
		serverUrl_ = serverUrl_ + ":" + std::to_string(port_);
	}
	if (serverUrl_.find("RPC2") == std::string::npos)
	{
		serverUrl_ = serverUrl_ + "/RPC2";
	}
	TLOG(TLVL_INFO) << "XMLRPC COMMANDER CONSTRUCTOR: Port: " << port_ << ", Server Url: " << serverUrl_;
}

void xmlrpc_commander::run_server()
try
{
	// std::cout << "XMLRPC_COMMANDER RUN_SERVER CALLED!" << std::endl;
	xmlrpc_c::registry registry;
	struct xmlrpc_method_info3 methodInfo;
	memset(&methodInfo, 0, sizeof(methodInfo));

	/*#define register_method(m) \
	    //  xmlrpc_c::methodPtr const ptr_ ## m(new m ## _(*this));\
	       registry.addMethod ("daq." #m, ptr_ ## m) */
#define register_method(m) register_method2(m, 0x400000)

	xmlrpc_env env;  // xmlrpc_env_init(&env);
	xmlrpc_registry*** c_registryPPP;
	c_registryPPP = reinterpret_cast<xmlrpc_registry***>(reinterpret_cast<char*>(&registry) + sizeof(girmem::autoObject));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)

#define register_method2(m, ss)                                                  \
	xmlrpc_c::method* ptr_##m(dynamic_cast<xmlrpc_c::method*>(new m##_(*this))); \
	std::string m##signature = ptr_##m->signature(), m##help = ptr_##m->help();  \
	methodInfo.methodName = "daq." #m;                                           \
	methodInfo.methodFunction = &c_executeMethod;                                \
	methodInfo.serverInfo = ptr_##m;                                             \
	methodInfo.stackSize = ss;                                                   \
	methodInfo.signatureString = &m##signature[0];                               \
	methodInfo.help = &m##help[0];                                               \
	xmlrpc_env_init(&env);                                                       \
	xmlrpc_registry_add_method3(&env, **c_registryPPP, &methodInfo);             \
	if (env.fault_occurred) throw(girerr::error(env.fault_string));              \
	xmlrpc_env_clean(&env)

#define unregister_method(m) delete ptr_##m;

	register_method2(init, 0x200000);
	register_method(soft_init);
	register_method(reinit);
	register_method(start);
	register_method(status);
	register_method(report);
	register_method(stop);
	register_method(pause);
	register_method(resume);
	register_method(register_monitor);
	register_method(unregister_monitor);
	register_method(legal_commands);
	register_method(trace_set);
	register_method(trace_get);
	register_method(meta_command);
	register_method(rollover_subrun);
	register_method(add_config_archive_entry);
	register_method(clear_config_archive);

	register_method(shutdown);

	// alias "daq.reset" to the internal shutdown transition
	xmlrpc_c::methodPtr const ptr_reset(new shutdown_(*this));
	registry.addMethod("daq.reset", ptr_reset);

#undef register_method

	// JCF, 6/3/15

	// In the following code, I configure a socket to have the
	// SO_REUSEADDR option so that once an artdaq process closes, the
	// port it was communicating on becomes immediately available
	// (desirable if, say, the DAQ program is terminated and then
	// immediately restarted)

	// Much of the following code is cribbed from
	// http://fossies.org/linux/freeswitch/libs/xmlrpc-c/src/cpp/test/server_abyss.cpp

	// Below, "0" is the default protocol (in this case, given the IPv4
	// Protocol Family (PF_INET) and the SOCK_STREAM communication
	// method)

	XMLRPC_SOCKET socket_file_descriptor = socket(PF_INET, SOCK_STREAM, 0);

	if (socket_file_descriptor < 0)
	{
		throw cet::exception("xmlrpc_commander::run") << "Problem with the socket() call; C-style errno == " << errno << " (" << strerror(errno) << ")";  // NOLINT(cert-err60-cpp)
	}

	int enable = 1;
	int retval = setsockopt(socket_file_descriptor,
	                        SOL_SOCKET, SO_REUSEADDR,
	                        &enable, sizeof(int));

	if (retval < 0)
	{
		throw cet::exception("xmlrpc_commander::run") << "Problem with the call to setsockopt(); C-style errno == " << errno << " (" << strerror(errno) << ")";  // NOLINT(cert-err60-cpp)
	}

	struct sockaddr_in sockAddr;

	sockAddr.sin_family = AF_INET;
	sockAddr.sin_port = htons(port_);
	sockAddr.sin_addr.s_addr = 0;

	retval = bind(socket_file_descriptor,
	              reinterpret_cast<struct sockaddr*>(&sockAddr),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	              sizeof(sockAddr));

	if (retval != 0)
	{
		close(socket_file_descriptor);
		throw cet::exception("xmlrpc_commander::run") << "Problem with the bind() call; C-style errno == " << errno << " (" << strerror(errno) << ")";  // NOLINT(cert-err60-cpp)
	}

	server = std::make_unique<xmlrpc_c::serverAbyss>(xmlrpc_c::serverAbyss::constrOpt().registryP(&registry).socketFd(socket_file_descriptor));

#if 0
		xmlrpc_c::serverAbyss::shutdown shutdown_obj(&server);
		registry.setShutdown(&shutdown_obj);
#endif

	TLOG(TLVL_DEBUG + 32) << "running server";

	// JCF, 6/3/15

	// Use a catch block to clean up (i.e., close the socket). An
	// opportunity for RAII, although all control paths are limited to
	// this section of the file...

	try
	{
		running_ = true;
		server->run();
		running_ = false;
	}
	catch (...)
	{
		TLOG(TLVL_WARNING) << "server threw an exception; closing the socket and rethrowing";
		running_ = false;
		close(socket_file_descriptor);
		throw;
	}

	close(socket_file_descriptor);

	unregister_method(init);
	unregister_method(soft_init);
	unregister_method(reinit);
	unregister_method(start);
	unregister_method(status);
	unregister_method(report);
	unregister_method(stop);
	unregister_method(pause);
	unregister_method(resume);
	unregister_method(register_monitor);
	unregister_method(unregister_monitor);
	unregister_method(legal_commands);
	unregister_method(trace_set);
	unregister_method(trace_get);
	unregister_method(meta_command);
	unregister_method(rollover_subrun);
	unregister_method(add_config_archive_entry);
	unregister_method(clear_config_archive);

	unregister_method(shutdown);

	TLOG(TLVL_DEBUG + 32) << "server terminated";
}
catch (...)
{
	throw;
}

std::string xmlrpc_commander::send_command_(const std::string& command)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "", &result);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string xmlrpc_commander::send_command_(const std::string& command, const std::string& arg)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "s", &result, arg.c_str());
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string xmlrpc_commander::send_command_(const std::string& command, const fhicl::ParameterSet& pset, uint64_t timestamp, uint64_t timeout)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "sII", &result, pset.to_string().c_str(), timestamp, timeout);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string artdaq::xmlrpc_commander::send_command_(const std::string& command, uint64_t a, uint64_t b)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "II", &result, a, b);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string artdaq::xmlrpc_commander::send_command_(const std::string& command, uint64_t a, uint32_t b)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "Ii", &result, a, b);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string artdaq::xmlrpc_commander::send_command_(const std::string& command, art::RunID r, uint64_t a, uint64_t b)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "iII", &result, r.run(), a, b);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string artdaq::xmlrpc_commander::send_command_(const std::string& command, uint64_t arg1)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "I", &result, arg1);
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string artdaq::xmlrpc_commander::send_command_(const std::string& command, const std::string& arg1, const std::string& arg2)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "ss", &result, arg1.c_str(), arg2.c_str());
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string artdaq::xmlrpc_commander::send_command_(const std::string& command, const std::string& arg1, const std::string& arg2, const std::string& arg3)
{
	if (serverUrl_.empty())
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call: No server URL set!";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}
	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq." + command, "sss", &result, arg1.c_str(), arg2.c_str(), arg3.c_str());
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting " << command << " XML-RPC call on host " << serverUrl_
		       << "; possible causes are bad arguments or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str());
	}

	return xmlrpc_c::value_string(result);
}

std::string xmlrpc_commander::send_register_monitor(std::string const& monitor_fhicl)
{
	return send_command_("register_monitor", monitor_fhicl);
}
std::string xmlrpc_commander::send_unregister_monitor(std::string const& monitor_label)
{
	return send_command_("unregister_monitor", monitor_label);
}
std::string artdaq::xmlrpc_commander::send_init(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp)
{
	return send_command_("init", ps, timeout, timestamp);
}
std::string artdaq::xmlrpc_commander::send_soft_init(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp)
{
	return send_command_("soft_init", ps, timeout, timestamp);
}
std::string xmlrpc_commander::send_reinit(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp)
{
	return send_command_("reinit", ps, timeout, timestamp);
}
std::string xmlrpc_commander::send_start(art::RunID run, uint64_t timeout, uint64_t timestamp)
{
	return send_command_("start", run, timeout, timestamp);
}
std::string xmlrpc_commander::send_pause(uint64_t timeout, uint64_t timestamp)
{
	return send_command_("pause", timeout, timestamp);
}
std::string xmlrpc_commander::send_resume(uint64_t timeout, uint64_t timestamp)
{
	return send_command_("resume", timeout, timestamp);
}
std::string xmlrpc_commander::send_stop(uint64_t timeout, uint64_t timestamp)
{
	return send_command_("stop", timeout, timestamp);
}
std::string xmlrpc_commander::send_shutdown(uint64_t timeout)
{
	return send_command_("shutdown", timeout);
}
std::string xmlrpc_commander::send_status()
{
	return send_command_("status");
}
std::string xmlrpc_commander::send_report(std::string const& what)
{
	return send_command_("report", what);
}
std::string xmlrpc_commander::send_legal_commands()
{
	return send_command_("legal_commands");
}
std::string xmlrpc_commander::send_trace_get(std::string const& name)
{
	return send_command_("trace_get", name);
}
std::string xmlrpc_commander::send_trace_set(std::string const& name, std::string const& type, std::string const& mask)
{
	return send_command_("trace_set", name, type, mask);
}
std::string xmlrpc_commander::send_meta_command(std::string const& command, std::string const& arg)
{
	return send_command_("meta_command", command, arg);
}
std::string xmlrpc_commander::send_rollover_subrun(uint64_t when, uint32_t sr)
{
	return send_command_("rollover_subrun", when, sr);
}
}  // namespace artdaq

DEFINE_ARTDAQ_COMMANDER(artdaq::xmlrpc_commander)
