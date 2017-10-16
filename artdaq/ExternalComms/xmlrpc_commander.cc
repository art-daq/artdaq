/* DarkSide 50 DAQ program
 * This file add the xmlrpc commander as a client to the SC
 * Author: Alessandro Razeto <Alessandro.Razeto@ge.infn.it>
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <xmlrpc-c/base.hpp>
#include <xmlrpc-c/registry.hpp>
#include <xmlrpc-c/server_abyss.hpp>
#include <xmlrpc-c/girerr.hpp>
#include <xmlrpc-c/client_simple.hpp>
#pragma GCC diagnostic pop
#include <stdexcept>
#include <iostream>
#include <limits>
#include <memory>
#include <cstdint>

#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <cstring>

#include "canvas/Persistency/Provenance/RunID.h"

#include "artdaq/ExternalComms/xmlrpc_commander.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/LoadParameterSet.hh"

namespace artdaq
{
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
		msg.append(er.what()); //std::string(er.what ()).substr (2);
		if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
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
		if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
		return msg;
	}

	/**
	* \brief Write an exception message
	* \param er A cet::exceptio to print
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
		if (msg[msg.size() - 1] == '\n') msg.erase(msg.size() - 1);
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
		cmd_(xmlrpc_commander& c, const std::string& signature, const std::string& description) : _c(c)
		{
			_signature = signature;
			_help = description;
		}

		/**
		 * \brief Execute trhe command with the given parameters
		 * \param paramList List of parameters for the command (i.e. a fhicl string for init transitions)
		 * \param retvalP Pointer to the return value (usually a string describing result of command)
		 */
		void execute(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const retvalP) final;

	protected:

		xmlrpc_commander& _c; ///< The xmlrpc_commander instance that the command will be sent to

		/**
		 * \brief "execute_" is a wrapper function around the call to the commandable object's function
		 * \param retvalP Pointer to the return value (usually a string describing result of command)
		 * \return Whether the command succeeded
		 */
		virtual bool execute_(const xmlrpc_c::paramList&, xmlrpc_c::value* const retvalP) = 0;

		/**
		 * \brief Get a parameter from the parameter list
		 * \tparam T Type of the parameter
		 * \param paramList The parameter list
		 * \param index Index of the parameter in the parameter list
		 * \return The requested parameter
		 *
		 * Template specilization is used to provide valid overloads
		 */
		template <typename T>
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
		template <typename T>
		T getParam(const xmlrpc_c::paramList& paramList, int index, T default_value);
	};

	// Users are only allowed to call getParam for predefined types; see
	// template specializations below this default function

	template <typename T>
	T cmd_::getParam(const xmlrpc_c::paramList&, int)
	{
		throw cet::exception("cmd_") << "Error in cmd_::getParam(): value type not supported" << std::endl;
	}

	/**
	* \brief Get a parameter from the parameter list
	* \param paramList The parameter list
	* \param index Index of the parameter in the parameter list
	* \return The requested parameter
	*
	* This specialized cmd_getParam for the uint64_t type
	*/
	template <>
	uint64_t cmd_::getParam<uint64_t>(const xmlrpc_c::paramList& paramList, int index)
	{
		return boost::lexical_cast<uint64_t>(paramList.getInt(index));
	}

	/**
	* \brief Get a parameter from the parameter list
	* \param paramList The parameter list
	* \param index Index of the parameter in the parameter list
	* \return The requested parameter
	*
	* This specialized cmd_getParam for the std::string type
	*/
	template <>
	std::string cmd_::getParam<std::string>(const xmlrpc_c::paramList& paramList, int index)
	{
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
	template <>
	art::RunID cmd_::getParam<art::RunID>(const xmlrpc_c::paramList& paramList, int index)
	{
		std::string run_number_string = paramList.getString(index);
		art::RunNumber_t run_number =
			boost::lexical_cast<art::RunNumber_t>(run_number_string);
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
	template <>
	fhicl::ParameterSet cmd_::getParam<fhicl::ParameterSet>(const xmlrpc_c::paramList& paramList, int index)
	{
		std::string configString = paramList.getString(index);
		fhicl::ParameterSet pset = LoadParameterSet(configString);

		return pset;
	}

	template <typename T>
	T cmd_::getParam(const xmlrpc_c::paramList& paramList, int index,
					 T default_value)
	{
		T val = default_value;

		try
		{
			val = getParam<T>(paramList, index);
		}
		catch (const cet::exception& exception)
		{
			throw exception;
		}
		catch (...) {}

		return val;
	}

	void cmd_::execute(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const retvalP)
	{
		std::unique_lock<std::mutex> lk(_c.mutex_, std::try_to_lock);
		if (lk.owns_lock())
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
				TLOG_ERROR("XMLRPC_Commander") << msg << TLOG_ENDL;
			}
			catch (art::Exception& er)
			{
				std::string msg = exception_msg(er, _help);
				*retvalP = xmlrpc_c::value_string(msg);
				TLOG_ERROR("XMLRPC_Commander") << msg << TLOG_ENDL;
			}
			catch (cet::exception& er)
			{
				std::string msg = exception_msg(er, _help);
				*retvalP = xmlrpc_c::value_string(msg);
				TLOG_ERROR("XMLRPC_Commander") << msg << TLOG_ENDL;
			}
			catch (...)
			{
				std::string msg = exception_msg("Unknown exception", _help);
				*retvalP = xmlrpc_c::value_string(msg);
				TLOG_ERROR("XMLRPC_Commander") << msg << TLOG_ENDL;
			}
		}
		else
		{
			*retvalP = xmlrpc_c::value_string("busy");
		}
	}


	//////////////////////////////////////////////////////////////////////

	// JCF, 9/5/14

	// The three "init" transitions all take a FHiCL parameter list, and
	// optionally a timeout and a timestamp; thus we can kill three birds
	// with one stone in the GENERATE_INIT_TRANSITION macro

#define GENERATE_INIT_TRANSITION(NAME, CALL, DESCRIPTION)			\
	/** Command class representing an init transition */		\
  class NAME ## _: public cmd_ {					\
								\
  public:							\
  /** Command class Constructor \
	* \param c xmlrpc_commander to send parsed command to \
	*/ \
  explicit NAME ## _(xmlrpc_commander& c):				\
  cmd_(c, "s:sii", DESCRIPTION) {}				\
								\
  /** Default timeout for command */ \
  static const uint64_t defaultTimeout = 45;				\
  /** Default timestamp for Command */ \
  static const uint64_t defaultTimestamp = std::numeric_limits<const uint64_t>::max(); \
									\
  private:								\
  bool execute_(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const retvalP ) { \
										\
	try {								\
	  getParam<fhicl::ParameterSet>(paramList, 0);			\
	} catch (...) {							\
	  *retvalP = xmlrpc_c::value_string ("The "#NAME" message requires a single argument that is a string containing the initialization ParameterSet");	\
	  return true;							\
	}									\
									\
	return _c._commandable.CALL( getParam<fhicl::ParameterSet>(paramList, 0), \
				 getParam<uint64_t>(paramList, 1, defaultTimeout), \
				 getParam<uint64_t>(paramList, 2, defaultTimestamp) \
				 );					\
  }									\
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
		explicit start_(xmlrpc_commander& c) :
			cmd_(c, "s:iii", "start the run")
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
				*retvalP = xmlrpc_c::value_string("The start message requires the run number as an argument.");
				return true;
			}

			return _c._commandable.start(getParam<art::RunID>(paramList, 0),
										 getParam<uint64_t>(paramList, 1, defaultTimeout),
										 getParam<uint64_t>(paramList, 2, defaultTimestamp)
			);
		}
	};


	//////////////////////////////////////////////////////////////////////

	// JCF, 9/5/14

	// "pause", "resume" and "stop" all take an optional timeout and
	// timestamp parameter, so we can generate them all with the
	// GENERATE_TIMEOUT_TIMESTAMP_TRANSITION macro

#define GENERATE_TIMEOUT_TIMESTAMP_TRANSITION(NAME, CALL, DESCRIPTION, TIMEOUT)	\
		/** NAME ## _ Command class */							\
  class NAME ## _: public cmd_ {					\
									\
public:									\
/** NAME ## _ Constructor \
	\param c xmlrpc_commander to send transition commands to */ \
 NAME ## _(xmlrpc_commander& c):					\
 cmd_(c, "s:ii", DESCRIPTION) {}					\
									\
  /** Default timeout for command */ \
 static const uint64_t defaultTimeout = TIMEOUT ;			\
  /** Default timestamp for Command */ \
 static const uint64_t defaultTimestamp = std::numeric_limits<const uint64_t>::max(); \
									\
private:								\
									\
 bool execute_ (const xmlrpc_c::paramList& paramList , xmlrpc_c::value* const ) { \
									\
   return _c._commandable.CALL( getParam<uint64_t>(paramList, 0, defaultTimeout), \
				getParam<uint64_t>(paramList, 1, defaultTimestamp) \
				);					\
 }									\
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
		shutdown_(xmlrpc_commander& c) :
			cmd_(c, "s:i", "shutdown the program")
		{}

		/** Default timeout for command */
		static const uint64_t defaultTimeout = 45;

	private:

		bool execute_(const xmlrpc_c::paramList& paramList, xmlrpc_c::value* const)
		{
			return _c._commandable.shutdown(getParam<uint64_t>(paramList, 0, defaultTimeout));
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
		status_(xmlrpc_commander& c) :
			cmd_(c, "s:n", "report the current state")
		{}

	private:

		bool execute_(xmlrpc_c::paramList const&, xmlrpc_c::value* const retvalP)
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
		report_(xmlrpc_commander& c) :
			cmd_(c, "s:s", "report statistics")
		{}

	private:
		bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP)
		{
			try
			{
				getParam<std::string>(paramList, 0);
			}
			catch (...)
			{
				*retvalP = xmlrpc_c::value_string("The report message requires a single argument that selects the type of statistics to be reported.");
				return true;
			}

			*retvalP = xmlrpc_c::value_string(_c._commandable.report(getParam<std::string>(paramList, 0)));
			return true;
		}
	};


	/**
	* \brief reset_stats_ Command class
	*/
	class reset_stats_ : public cmd_
	{
	public:
		/**
		* \brief reset_stats_ Constructor
		* \param c xmlrpc_commander to send transition commands to
		*/
		reset_stats_(xmlrpc_commander& c) :
			cmd_(c, "s:s", "reset statistics")
		{}

	private:
		bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP)
		{
			try
			{
				getParam<std::string>(paramList, 0);
			}
			catch (...)
			{
				*retvalP = xmlrpc_c::value_string("The reset_stats message requires a single argument that selects the type of statistics to be reported.");
				return true;
			}

			return _c._commandable.reset_stats(getParam<std::string>(paramList, 0));
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
		legal_commands_(xmlrpc_commander& c) :
			cmd_(c, "s:n", "return the currently legal commands")
		{}

	private:
		bool execute_(xmlrpc_c::paramList const&, xmlrpc_c::value* const retvalP)
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
		register_monitor_(xmlrpc_commander& c) :
			cmd_(c, "s:s", "Get notified of a new monitor")
		{}

	private:
		bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP)
		{
			try
			{
				getParam<fhicl::ParameterSet>(paramList, 0);
			}
			catch (...)
			{
				*retvalP = xmlrpc_c::value_string("The register_monitor command expects a string representing the FHiCL definition of a Transfer plugin");
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
		unregister_monitor_(xmlrpc_commander& c) :
			cmd_(c, "s:s", "Remove a monitor")
		{}

	private:
		bool execute_(xmlrpc_c::paramList const& paramList, xmlrpc_c::value* const retvalP)
		{
			try
			{
				getParam<std::string>(paramList, 0);
			}
			catch (...)
			{
				*retvalP = xmlrpc_c::value_string("The unregister_monitor command expects a string representing the label of the monitor to be removed");
				return true;
			}

			*retvalP = xmlrpc_c::value_string(_c._commandable.unregister_monitor(getParam<std::string>(paramList, 0)));
			return true;
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
			TLOG_INFO("XMLRPC_Commander") << "A shutdown command was sent "
				<< "with parameter "
				<< paramString << "\"" << TLOG_ENDL;
			_server->terminate();
		}
	private:
		xmlrpc_c::serverAbyss *_server;
	};
#endif



	xmlrpc_commander::xmlrpc_commander(fhicl::ParameterSet ps, artdaq::Commandable& commandable)
		: CommanderInterface(ps, commandable)
		, port_(ps.get<int>("id", 0))
		, serverUrl_(ps.get<std::string>("server_url",""))
	{}

	void xmlrpc_commander::run_server() try
	{
		xmlrpc_c::registry registry;

#define register_method(m) \
  xmlrpc_c::methodPtr const ptr_ ## m(new m ## _(*this));\
  registry.addMethod ("daq." #m, ptr_ ## m);

		register_method(init);
		register_method(soft_init);
		register_method(reinit);
		register_method(start);
		register_method(status);
		register_method(report);
		register_method(stop);
		register_method(pause);
		register_method(resume);
		register_method(reset_stats);
		register_method(register_monitor);
		register_method(unregister_monitor);
		register_method(legal_commands);

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
			throw cet::exception("xmlrpc_commander::run") <<
				"Problem with the socket() call; C-style errno == " <<
				errno << " (" << strerror(errno) << ")";
		}

		int enable = 1;
		int retval = setsockopt(socket_file_descriptor,
								SOL_SOCKET, SO_REUSEADDR,
								&enable, sizeof(int));

		if (retval < 0)
		{
			throw cet::exception("xmlrpc_commander::run") <<
				"Problem with the call to setsockopt(); C-style errno == " <<
				errno << " (" << strerror(errno) << ")";
		}

		struct sockaddr_in sockAddr;

		sockAddr.sin_family = AF_INET;
		sockAddr.sin_port = htons(port_);
		sockAddr.sin_addr.s_addr = 0;

		retval = bind(socket_file_descriptor,
					  reinterpret_cast<struct sockaddr*>(&sockAddr),
					  sizeof(sockAddr));

		if (retval != 0)
		{
			close(socket_file_descriptor);
			throw cet::exception("xmlrpc_commander::run") <<
				"Problem with the bind() call; C-style errno == " <<
				errno << " (" << strerror(errno) << ")";
		}

		xmlrpc_c::serverAbyss server(xmlrpc_c::serverAbyss::constrOpt().registryP(&registry).socketFd(socket_file_descriptor));

#if 0
		shutdown_ shutdown_obj(&server);
		registry.setShutdown(&shutdown_obj);
#endif

		TLOG_DEBUG("XMLRPC_Commander") << "running server" << TLOG_ENDL;

		// JCF, 6/3/15

		// Use a catch block to clean up (i.e., close the socket). An
		// opportunity for RAII, although all control paths are limited to
		// this section of the file...

		try
		{
			server.run();
		}
		catch (...)
		{
			TLOG_WARNING("XMLRPC_Commander") << "server threw an exception; closing the socket and rethrowing" << TLOG_ENDL;
			close(socket_file_descriptor);
			throw;
		}

		close(socket_file_descriptor);
		TLOG_DEBUG("XMLRPC_Commander") << "server terminated" << TLOG_ENDL;
	}
	catch (...)
	{
		throw;
	}


	std::string xmlrpc_commander::send_register_monitor(std::string monitor_fhicl)
	{
		if (serverUrl_ == "")
		{
			std::stringstream errmsg;
			errmsg << "Problem attempting XML-RPC call: No server URL set!";
			ExceptionHandler(ExceptionHandlerRethrow::yes,
							 errmsg.str());

		}
		xmlrpc_c::clientSimple myClient;
		xmlrpc_c::value result;
		
		try
		{
			myClient.call(serverUrl_, "daq.register_monitor", "s", &result, monitor_fhicl.c_str());
		}
		catch (...)
		{
			std::stringstream errmsg;
			errmsg << "Problem attempting XML-RPC call on host " << serverUrl_
				<< "; possible causes are malformed FHiCL or nonexistent process at requested port";
			ExceptionHandler(ExceptionHandlerRethrow::yes,
							 errmsg.str());
		}

		return xmlrpc_c::value_string(result);
	}

	std::string xmlrpc_commander::send_unregister_monitor(std::string monitor_label)
	{
		if (serverUrl_ == "")
		{
			std::stringstream errmsg;
			errmsg << "Problem attempting XML-RPC call: No server URL set!";
			ExceptionHandler(ExceptionHandlerRethrow::yes,
							 errmsg.str());

		}

		xmlrpc_c::clientSimple myClient;
		xmlrpc_c::value result;

		try
		{
			myClient.call(serverUrl_, "daq.unregister_monitor", "s", &result, monitor_label.c_str());
		}
		catch (...)
		{
			std::stringstream errmsg;
			errmsg << "Problem attempting to unregister monitor via XML-RPC call on host " << serverUrl_
				<< "; possible causes are that the monitor label \""
				<< monitor_label
				<< "\" is unrecognized by contacted process or process at requested port doesn't exist";
			ExceptionHandler(ExceptionHandlerRethrow::no,
							 errmsg.str());
		}

		return xmlrpc_c::value_string(result);

	}
} // namespace artdaq