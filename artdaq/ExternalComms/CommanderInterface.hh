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

		virtual void run_server() = 0;

		virtual std::string send_init(fhicl::ParameterSet, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_init!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_soft_init(fhicl::ParameterSet, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_soft_init!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_reinit(fhicl::ParameterSet, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_reinit!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_start(art::RunID, uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_start!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_pause(uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_pause!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_resume(uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_resume!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_stop(uint64_t, uint64_t)
		{
#pragma message "Using default implementation of send_stop!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_shutdown(uint64_t)
		{
#pragma message "Using default implementation of send_shutdown!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_status()
		{
#pragma message "Using default implementation of send_status!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_report(std::string)
		{
#pragma message "Using default implementation of send_report!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_reset_statistics(std::string)
		{
#pragma message "Using default implementation of send_reset_statistics!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_legal_commands()
		{
#pragma message "Using default implementation of send_legal_commands!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_register_monitor(std::string)
		{
#pragma message "Using default implementation of send_register_monitor!"
			return "NOT IMPLEMENTED";
		}
		virtual std::string send_unregister_monitor(std::string)
		{
#pragma message "Using default implementation of send_unregister_monitor!"
			return "NOT IMPLEMENTED";
		}


	private:

	public:
		artdaq::Commandable& _commandable;

	protected:
		int _id;
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
