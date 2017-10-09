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
			, _id(ps.get<int>("id"))
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
