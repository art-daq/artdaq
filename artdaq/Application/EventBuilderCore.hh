#ifndef artdaq_Application_MPI2_EventBuilderCore_hh
#define artdaq_Application_MPI2_EventBuilderCore_hh

#include "artdaq/Application/DataReceiverCore.hh"
#include "canvas/Persistency/Provenance/RunID.h"
#include "fhiclcpp/ParameterSet.h"

namespace artdaq {
class EventBuilderCore;
}

/**
 * \brief EventBuilderCore implements the state machine for the EventBuilder artdaq application.
 * EventBuilderCore receives Fragment objects from the DataReceiverManager, and sends them to the EventStore.
 */
class artdaq::EventBuilderCore : public DataReceiverCore
{
public:
	/**
	 * \brief EventBuilderCore Constructor.
	 */
	EventBuilderCore() = default;

	/**
	* \brief Copy Constructor is deleted
	*/
	EventBuilderCore(EventBuilderCore const&) = delete;

	/**
	* Destructor.
	*/
	~EventBuilderCore()
	{
		TLOG(TLVL_DEBUG + 32) << "Destructor";
	}

	/**
	* \brief Copy Assignment operator is deleted
	* \return AggregatorCore copy
	*/
	EventBuilderCore& operator=(EventBuilderCore const&) = delete;
	EventBuilderCore(EventBuilderCore&&) = delete;             ///< Move Constructor is deleted
	EventBuilderCore& operator=(EventBuilderCore&&) = delete;  ///< Move Assignment Operator is deleted

	/**
	* \brief Processes the initialize request.
	* \param pset ParameterSet used to configure the EventBuilderCore
	* \return Whether the initialize attempt succeeded
	*
	* \verbatim
	* EventBuilderCore accepts the following Parameters:
	* "daq" (REQUIRED): FHiCL table containing DAQ configuration
	*   "event_builder" (REQUIRED): FHiCL table containing Aggregator paramters
	*     "fragment_count" (REQUIRED): Number of Fragment objects to collect before sending them to art
	*     "inrun_recv_timeout_usec" (Default: 100000): Amount of time to wait for new Fragment objects while running
	*     "endrun_recv_timeout_usec" (Default: 20000000): Amount of time to wait for additional Fragment objects at EndOfRun
	*     "pause_recv_timeout_usec" (Default: 3000000): Amount of time to wait for additional Fragment objects at PauseRun
	*     "verbose" (Default: true): Whether to print transition messages
	*   "metrics": FHiCL table containing configuration for MetricManager
	* \endverbatim
	*
	*  Note that the "event_builder" ParameterSet is also used to configure the SharedMemoryEventManager. See that class' documentation for more information.
	*/
	bool initialize(fhicl::ParameterSet const& pset) override;
};

#endif /* artdaq_Application_MPI2_EventBuilderCore_hh */
