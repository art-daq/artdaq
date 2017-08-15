#ifndef artdaq_Application_MPI2_DataReceiverCore_hh
#define artdaq_Application_MPI2_DataReceiverCore_hh

#include <string>
#include <atomic>

#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"

#include "artdaq-utilities/Plugins/MetricManager.hh"

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/Application/StatisticsHelper.hh"

namespace artdaq
{
	class DataReceiverCore;
}

/**
 * \brief DataReceiverCore implements the state machine for the DataReceiver artdaq application.
 * DataReceiverCore receives Fragment objects from the DataReceiverManager, and sends them to the EventStore.
 */
class artdaq::DataReceiverCore
{
public:

	/**
	 * \brief DataReceiverCore Constructor.
	 * \param rank Rank of the DataReceiver
	 * \param name Friendly name for the DataReceiver
	 */
	DataReceiverCore(int rank, std::string name);

	/**
	* \brief Copy Constructor is deleted
	*/
	DataReceiverCore(DataReceiverCore const&) = delete;

	/**
	* Destructor.
	*/
	~DataReceiverCore();

	/**
	* \brief Copy Assignment operator is deleted
	* \return AggregatorCore copy
	*/
	DataReceiverCore& operator=(DataReceiverCore const&) = delete;

	/**
	* \brief Processes the initialize request.
	* \param pset ParameterSet used to configure the DataReceiverCore
	* \return Whether the initialize attempt succeeded
	*
	* \verbatim
	* DataReceiverCore accepts the following Parameters:
	* "daq" (REQUIRED): FHiCL table containing DAQ configuration
	*   "event_builder" (REQUIRED): FHiCL table containing Aggregator paramters
	*     "fragment_count" (REQUIRED): Number of Fragment objects to collect before sending them to art
	*     "inrun_recv_timeout_usec" (Default: 100000): Amount of time to wait for new Fragment objects while running
	*     "endrun_recv_timeout_usec" (Default: 20000000): Amount of time to wait for additional Fragment objects at EndOfRun
	*     "pause_recv_timeout_usec" (Default: 3000000): Amount of time to wait for additional Fragment objects at PauseRun
	*     "verbose" (Default: false): Whether to print more verbose status information
	*   "metrics": FHiCL table containing configuration for MetricManager
	* \endverbatim
	*
	*  Note that the "event_builder" ParameterSet is also used to configure the SharedMemoryEventManager. See that class' documentation for more information.
	*/
	virtual bool initialize(fhicl::ParameterSet const& pset) = 0;

	/**
	* \brief Start the DataReceiverCore
	* \param id Run ID of the current run
	* \return True if no exception
	*/
	bool start(art::RunID id);

	/**
	* \brief Stops the DataReceiverCore
	* \return True if no exception
	*/
	bool stop();

	/**
	* \brief Pauses the DataReceiverCore
	* \return True if no exception
	*/
	bool pause();

	/**
	* \brief Resumes the DataReceiverCore
	* \return True if no exception
	*/
	bool resume();

	/**
	* \brief Shuts Down the DataReceiverCore
	* \return If the shutdown was successful
	*/
	bool shutdown();

	/**
	* \brief Soft-Initializes the DataReceiverCore. No-Op
	* \param pset ParameterSet for configuring DataReceiverCore
	* \return Always returns true
	*/
	bool soft_initialize(fhicl::ParameterSet const& pset);

	/**
	* \brief Reinitializes the DataReceiverCore.
	* \param pset ParameterSet for configuring DataReceiverCore
	* \return True if no exception
	*/
	bool reinitialize(fhicl::ParameterSet const& pset);
	
	/**
	* \brief Send a report on a given run-time quantity
	* \param which Which quantity to report
	* \return A string containing the requested quantity.
	*
	* report accepts the following values of "which":
	* "event_count": The number of events received, or -1 if not initialized
	* "incomplete_event_count": The number of incomplete event bunches in the EventStore, or -1 if not initalized
	*
	* Anything else will return the run number and an error message.
	*/
	std::string report(std::string const& which) const;

protected:
	/**
	 * \brief Initialize the DataReceiverCore (should be called from initialize() overrides
	 * \param pset ParameterSet for art configuration
	 * \param data_pset ParameterSet for DataReceiverManager and SharedMemoryEventManager configuration
	 * \param metric_pset ParameterSet for MetricManager
	 * \return Whether the initialize succeeded
	 */
	bool initializeDataReceiver(fhicl::ParameterSet const& pset, fhicl::ParameterSet const& data_pset, fhicl::ParameterSet const& metric_pset);

	std::string name_; ///< Name of this DataReceiverCore instance

	std::unique_ptr<DataReceiverManager> receiver_ptr_; ///< Pointer to the DataReceiverManager
	std::shared_ptr<SharedMemoryEventManager> event_store_ptr_; ///< Pointer to the SharedMemoryEventManager (art)
	std::atomic<bool> stop_requested_; ///< Stop has been requested?
	std::atomic<bool> pause_requested_; ///< Pause has been requested?
	std::atomic<bool> run_is_paused_; ///< Pause has been successfully completed?
	bool verbose_; ///< Whether to log verbosely
	
	MetricManager metricMan_; ///< MetricManager concrete instance (for Globals.hh::metricMan)
	
	/**
	 * \brief Log a message, setting severity based on verbosity flag
	 * \param text Message to log
	 */
	void logMessage_(std::string const& text);
};

#endif /* artdaq_Application_MPI2_DataReceiverCore_hh */