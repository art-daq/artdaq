#ifndef artdaq_Application_MPI2_EventBuilderCore_hh
#define artdaq_Application_MPI2_EventBuilderCore_hh

#include <string>
#include <atomic>

#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"

#include "artdaq-utilities/Plugins/MetricManager.hh"

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "artdaq/Application/StatisticsHelper.hh"

namespace artdaq
{
	class EventBuilderCore;
}

/**
 * \brief EventBuilderCore implements the state machine for the EventBuilder artdaq application.
 * EventBuilderCore receives Fragment objects from the DataReceiverManager, and sends them to the EventStore.
 */
class artdaq::EventBuilderCore
{
public:
	static const std::string INPUT_FRAGMENTS_STAT_KEY; ///< Key for the Input Fragments MonitoredQuantity
	static const std::string INPUT_WAIT_STAT_KEY; ///< Key for the Input Wait MonitoredQuantity
	static const std::string STORE_EVENT_WAIT_STAT_KEY; ///< Key for the Store Event Wait MonitoredQuantity

	/**
	 * \brief EventBuilderCore Constructor.
	 * \param rank Rank of the EventBuilder
	 * \param name Friendly name for the EventBuilder
	 */
	EventBuilderCore(int rank, std::string name);

	/**
	* \brief Copy Constructor is deleted
	*/
	EventBuilderCore(EventBuilderCore const&) = delete;

	/**
	* Destructor.
	*/
	~EventBuilderCore();

	/**
	* \brief Copy Assignment operator is deleted
	* \return AggregatorCore copy
	*/
	EventBuilderCore& operator=(EventBuilderCore const&) = delete;

	/**
	* \brief Processes the initialize request.
	* \param pset ParameterSet used to configure the EventBuilderCore
	* \return Whether the initialize attempt succeeded
	*
	* \verbatim
	* EventBuilderCore accepts the following Parameters:
	* "daq" (REQUIRED): FHiCL table containing DAQ configuration
	*   "event_builder" (REQUIRED): FHiCL table containing Aggregator paramters
	*     "expected_fragments_per_event" (REQUIRED): Number of Fragment objects to collect before sending them to art
	*     "use_art" (REQUIRED): Whether to start an art thread in the EventStore
	*     "inrun_recv_timeout_usec" (Default: 100000): Amount of time to wait for new Fragment objects while running
	*     "endrun_recv_timeout_usec" (Default: 20000000): Amount of time to wait for additional Fragment objects at EndOfRun
	*     "pause_recv_timeout_usec" (Default: 3000000): Amount of time to wait for additional Fragment objects at PauseRun
	*     "verbose" (Default: false): Whether to print more verbose status information
	*   "metrics": FHiCL table containing configuration for MetricManager
	* \endverbatim
	*
	*  Note that the "event_builder" ParameterSet is also used to configure the EventStore. See that class' documentation for more information.
	*/
	bool initialize(fhicl::ParameterSet const& pset);

	/**
	* \brief Start the EventBuilderCore
	* \param id Run ID of the current run
	* \return True if no exception
	*/
	bool start(art::RunID id);

	/**
	* \brief Stops the EventBuilderCore
	* \return True if no exception
	*/
	bool stop();

	/**
	* \brief Pauses the EventBuilderCore
	* \return True if no exception
	*/
	bool pause();

	/**
	* \brief Resumes the EventBuilderCore
	* \return True if no exception
	*/
	bool resume();

	/**
	* \brief Shuts Down the EventBuilderCore
	* \return If the shutdown was successful
	*/
	bool shutdown();

	/**
	* \brief Soft-Initializes the EventBuilderCore. No-Op
	* \param pset ParameterSet for configuring EventBuilderCore
	* \return Always returns true
	*/
	bool soft_initialize(fhicl::ParameterSet const& pset);

	/**
	* \brief Reinitializes the EventBuilderCore.
	* \param pset ParameterSet for configuring EventBuilderCore
	* \return True if no exception
	*/
	bool reinitialize(fhicl::ParameterSet const& pset);

	/**
	 * \brief The main loop of the EventBuilderCore. Receives Fragment objects from DataReceiverManager
	 * and enqueues them on the EventStore, with special processing for EndOfDatafragmentType Fragments
	 * \return 0 if shutdown successfully
	 */
	size_t process_fragments();

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

private:
	void initializeEventStore(fhicl::ParameterSet pset);

	std::string name_;

	std::string init_string_;
	fhicl::ParameterSet previous_pset_;

	fhicl::ParameterSet data_pset_;
	size_t expected_fragments_per_event_;
	size_t eod_fragments_received_;
	bool use_art_;
	art::RunID run_id_;

	std::unique_ptr<artdaq::DataReceiverManager> receiver_ptr_;
	std::unique_ptr<artdaq::EventStore> event_store_ptr_;
	bool art_initialized_;
	std::atomic<bool> stop_requested_;
	std::atomic<bool> pause_requested_;
	std::atomic<bool> run_is_paused_;
	std::atomic<bool> processing_fragments_;
	size_t inrun_recv_timeout_usec_;
	size_t endrun_recv_timeout_usec_;
	size_t pause_recv_timeout_usec_;
	bool verbose_;

	size_t fragment_count_in_run_;

	/* This is used for syncronization between the thread running
	   process_fragments() and XMLRPC calls.  This will be locked before data
	   readout begins by the start() and resume() methods in the event builder.
	   It will be unlocked by the process_fragments() thread once EOD fragments
	   and all data has been received.  The stop() and pause() methods will
	   attempt to lock the mutex as well and will be blocked until all data has
	   been clocked into the EventBuilderCore. */
	std::mutex flush_mutex_;

	// attributes and methods for statistics gathering & reporting
	artdaq::StatisticsHelper statsHelper_;

	std::string buildStatisticsString_();

	artdaq::MetricManager metricMan_;

	void sendMetrics_();

	void logMessage_(std::string const& text);
};

#endif /* artdaq_Application_MPI2_EventBuilderCore_hh */
