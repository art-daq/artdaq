#ifndef artdaq_Application_MPI2_AggregatorCore_hh
#define artdaq_Application_MPI2_AggregatorCore_hh

#include <string>
#include <vector>
#include <atomic>
#include <thread>

#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"

#include "artdaq-core/Core/GlobalQueue.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"

#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "artdaq/Application/StatisticsHelper.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"


namespace artdaq
{
	class AggregatorCore;
}

/**
 * \brief AggregatorCore contains the state machine for the Aggregator artdaq application.
 * AggregatorCore processes incoming events in one of three roles: Data Logger, Online Monitor, or Dispatcher.
 */
class artdaq::AggregatorCore
{
public:
	static const std::string INPUT_EVENTS_STAT_KEY; ///< Key for the Input Events MonitoredQuantity
	static const std::string INPUT_WAIT_STAT_KEY; ///< Key for the Input Wait MonitoredQuantity
	static const std::string STORE_EVENT_WAIT_STAT_KEY; ///< Key for the EventStore Event Wait MonitoredQuantity
	static const std::string SHM_COPY_TIME_STAT_KEY; ///< Key for the Shared Memory Copy Time MonitoredQuantity
	static const std::string FILE_CHECK_TIME_STAT_KEY; ///< Key for the File Check Time MonitoredQuantity

	/**
	* \brief AggregatorCore Constructor.
	* \param rank Rank of the Aggregator
	* \param name Friendly name for the Aggregator
	* \todo Make the global queue size configurable
	*/
	AggregatorCore(int rank, std::string name);

	/**
	 * \brief Copy Constructor is deleted
	 */
	AggregatorCore(AggregatorCore const&) = delete;

	/**
	* Destructor.
	*/
	~AggregatorCore();

	/**
	 * \brief Copy Assignment operator is deleted
	 * \return AggregatorCore copy
	 */
	AggregatorCore& operator=(AggregatorCore const&) = delete;

	/**
	* \brief Processes the initialize request.
	* \param pset ParameterSet used to configure the AggregatorCore
	* \return Whether the initialize attempt succeeded
	* 
	* AggregatorCore accepts the following Parameters:
	* "daq" (REQUIRED): FHiCL table containing DAQ configuration
	*   "aggregator" (REQUIRED): FHiCL table containing Aggregator paramters
	*     "expected_events_per_bunch" (REQUIRED): Number of events to collect before sending them to art
	*     "enq_timeout" (Default: 5.0): Maximum amount of time to wait while enqueueing events to the ConcurrentQueue
	*     "is_data_logger": True if the Aggregator is a Data Logger
	*     "is_online_monitor": True if the Aggregator is an Online Monitor. is_data_logger takes precedence
	*     "is_dispatcher": True if the Aggregator is a Dispatcher. is_data_logger and is_online_monitor take precedence
	*       NOTE: At least ONE of these three parameters must be specified.
	*     "xmlrpc_client_list" (Default: ""): List of XMLRPC addresses of other applications in the artdaq system
	*     "subrun_size_MB" (Default: 0): Maximum size for Aggregator-based subrun rollover
	*     "subrun_duration" (Dfeault: 0): Maximum time for Aggregator-based subrun rollover
	*     "subrun_event_count" (Default: 0): Maximum event count for Aggregator-based subrun rollover
	*     "inrun_recv_timeout_usec" (Default: 100000): Amount of time to wait for new events while running
	*     "endrun_recv_timeout_usec" (Default: 20000000): Amount of time to wait for additional events at EndOfRun
	*     "pause_recv_timeout_usec" (Default: 3000000): Amount of time to wait for additional events at PauseRun
	*     "onmon_event_prescale" (Default: 1): Only send 1/N events to art for online monitoring (requires is_data_logger: true)
	*     "filesize_check_interval_seconds" (Default: 20): Interval to check the file size when using Aggregator-based subrun rollover
	*     "filesize_check_interval_events" (Default: 20): Interval to check the file size when using Aggregator-based subrun rollover
	*   "metrics": FHiCL table containing configuration for MetricManager
	* "outputs" (REQUIRED): FHiCL table containing output parameters
	*   "normalOutput" (REQUIRED): FHiCL table containing default output parameters
	*     "fileName" (Default: ""): Name template of the output file. Used to determine output directory
	*     
	*  Note that the "aggregator" ParameterSet is also used to configure the EventStore. See that class' documentation for more information.
	*/
	bool initialize(fhicl::ParameterSet const& pset);

	/**
	 * \brief Start the AggregatorCore
	 * \param id Run ID of the current run
	 * \return True if no exception
	 */
	bool start(art::RunID id);

	/**
	 * \brief Stops the AggregatorCore
	 * \return True if no exception
	 */
	bool stop();

	/**
	* \brief Pauses the AggregatorCore
	* \return True if no exception
	*/
	bool pause();

	/**
	* \brief Resumes the AggregatorCore
	* \return True if no exception
	*/
	bool resume();

	/**
	* \brief Shuts Down the AggregatorCore
	* \return If the shutdown was successful
	*/
	bool shutdown();

	/**
	* \brief Soft-Initializes the AggregatorCore. No-Op
	* \param pset ParameterSet for configuring AggregatorCore
	* \return Always returns true
	*/
	bool soft_initialize(fhicl::ParameterSet const& pset);

	/**
	* \brief Reinitializes the AggregatorCore. No-Op
	* \param pset ParameterSet for configuring AggregatorCore
	* \return Always returns true
	*/
	bool reinitialize(fhicl::ParameterSet const& pset);

	/**
	 * \brief The main working loop of the AggregatorCore. Receives events from DataReceiverManager
	 * and processes them according to the Aggreagtor's configuration
	 * \return 0 if stopped normally
	 * 
	 * process_fragments first receives an event from DataReceiverManager, and checks the validity
	 * of the sender rank. If the Aggregator is a Data Logger, it checks the onmon presale and if the event
	 * passes, it sends it to any online monitors or dispatchers. If the Aggregator is a dispatcher, it saves
	 * the event if it is the init event, for later re-sending. It will dispatch the event to any connected
	 * monitors. The event is then sent to the EventStore for art processing. If the Aggregator is a Data Logger,
	 * it will check for configured Aggregator-based subrun rollover.
	 */
	size_t process_fragments();

	/**
	 * \brief Send a report on a given run-time quantity
	 * \param which Which quantity to report
	 * \return A string containing the requested quantity.
	 * 
	 * report accepts the following values of "which":
	 * "event_count": The number of events received, or -1 if not initialized
	 * "run_duration": The time that the current run has been running for, or the previous if not running
	 * "file_size": The size of the latest file written, or being written
	 * "subrun_number": The current subrun number, or -1 if not initialized
	 * "incomplete_event_count": The number of incomplete event bunches in the EventStore, or -1 if not initalized
	 * 
	 * Anything else will return the run number and an error message.
	 */
	std::string report(std::string const& which) const;

	/**
	 * \brief Create a new TransferInterface instance using the given configuration
	 * \param pset ParameterSet used to configure the TransferInterface
	 * \return String detailing any errors encountered or "Success"
	 * 
	 * See TransferInterface for details on the expected configuration
	 */
	std::string register_monitor(fhicl::ParameterSet const& pset);

	/**
	 * \brief Delete the TransferInterface having the given unique label
	 * \param label Label of the TransferInterface to delete
	 * \return String detailing any errors encountered or "Success"
	 */
	std::string unregister_monitor(std::string const& label);

private:
	std::string name_;
	art::RunID run_id_;
	bool art_initialized_;

	fhicl::ParameterSet data_pset_;
	std::string init_string_;
	size_t expected_events_per_bunch_;
	size_t inrun_recv_timeout_usec_;
	size_t endrun_recv_timeout_usec_;
	size_t pause_recv_timeout_usec_;
	size_t onmon_event_prescale_;
	int32_t filesize_check_interval_seconds_;
	int32_t filesize_check_interval_events_;
	bool is_data_logger_;
	bool is_online_monitor_;
	bool is_dispatcher_;
	artdaq::detail::seconds enq_timeout_;

	std::unique_ptr<artdaq::DataReceiverManager> receiver_ptr_;
	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_;
	std::unique_ptr<artdaq::EventStore> event_store_ptr_;
	artdaq::RawEventQueue& event_queue_;
	fhicl::ParameterSet previous_pset_;
	std::atomic<bool> stop_requested_;
	std::atomic<bool> local_pause_requested_;
	std::atomic<bool> processing_fragments_;

	size_t event_count_in_run_;
	size_t event_count_in_subrun_;
	time_t subrun_start_time_;
	std::string disk_writing_directory_;

	size_t getLatestFileSize_() const;

	std::vector<std::vector<std::string>> xmlrpc_client_lists_;
	size_t file_close_threshold_bytes_;
	time_t file_close_timeout_secs_;
	size_t file_close_event_count_;

	bool sendPauseAndResume_();

	std::atomic<bool> system_pause_requested_;
	std::shared_ptr<std::thread> pause_thread_;

	void logMessage_(std::string const& text);

	artdaq::StatisticsHelper stats_helper_;

	std::string buildStatisticsString_();

	double previous_run_duration_;
	artdaq::MetricManager metricMan_;

	void sendMetrics_();
	
	std::unique_ptr<Fragment> init_fragment_ptr_;

	std::mutex dispatcher_transfers_mutex_;
	std::deque<std::unique_ptr<TransferInterface>> dispatcher_transfers_;
	size_t new_transfers_;
};

#endif

//  LocalWords:  ds
