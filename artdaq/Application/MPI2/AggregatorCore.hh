#ifndef artdaq_Application_MPI2_AggregatorCore_hh
#define artdaq_Application_MPI2_AggregatorCore_hh

#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>
#include <iostream>
#include <queue>


#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq-core/Core/GlobalQueue.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "artdaq/Application/MPI2/StatisticsHelper.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

namespace artdaq
{
	class AggregatorCore;
}

class artdaq::AggregatorCore
{
public:
	static const std::string INPUT_EVENTS_STAT_KEY;
	static const std::string INPUT_WAIT_STAT_KEY;
	static const std::string STORE_EVENT_WAIT_STAT_KEY;
	static const std::string SHM_COPY_TIME_STAT_KEY;
	static const std::string FILE_CHECK_TIME_STAT_KEY;

	AggregatorCore(int mpi_rank, MPI_Comm local_group_comm, std::string name);

	AggregatorCore(AggregatorCore const&) = delete;

	~AggregatorCore();

	AggregatorCore& operator=(AggregatorCore const&) = delete;

	bool initialize(fhicl::ParameterSet const&);

	bool start(art::RunID);

	bool stop();

	bool pause();

	bool resume();

	bool shutdown();

	bool soft_initialize(fhicl::ParameterSet const&);

	bool reinitialize(fhicl::ParameterSet const&);

	size_t process_fragments();

	std::string report(std::string const& which) const;

	std::string register_monitor(fhicl::ParameterSet const&);

	std::string unregister_monitor(std::string const&);

private:
	MPI_Comm local_group_comm_;
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

	std::unique_ptr<TransferInterface> data_logger_transfer_;

	std::unique_ptr<Fragment> init_fragment_ptr_;

	std::mutex dispatcher_transfers_mutex_;
	std::vector<std::unique_ptr<TransferInterface>> dispatcher_transfers_;
	size_t new_transfers_;
};

#endif

//  LocalWords:  ds
