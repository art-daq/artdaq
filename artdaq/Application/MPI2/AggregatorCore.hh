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
#include "art/Persistency/Provenance/RunID.h"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq-core/Core/GlobalQueue.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "artdaq/Application/MPI2/StatisticsHelper.hh"
#include "artdaq/DAQrate/MetricManager.hh"

#include <sys/shm.h> 

#include <ndds/ndds_cpp.h>

namespace artdaq
{
  class AggregatorCore;

  class OctetsListener: public DDSDataReaderListener {
  public:
    
    void on_data_available(DDSDataReader *reader);

    size_t receiveFragmentFromDDS(artdaq::Fragment& fragment,
				  size_t receiveTimeout);

  private:

    DDS_Octets dds_octets_;
    std::queue<DDS_Octets> dds_octets_queue_;

    std::mutex queue_mutex_;

  };

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

private:
  int mpi_rank_;
  MPI_Comm local_group_comm_;
  std::string name_;
  art::RunID run_id_;
  bool art_initialized_;

  std::string init_string_;
  uint64_t max_fragment_size_words_;
  size_t mpi_buffer_count_;
  size_t first_data_sender_rank_;
  size_t data_sender_count_;
  size_t expected_events_per_bunch_;
  size_t inrun_recv_timeout_usec_;
  size_t endrun_recv_timeout_usec_;
  size_t pause_recv_timeout_usec_;
  size_t onmon_event_prescale_;
  int32_t filesize_check_interval_seconds_;
  int32_t filesize_check_interval_events_;
  bool is_data_logger_;
  bool is_online_monitor_;

  std::unique_ptr<artdaq::RHandles> receiver_ptr_;
  std::unique_ptr<artdaq::EventStore> event_store_ptr_;
  artdaq::RawEventQueue &event_queue_;
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

  std::string EVENT_RATE_METRIC_NAME_;
  std::string EVENT_SIZE_METRIC_NAME_;
  std::string DATA_RATE_METRIC_NAME_;
  std::string INPUT_WAIT_METRIC_NAME_;
  std::string EVENT_STORE_WAIT_METRIC_NAME_;
  std::string SHM_COPY_TIME_METRIC_NAME_;
  std::string FILE_CHECK_TIME_METRIC_NAME_;

  // *** Shared memory declarations ***
  struct ShmStruct {
    size_t hasFragment;
    size_t fragmentSizeWords;
    artdaq::RawDataType fragmentInnards[2];
  };
  int shm_segment_id_;
  ShmStruct* shm_ptr_;
  size_t fragment_count_to_shm_;


  std::unique_ptr<DDSDomainParticipant, std::function<void(DDSDomainParticipant*)> >  participant_;

  DDSTopic* topic_octets_;
  DDSOctetsDataWriter* octets_writer_;
  DDSDataReader* octets_reader_;
  OctetsListener octets_listener_;

  static void participantDeleter(DDSDomainParticipant* participant);

  void attachToSharedMemory_(bool initialize);
  void copyFragmentToSharedMemory_(bool& fragment_has_been_copied,
                                   bool& esr_has_been_copied,
                                   bool& eod_has_been_copied,
                                   artdaq::Fragment& fragment,
                                   size_t send_timeout_usec);
  size_t receiveFragmentFromSharedMemory_(artdaq::Fragment& fragment,
                                          size_t receiveTimeout);
  void detachFromSharedMemory_(bool destroy);

  void copyFragmentToDDS_(bool& fragment_has_been_copied,
			  bool& esr_has_been_copied,
			  bool& eod_has_been_copied,
			  artdaq::Fragment& fragment);

};

#endif

//  LocalWords:  ds
