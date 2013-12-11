#ifndef artdaq_Application_MPI2_Aggregator_hh
#define artdaq_Application_MPI2_Aggregator_hh

#include <string>
#include <vector>
#include <atomic>
#include <thread>

#include "fhiclcpp/ParameterSet.h"
#include "art/Persistency/Provenance/RunID.h"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQdata/CommandableFragmentGenerator.hh"
#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq/DAQrate/GlobalQueue.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "artdaq/Application/MPI2/StatisticsHelper.hh"

#include <sys/shm.h> 

namespace artdaq
{
  class Aggregator;
}

class artdaq::Aggregator
{
public:
  static const std::string INPUT_EVENTS_STAT_KEY;
  static const std::string INPUT_WAIT_STAT_KEY;
  static const std::string STORE_EVENT_WAIT_STAT_KEY;

  Aggregator(int mpi_rank, MPI_Comm local_group_comm);
  Aggregator(Aggregator const&) = delete;
  ~Aggregator();
  Aggregator& operator=(Aggregator const&) = delete;

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
  art::RunID run_id_;
  bool art_initialized_;

  std::string init_string_;
  uint64_t max_fragment_size_words_;
  size_t mpi_buffer_count_;
  size_t first_data_sender_rank_;
  size_t data_sender_count_;
  size_t expected_events_per_bunch_;
  bool print_event_store_stats_;
  size_t inrun_recv_timeout_usec_;
  size_t endrun_recv_timeout_usec_;
  size_t pause_recv_timeout_usec_;
  size_t onmon_event_prescale_;
  bool is_data_logger_;
  bool is_online_monitor_;

  std::unique_ptr<artdaq::RHandles> receiver_ptr_;
  std::unique_ptr<artdaq::EventStore> event_store_ptr_;
  artdaq::RawEventQueue &event_queue_;
  fhicl::ParameterSet previous_pset_;
  std::atomic<bool> stop_requested_;
  std::atomic<bool> local_pause_requested_;

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

  // *** Shared memory declarations ***
  struct ShmStruct {
    size_t hasFragment;
    size_t fragmentSizeWords;
    artdaq::RawDataType fragmentInnards[2];
  };
  int shm_segment_id_;
  ShmStruct* shm_ptr_;
  size_t fragment_count_to_shm_;

  void attachToSharedMemory_(bool initialize);
  void copyFragmentToSharedMemory_(bool& fragment_has_been_copied,
                                   bool& esr_has_been_copied,
                                   bool& eod_has_been_copied,
                                   artdaq::Fragment& fragment,
                                   size_t send_timeout_usec);
  size_t receiveFragmentFromSharedMemory_(artdaq::Fragment& fragment,
                                          size_t receiveTimeout);
  void detachFromSharedMemory_(bool destroy);
};

#endif

//  LocalWords:  ds
