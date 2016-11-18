#ifndef artdaq_Application_MPI2_BoardReaderCore_hh
#define artdaq_Application_MPI2_BoardReaderCore_hh

#include <string>
#include <vector>
#include <iostream>

#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "artdaq/Application/Commandable.hh"
#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQrate/DataTransferManager.hh"
#include "artdaq/Application/MPI2/StatisticsHelper.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"

namespace artdaq
{
  class BoardReaderCore;
}

class artdaq::BoardReaderCore
{
public:
  static const std::string FRAGMENTS_PROCESSED_STAT_KEY;
  static const std::string INPUT_WAIT_STAT_KEY;
  static const std::string BRSYNC_WAIT_STAT_KEY;
  static const std::string OUTPUT_WAIT_STAT_KEY;
  static const std::string FRAGMENTS_PER_READ_STAT_KEY;

  BoardReaderCore(Commandable& parent_application, MPI_Comm local_group_comm,
                  std::string name);
  BoardReaderCore(BoardReaderCore const&) = delete;
  ~BoardReaderCore();
  BoardReaderCore& operator=(BoardReaderCore const&) = delete;

  bool initialize(fhicl::ParameterSet const&, uint64_t, uint64_t);
  bool start(art::RunID, uint64_t, uint64_t);
  bool stop(uint64_t, uint64_t);
  bool pause(uint64_t, uint64_t);
  bool resume(uint64_t, uint64_t);
  bool shutdown(uint64_t);
  bool soft_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t);
  bool reinitialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

  size_t process_fragments();

  std::string report(std::string const&) const;

private:
  Commandable& parent_application_;
  int mpi_rank_;
  MPI_Comm local_group_comm_;
  std::unique_ptr<CommandableFragmentGenerator> generator_ptr_;
  art::RunID run_id_;
  std::string name_;

  fhicl::ParameterSet data_pset_;
  uint64_t max_fragment_size_words_;
  size_t mpi_buffer_count_;
  size_t first_evb_rank_;
  size_t evb_count_;
  int rt_priority_;
  bool skip_seqId_test_;
  bool synchronous_sends_;
  int mpi_sync_fragment_interval_;
  double mpi_sync_wait_threshold_fraction_;
  int mpi_sync_wait_threshold_count_;
  size_t mpi_sync_wait_interval_usec_;
  int mpi_sync_wait_log_level_;
  int mpi_sync_wait_log_interval_sec_;

  std::unique_ptr<artdaq::DataTransferManager> sender_ptr_;

  size_t fragment_count_;
  artdaq::Fragment::sequence_id_t prev_seq_id_;
  std::atomic<bool> stop_requested_;
  std::atomic<bool> pause_requested_;

  // attributes and methods for statistics gathering & reporting
  artdaq::StatisticsHelper statsHelper_;
  std::string buildStatisticsString_();
  artdaq::MetricManager metricMan_;
  void sendMetrics_();

};

#endif /* artdaq_Application_MPI2_BoardReaderCore_hh */
