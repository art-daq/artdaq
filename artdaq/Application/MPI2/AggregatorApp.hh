#ifndef artdaq_Application_MPI2_AggregatorApp_hh
#define artdaq_Application_MPI2_AggregatorApp_hh

#include <future>

#include "artdaq/Application/MPI2/AggregatorCore.hh"
#include "artdaq/Application/Commandable.hh"
#include "artdaq/DAQrate/RHandles.hh"

namespace artdaq
{
  class AggregatorApp;
}

class artdaq::AggregatorApp : public artdaq::Commandable
{
public:
  AggregatorApp(int mpi_rank, MPI_Comm local_group_comm);
  AggregatorApp(AggregatorApp const&) = delete;
  virtual ~AggregatorApp() = default;
  AggregatorApp& operator=(AggregatorApp const&) = delete;

  // these methods provide the operations that are used by the state machine
  bool do_initialize(fhicl::ParameterSet const&) override;
  bool do_start(art::RunID run, uint64_t timeout, uint64_t timestamp) override;
  bool do_stop() override;
  bool do_pause() override;
  bool do_resume() override;
  bool do_shutdown() override;
  bool do_soft_initialize(fhicl::ParameterSet const&) override;
  bool do_reinitialize(fhicl::ParameterSet const&) override;

  /* Report_ptr */
  std::string report(std::string const& which) const override;

private:
  int mpi_rank_;
  MPI_Comm local_group_comm_;
  std::unique_ptr<AggregatorCore> aggregator_ptr_;
  std::future<size_t> aggregator_future_;
};

#endif
