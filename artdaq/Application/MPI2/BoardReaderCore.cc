#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/MPI2/BoardReaderCore.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/Application/makeCommandableFragmentGenerator.hh"
#include "art/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include <pthread.h>
#include <sched.h>
#include <algorithm>
#include "tracelib.h"

const std::string artdaq::BoardReaderCore::
  FRAGMENTS_PROCESSED_STAT_KEY("BoardReaderCoreFragmentsProcessed");
const std::string artdaq::BoardReaderCore::
  INPUT_WAIT_STAT_KEY("BoardReaderCoreInputWaitTime");
const std::string artdaq::BoardReaderCore::
  OUTPUT_WAIT_STAT_KEY("BoardReaderCoreOutputWaitTime");
const std::string artdaq::BoardReaderCore::
  FRAGMENTS_PER_READ_STAT_KEY("BoardReaderCoreFragmentsPerRead");

/**
 * Default constructor.
 */
artdaq::BoardReaderCore::BoardReaderCore(MPI_Comm local_group_comm, std::string name) :
  local_group_comm_(local_group_comm), generator_ptr_(nullptr), name_(name),
  stop_requested_(false), pause_requested_(false)
{
  mf::LogDebug(name_) << "Constructor";
  statsHelper_.addMonitoredQuantityName(FRAGMENTS_PROCESSED_STAT_KEY);
  statsHelper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
  statsHelper_.addMonitoredQuantityName(OUTPUT_WAIT_STAT_KEY);
  statsHelper_.addMonitoredQuantityName(FRAGMENTS_PER_READ_STAT_KEY);
}

/**
 * Destructor.
 */
artdaq::BoardReaderCore::~BoardReaderCore()
{
  mf::LogDebug(name_) << "Destructor";
}

/**
 * Processes the initialize request.
 */
bool artdaq::BoardReaderCore::initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t )
{
  mf::LogDebug(name_) << "initialize method called with "
                                   << "ParameterSet = \"" << pset.to_string()
                                   << "\".";

  // pull out the relevant parts of the ParameterSet
  fhicl::ParameterSet daq_pset;
  try {
    daq_pset = pset.get<fhicl::ParameterSet>("daq");
  }
  catch (...) {
    mf::LogError(name_)
      << "Unable to find the DAQ parameters in the initialization "
      << "ParameterSet: \"" + pset.to_string() + "\".";
    return false;
  }
  fhicl::ParameterSet fr_pset;
  try {
    fr_pset = daq_pset.get<fhicl::ParameterSet>("fragment_receiver");
  }
  catch (...) {
    mf::LogError(name_)
      << "Unable to find the fragment_receiver parameters in the DAQ "
      << "initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
    return false;
  }

  // pull out the Metric part of the ParameterSet
  fhicl::ParameterSet metric_pset;
  try {
    metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
    metricMan_.initialize(metric_pset, name_ + ".");
  }
  catch (...) {
    //Okay if no metrics have been defined...
    mf::LogDebug(name_) << "Error loading metrics or no metric plugins defined.";
  }
  // create the requested CommandableFragmentGenerator
  std::string frag_gen_name = fr_pset.get<std::string>("generator", "");
  if (frag_gen_name.length() == 0) {
    mf::LogError(name_)
      << "No fragment generator (parameter name = \"generator\") was "
      << "specified in the fragment_receiver ParameterSet.  The "
      << "DAQ initialization PSet was \"" << daq_pset.to_string() << "\".";
    return false;
  }

  try {
    generator_ptr_ = artdaq::makeCommandableFragmentGenerator(frag_gen_name, fr_pset);
    generator_ptr_->SetMetricManager(&metricMan_);
  }
  catch (art::Exception& excpt) {
    mf::LogError(name_)
      << "Exception creating a CommandableFragmentGenerator of type \""
      << frag_gen_name << "\" with parameter set \"" << fr_pset.to_string()
      << "\", exception = " << excpt;
    return false;
  }
  catch (cet::exception& excpt) {
    mf::LogError(name_)
      << "Exception creating a CommandableFragmentGenerator of type \""
      << frag_gen_name << "\" with parameter set \"" << fr_pset.to_string()
      << "\", exception = " << excpt;
    return false;
  }
  catch (...) {
    mf::LogError(name_)
      << "Unknown exception creating a CommandableFragmentGenerator of type \""
      << frag_gen_name << "\" with parameter set \"" << fr_pset.to_string()
      << "\".";
    return false;
  }
  FRAGMENT_RATE_METRIC_NAME_ =
    generator_ptr_->metricsReportingInstanceName() + " Fragment Rate";
  FRAGMENT_SIZE_METRIC_NAME_ =
    generator_ptr_->metricsReportingInstanceName() + " Average Fragment Size";
  DATA_RATE_METRIC_NAME_ =
    generator_ptr_->metricsReportingInstanceName() + " Data Rate";
  INPUT_WAIT_METRIC_NAME_ =
    generator_ptr_->metricsReportingInstanceName() + " Avg Input Wait Time";
  OUTPUT_WAIT_METRIC_NAME_ =
    generator_ptr_->metricsReportingInstanceName() + " Avg Output Wait Time";
  FRAGMENTS_PER_READ_METRIC_NAME_ =
    generator_ptr_->metricsReportingInstanceName() + " Avg Frags Per Read";

  // determine the data sending parameters
  try {
    max_fragment_size_words_ = daq_pset.get<uint64_t>("max_fragment_size_words");
  }
  catch (...) {
    mf::LogError(name_)
      << "The max_fragment_size_words parameter was not specified "
      << "in the DAQ initialization PSet: \""
      << daq_pset.to_string() << "\".";
    return false;
  }
  try {mpi_buffer_count_ = fr_pset.get<size_t>("mpi_buffer_count");}
  catch (...) {
    mf::LogError(name_)
      << "The mpi_buffer_count parameter was not specified "
      << "in the fragment_receiver initialization PSet: \""
      << fr_pset.to_string() << "\".";
    return false;
  }
  try {first_evb_rank_ = fr_pset.get<size_t>("first_event_builder_rank");}
  catch (...) {
    mf::LogError(name_)
      << "The first_event_builder_rank parameter was not specified "
      << "in the fragment_receiver initialization PSet: \""
      << fr_pset.to_string() << "\".";
    return false;
  }
  try {evb_count_ = fr_pset.get<size_t>("event_builder_count");}
  catch (...) {
    mf::LogError(name_)
      << "The event_builder_count parameter was not specified "
      << "in the fragment_receiver initialization PSet: \""
      << fr_pset.to_string() << "\".";
    return false;
  }
  rt_priority_ = fr_pset.get<int>("rt_priority", 0);
  synchronous_sends_ = fr_pset.get<bool>("synchronous_sends", true);

  // fetch the monitoring parameters and create the MonitoredQuantity instances
  statsHelper_.createCollectors(fr_pset, 100, 30.0, 60.0, FRAGMENTS_PROCESSED_STAT_KEY);

  // check if we should skip the sequence ID test...
  skip_seqId_test_ = (generator_ptr_->fragmentIDs().size() > 1);

  return true;
}

bool artdaq::BoardReaderCore::start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
  stop_requested_.store(false);
  pause_requested_.store(false);

  fragment_count_ = 0;
  prev_seq_id_ = 0;
  statsHelper_.resetStatistics();

  generator_ptr_->StartCmd(id.run(), timeout, timestamp);
  run_id_ = id;
  metricMan_.do_start();

  mf::LogDebug(name_) << "Started run " << run_id_.run() << 
    ", timeout = " << timeout <<  ", timestamp = " << timestamp << std::endl;
  return true;
}

bool artdaq::BoardReaderCore::stop(uint64_t timeout, uint64_t timestamp)
{
  mf::LogDebug(name_) << "Stopping run " << run_id_.run()
                                   << " after " << fragment_count_
                                   << " fragments.";
  stop_requested_.store(true);
  generator_ptr_->StopCmd(timeout, timestamp);
  return true;
}

bool artdaq::BoardReaderCore::pause(uint64_t timeout, uint64_t timestamp)
{
  mf::LogDebug(name_) << "Pausing run " << run_id_.run()
                                   << " after " << fragment_count_
                                   << " fragments.";
  pause_requested_.store(true);
  generator_ptr_->PauseCmd(timeout, timestamp);
  return true;
}

bool artdaq::BoardReaderCore::resume(uint64_t timeout, uint64_t timestamp)
{
  mf::LogDebug(name_) << "Resuming run " << run_id_.run();
  pause_requested_.store(false);
  generator_ptr_->ResumeCmd(timeout, timestamp);
  metricMan_.do_resume();
  return true;
}

bool artdaq::BoardReaderCore::shutdown(uint64_t )
{
  generator_ptr_.reset(nullptr);
  metricMan_.shutdown();
  return true;
}

bool artdaq::BoardReaderCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
  mf::LogDebug(name_) << "soft_initialize method called with "
                                   << "ParameterSet = \"" << pset.to_string()
                                   << "\".";
  return true;
}

bool artdaq::BoardReaderCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
  mf::LogDebug(name_) << "reinitialize method called with "
                                   << "ParameterSet = \"" << pset.to_string()
                                   << "\".";
  return true;
}

size_t artdaq::BoardReaderCore::process_fragments()
{
  if (rt_priority_ > 0) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
    sched_param s_param = {};
    s_param.sched_priority = rt_priority_;
    if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param))
      mf::LogWarning(name_) << "setting realtime prioriry failed";
#pragma GCC diagnostic pop
  }

  // try-catch block here?

  // how to turn RT PRI off?
  if (rt_priority_ > 0) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
    sched_param s_param = {};
    s_param.sched_priority = rt_priority_;
    int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
    if (status != 0) {
      mf::LogError(name_)
        << "Failed to set realtime priority to " << rt_priority_
        << ", return code = " << status;
    }
#pragma GCC diagnostic pop
  }

  sender_ptr_.reset(new artdaq::SHandles(mpi_buffer_count_,
                                         max_fragment_size_words_,
                                         evb_count_,
                                         first_evb_rank_,
                                         false,
                                         synchronous_sends_));

  MPI_Barrier(local_group_comm_);

  mf::LogDebug(name_) << "Waiting for first fragment.";
  artdaq::MonitoredQuantity::TIME_POINT_T startTime;
  double delta_time;
  artdaq::FragmentPtrs frags;
  bool active = true;
  while (active) {
    startTime = artdaq::MonitoredQuantity::getCurrentTime();

    active = generator_ptr_->getNext(frags);

    delta_time=artdaq::MonitoredQuantity::getCurrentTime() - startTime;
    statsHelper_.addSample(INPUT_WAIT_STAT_KEY,delta_time);
    
    TRACE( 16, "%s::process_fragments INPUT_WAIT=%f", name_.c_str(), delta_time );

    if (! active) {break;}
    statsHelper_.addSample(FRAGMENTS_PER_READ_STAT_KEY, frags.size());

    startTime = artdaq::MonitoredQuantity::getCurrentTime();
    for (auto & fragPtr : frags) {
      artdaq::Fragment::sequence_id_t sequence_id = fragPtr->sequenceID();
      statsHelper_.addSample(FRAGMENTS_PROCESSED_STAT_KEY, fragPtr->size());

      if ((fragment_count_ % 250) == 0) {
        mf::LogDebug(name_)
          << "Sending fragment " << fragment_count_
          << " with sequence id " << sequence_id << ".";
      }

      // check for continous sequence IDs
      if (! skip_seqId_test_ && abs(sequence_id-prev_seq_id_) > 1) {
        mf::LogWarning(name_)
          << "Missing sequence IDs: current sequence ID = "
          << sequence_id << ", previous sequence ID = "
          << prev_seq_id_ << ".";
      }
      prev_seq_id_ = sequence_id;

      TRACE( 17, "%s::process_fragments seq=%lu sendFragment start", name_.c_str(), sequence_id );
      sender_ptr_->sendFragment(std::move(*fragPtr));
      TRACE( 17, "%s::process_fragments seq=%lu sendFragment done", name_.c_str(), sequence_id );
      ++fragment_count_;
      bool readyToReport = statsHelper_.readyToReport(fragment_count_);
      if (readyToReport) {
        std::string statString = buildStatisticsString_();
        mf::LogDebug(name_) << statString;
      }
      if (fragment_count_ == 1 || readyToReport) {
        mf::LogDebug(name_)
          << "Sending fragment " << fragment_count_
          << " with sequence id " << sequence_id << ".";
      }
    }
    if (statsHelper_.statsRollingWindowHasMoved()) {sendMetrics_();}
    statsHelper_.addSample(OUTPUT_WAIT_STAT_KEY,
                           artdaq::MonitoredQuantity::getCurrentTime() - startTime);
    frags.clear();
  }

  // 07-Feb-2013, KAB
  // removing this barrier so that we can stop the trigger (V1495)
  // generation and readout before stopping the readout of the other cards
  //MPI_Barrier(local_group_comm_);

  // 12-Jan-2015, KAB: moved MetricManager stop and pause commands here so
  // that they don't get called while metrics reporting is still going on.
  if (stop_requested_.load()) {metricMan_.do_stop();}
  else if (pause_requested_.load()) {metricMan_.do_pause();}

  sender_ptr_.reset(nullptr);
  return fragment_count_;
}

std::string artdaq::BoardReaderCore::report(std::string const&) const
{
  return generator_ptr_->ReportCmd();
}

std::string artdaq::BoardReaderCore::buildStatisticsString_()
{
  std::ostringstream oss;
  oss << name_ << " statistics:" << std::endl;

  double fragmentCount = 1.0;
  artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(FRAGMENTS_PROCESSED_STAT_KEY);
  if (mqPtr.get() != 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    oss << "  Fragment statistics: "
        << stats.recentSampleCount << " fragments received at "
        << stats.recentSampleRate  << " fragments/sec, effective data rate = "
        << (stats.recentValueRate * sizeof(artdaq::RawDataType)
            / 1024.0 / 1024.0) << " MB/sec, monitor window = "
        << stats.recentDuration << " sec, min::max event size = "
        << (stats.recentValueMin * sizeof(artdaq::RawDataType)
            / 1024.0 / 1024.0)
        << "::"
        << (stats.recentValueMax * sizeof(artdaq::RawDataType)
            / 1024.0 / 1024.0)
        << " MB" << std::endl;
    fragmentCount = std::max(double(stats.recentSampleCount), 1.0);
    oss << "  Average times per fragment: ";
    if (stats.recentSampleRate > 0.0) {
      oss << " elapsed time = "
          << (1.0 / stats.recentSampleRate) << " sec";
    }
  }

  // 31-Dec-2014, KAB - Just a reminder that using "fragmentCount" in the
  // denominator of the calculations below is important because the way that
  // the accumulation of these statistics is done is not fragment-by-fragment
  // but read-by-read (where each read can contain multiple fragments).

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    oss << ", input wait time = "
        << (mqPtr->recentValueSum() / fragmentCount) << " sec";
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    oss << ", output wait time = "
        << (mqPtr->recentValueSum() / fragmentCount) << " sec";
  }

  oss << std::endl << "  Fragments per read: ";
  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
  if (mqPtr.get() != 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    oss << "average = "
        << stats.recentValueAverage
        << ", min::max = "
        << stats.recentValueMin
        << "::"
        << stats.recentValueMax;
  }

  return oss.str();
}

void artdaq::BoardReaderCore::sendMetrics_()
{
  //mf::LogDebug("BoardReaderCore") << "Sending metrics " << __LINE__;
  double fragmentCount = 1.0;
  artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(FRAGMENTS_PROCESSED_STAT_KEY);
  if (mqPtr.get() != 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    fragmentCount = std::max(double(stats.recentSampleCount), 1.0);
    metricMan_.sendMetric(FRAGMENT_RATE_METRIC_NAME_,
                          stats.recentSampleRate, "fragments/sec", 1, false);
    metricMan_.sendMetric(FRAGMENT_SIZE_METRIC_NAME_,
                          (stats.recentValueAverage * sizeof(artdaq::RawDataType)
                           / 1024.0 / 1024.0), "MB/fragment", 2, false);
    metricMan_.sendMetric(DATA_RATE_METRIC_NAME_,
                          (stats.recentValueRate * sizeof(artdaq::RawDataType)
                           / 1024.0 / 1024.0), "MB/sec", 2, false);
  }

  // 31-Dec-2014, KAB - Just a reminder that using "fragmentCount" in the
  // denominator of the calculations below is important because the way that
  // the accumulation of these statistics is done is not fragment-by-fragment
  // but read-by-read (where each read can contain multiple fragments).

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric(INPUT_WAIT_METRIC_NAME_,
                          (mqPtr->recentValueSum() / fragmentCount),
                          "seconds/fragment", 3, false);
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric(OUTPUT_WAIT_METRIC_NAME_,
                          (mqPtr->recentValueSum() / fragmentCount),
                          "seconds/fragment", 3, false);
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric(FRAGMENTS_PER_READ_METRIC_NAME_,
                          mqPtr->recentValueAverage(), "fragments/read", 4, false);
  }
}
