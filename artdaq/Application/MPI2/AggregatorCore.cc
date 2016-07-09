#include "xmlrpc-c/client_simple.hpp"
#include "artdaq/Application/MPI2/AggregatorCore.hh"
#ifdef CANVAS
#include "canvas/Utilities/Exception.h"
#else
#include "art/Utilities/Exception.h"
#endif
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "artdaq/DAQrate/EventStore.hh"
#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "tracelib.h"		// TRACE
#include <errno.h>

#include <sstream>
#include <iomanip>

#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>    
namespace BFS = boost::filesystem;

const std::string artdaq::AggregatorCore::INPUT_EVENTS_STAT_KEY("AggregatorCoreInputEvents");
const std::string artdaq::AggregatorCore::INPUT_WAIT_STAT_KEY("AggregatorCoreInputWaitTime");
const std::string artdaq::AggregatorCore::STORE_EVENT_WAIT_STAT_KEY("AggregatorCoreStoreEventWaitTime");
const std::string artdaq::AggregatorCore::SHM_COPY_TIME_STAT_KEY("AggregatorCoreShmCopyTime");
const std::string artdaq::AggregatorCore::FILE_CHECK_TIME_STAT_KEY("AggregatorCoreFileCheckTime");

/**
 * Constructor.
 */
// TODO - make global queue size configurable
artdaq::AggregatorCore::AggregatorCore(int mpi_rank, MPI_Comm local_group_comm, std::string name) :
  mpi_rank_(mpi_rank), local_group_comm_(local_group_comm), name_(name),
  art_initialized_(false),
  data_sender_count_(0), event_queue_(artdaq::getGlobalQueue(10)),
  stop_requested_(false), local_pause_requested_(false),
  processing_fragments_(false),
  system_pause_requested_(false), previous_run_duration_(-1.0),
  shm_segment_id_(-1), shm_ptr_(NULL)
{
  mf::LogDebug(name_) << "Constructor";
  stats_helper_.addMonitoredQuantityName(INPUT_EVENTS_STAT_KEY);
  stats_helper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
  stats_helper_.addMonitoredQuantityName(STORE_EVENT_WAIT_STAT_KEY);
  stats_helper_.addMonitoredQuantityName(SHM_COPY_TIME_STAT_KEY);
  stats_helper_.addMonitoredQuantityName(FILE_CHECK_TIME_STAT_KEY);
}

/**
 * Destructor.
 */
artdaq::AggregatorCore::~AggregatorCore()
{
  mf::LogDebug(name_) << "Destructor";
}

/**
 * Processes the initialize request.
 */
bool artdaq::AggregatorCore::initialize(fhicl::ParameterSet const& pset)
{
  init_string_ = pset.to_string();
  mf::LogDebug(name_) << "initialize method called with DAQ "
                      << "ParameterSet = \"" << init_string_ << "\".";

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
  fhicl::ParameterSet agg_pset;
  try {
    agg_pset = daq_pset.get<fhicl::ParameterSet>("aggregator");
  }
  catch (...) {
    mf::LogError(name_)
      << "Unable to find the aggregator parameters in the DAQ "
      << "initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
    return false;
  }

  // determine the data receiver parameters
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
  try {mpi_buffer_count_ = agg_pset.get<size_t>("mpi_buffer_count");}
  catch (...) {
    mf::LogError(name_)
      << "The  mpi_buffer_count parameter was not specified "
      << "in the aggregator initialization PSet: \""
      << agg_pset.to_string() << "\".";
    return false;
  }
  try {
    first_data_sender_rank_ = agg_pset.get<size_t>("first_event_builder_rank");
  }
  catch (...) {
    mf::LogError(name_)
      << "The first_event_builder_rank parameter was not specified "
      << "in the aggregator initialization PSet: \""
      << agg_pset.to_string() << "\".";
    return false;
  }
  try {data_sender_count_ = agg_pset.get<size_t>("event_builder_count");}
  catch (...) {
    mf::LogError(name_)
      << "The event_builder_count parameter was not specified "
      << "in the aggregator initialization PSet: \""
      << agg_pset.to_string() << "\".";
    return false;
  }
  try {
    expected_events_per_bunch_ =
      agg_pset.get<size_t>("expected_events_per_bunch");}
  catch (...) {
    mf::LogError(name_)
      << "The expected_events_per_bunch parameter was not specified "
      << "in the aggregator initialization PSet: \"" << pset.to_string()
      << "\".";
    return false;
  }

  enq_timeout_ = agg_pset.get<daqrate::seconds>("enq_timeout", 
						static_cast<daqrate::seconds>(5.0));

  // 15-Jun-2016, KAB: added ability to specify either is_data_logger or
  // is_online_monitor in the parameter set.  If neither are set in the PSet,
  // then we default to the old-style of behavior in which the first AG is the
  // data logger and the second is the online monitor.
  is_data_logger_ = false;
  is_online_monitor_ = false;
  bool agtype_was_specified = false;
  if (! agtype_was_specified) {
    try {
      is_data_logger_ = agg_pset.get<bool>("is_data_logger");
      agtype_was_specified = true;
    }
    catch (...) {} // leave agtype_was_specified set to false
  }
  if (! agtype_was_specified) {
    try {
      is_online_monitor_ = agg_pset.get<bool>("is_online_monitor");
      agtype_was_specified = true;
    }
    catch (...) {} // leave agtype_was_specified set to false
  }
  if (! agtype_was_specified) {
    if (((size_t)mpi_rank_) == (first_data_sender_rank_ + data_sender_count_)) {
      is_data_logger_ = true;
    }
    if (((size_t)mpi_rank_) == (first_data_sender_rank_ + data_sender_count_ + 1)) {
      is_online_monitor_ = true;
    }
  }
  mf::LogDebug(name_) << "Rank " << mpi_rank_
                      << ", is_data_logger  = " << is_data_logger_
                      << ", is_online_monitor = " << is_online_monitor_;

  disk_writing_directory_ = "";
  try {
    fhicl::ParameterSet output_pset =
      pset.get<fhicl::ParameterSet>("outputs");
    fhicl::ParameterSet normalout_pset =
      output_pset.get<fhicl::ParameterSet>("normalOutput");

    if (!normalout_pset.is_empty()) {
      std::string filename = normalout_pset.get<std::string>("fileName", "");
      if (filename.size() > 0) {
	size_t pos = filename.rfind("/");
	if (pos != std::string::npos) {
	  disk_writing_directory_ = filename.substr(0, pos);
	}
      } else {
	mf::LogWarning(name_) << "Problem finding \"fileName\" parameter in \"normalOutput\" RootOutput module FHiCL code";
      }
    }
  }
  catch (...) {}

  std::string xmlrpcClientString =
    agg_pset.get<std::string>("xmlrpc_client_list", "");
  if (xmlrpcClientString.size() > 0) {
    xmlrpc_client_lists_.clear();
    boost::char_separator<char> sep1(";");
    boost::tokenizer<boost::char_separator<char>>
      primaryTokens(xmlrpcClientString, sep1);
    boost::tokenizer<boost::char_separator<char>>::iterator iter1;
    boost::tokenizer<boost::char_separator<char>>::iterator
      endIter1 = primaryTokens.end();
    for (iter1 = primaryTokens.begin(); iter1 != endIter1; ++iter1) {
      boost::char_separator<char> sep2(",");
      boost::tokenizer<boost::char_separator<char>>
        secondaryTokens(*iter1, sep2);
      boost::tokenizer<boost::char_separator<char>>::iterator iter2;
      boost::tokenizer<boost::char_separator<char>>::iterator
        endIter2 = secondaryTokens.end();
      int clientGroup = -1;
      std::string url = "";
      int loopCount = 0;
      for (iter2 = secondaryTokens.begin(); iter2 != endIter2; ++iter2) {
        switch (loopCount) {
        case 0:
          url = *iter2;
          break;
        case 1:
          try {
            clientGroup = boost::lexical_cast<int>(*iter2);
          }
          catch (...) {}
          break;
        default:
          mf::LogWarning(name_)
            << "Unexpected XMLRPC client list element, index = "
            << loopCount << ", value = \"" << *iter2 << "\"";
        }
        ++loopCount;
      }
      if (clientGroup >= 0 && url.size() > 0) {
        int elementsNeeded = clientGroup+1-((int)xmlrpc_client_lists_.size());
        for (int idx = 0; idx < elementsNeeded; ++idx) {
          std::vector<std::string> tmpVec;
          xmlrpc_client_lists_.push_back(tmpVec);
        }
        xmlrpc_client_lists_[clientGroup].push_back(url);
      }
    }
  }
  double fileSizeMB = agg_pset.get<double>("file_size_MB", 0);
  file_close_threshold_bytes_ = ((size_t) fileSizeMB * 1024.0 * 1024.0);
  file_close_timeout_secs_ = agg_pset.get<time_t>("file_duration", 0);
  file_close_event_count_ = agg_pset.get<size_t>("file_event_count", 0);

  inrun_recv_timeout_usec_=agg_pset.get<size_t>("inrun_recv_timeout_usec",    100000);
  endrun_recv_timeout_usec_=agg_pset.get<size_t>("endrun_recv_timeout_usec",20000000);
  pause_recv_timeout_usec_=agg_pset.get<size_t>("pause_recv_timeout_usec",3000000);

  onmon_event_prescale_ = agg_pset.get<size_t>("onmon_event_prescale", 1);

  filesize_check_interval_seconds_ = agg_pset.get<int32_t>("filesize_check_interval_seconds", 20);
  filesize_check_interval_events_ = agg_pset.get<int32_t>("filesize_check_interval_events", 20);

  // fetch the monitoring parameters and create the MonitoredQuantity instances
  stats_helper_.createCollectors(agg_pset, 50, 20.0, 60.0, INPUT_EVENTS_STAT_KEY);

  // initialize the MetricManager and the names of our metrics
  std::string metricsReportingInstanceName = "Data Logger";
  if (! is_data_logger_) {
    metricsReportingInstanceName = "Online Monitor";
  }
  fhicl::ParameterSet metric_pset;

  try {
    metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
  } catch (...) {} // OK if there's no metrics table defined in the FHiCL                                    

  if (metric_pset.is_empty()) {
    mf::LogInfo(name_) << "No metric plugins appear to be defined";
  } else {
    try {
      metricMan_.initialize(metric_pset, metricsReportingInstanceName);
    } catch (...) {
      ExceptionHandler(ExceptionHandlerRethrow::no,
                       "Error loading metrics in AggregatorCore::initialize()");
    }
  }

  if (event_store_ptr_ == nullptr) {
    artdaq::EventStore::ART_CFGSTRING_FCN * reader = &artapp_string_config;
    size_t desired_events_per_bunch = expected_events_per_bunch_;
    if (is_online_monitor_) {
      desired_events_per_bunch = 1;
    }
    event_store_ptr_.reset(new artdaq::EventStore(agg_pset, desired_events_per_bunch, 1,
                                                  mpi_rank_, init_string_,
                                                  reader, &metricMan_));
    event_store_ptr_->setSeqIDModulus(desired_events_per_bunch);
    fhicl::ParameterSet tmp = pset;
    tmp.erase("daq");
    previous_pset_ = tmp;
  } else {
    fhicl::ParameterSet tmp = pset;
    tmp.erase("daq");
    if (tmp != previous_pset_) {
      mf::LogError(name_)
        << "The art configuration can not be altered after art "
        << "has been configured.";
      return false;
    }
  }

  return true;
}

bool artdaq::AggregatorCore::start(art::RunID id)
{
  event_count_in_run_ = 0;
  event_count_in_subrun_ = 0;
  subrun_start_time_ = time(0);
  fragment_count_to_shm_ = 0;
  stats_helper_.resetStatistics();
  previous_run_duration_ = -1.0;

  stop_requested_.store(false);
  local_pause_requested_.store(false);
  run_id_ = id;
  metricMan_.do_start();
  event_store_ptr_->startRun(run_id_.run());

  logMessage_("Started run " + boost::lexical_cast<std::string>(run_id_.run()));
  return true;
}

bool artdaq::AggregatorCore::stop()
{
  logMessage_("Stopping run " + boost::lexical_cast<std::string>(run_id_.run()) +
              ", " + boost::lexical_cast<std::string>(event_count_in_run_) +
              " events received so far.");

  /* Nothing to do here.  The aggregator we clean up after itself once it has
     received all of the EOD fragments it expects.  Higher level code will block
     until the process_fragments() thread exits. */
  stop_requested_.store(true);
  return true;
}

bool artdaq::AggregatorCore::pause()
{
  logMessage_("Pausing run " + boost::lexical_cast<std::string>(run_id_.run()) +
              ", " + boost::lexical_cast<std::string>(event_count_in_run_) +
              " events received so far.");

  /* Nothing to do here.  The aggregator we clean up after itself once it has
     received all of the EOD fragments it expects.  Higher level code will block
     until the process_fragments() thread exits. */
  local_pause_requested_.store(true);
  return true;
}

bool artdaq::AggregatorCore::resume()
{
  event_count_in_subrun_ = 0;
  subrun_start_time_ = time(0);
  local_pause_requested_.store(false);

  logMessage_("Resuming run " + boost::lexical_cast<std::string>(run_id_.run()));
  metricMan_.do_start();
  event_store_ptr_->startSubrun();
  return true;
}

bool artdaq::AggregatorCore::shutdown()
{
  int readerReturnValue;
  bool endSucceeded = false;
  int attemptsToEnd = 1;
  endSucceeded = event_store_ptr_->endOfData(readerReturnValue);
  while (! endSucceeded && attemptsToEnd < 3) {
    ++attemptsToEnd;
    mf::LogDebug(name_) << "Retrying EventStore::endOfData()";
    endSucceeded = event_store_ptr_->endOfData(readerReturnValue);
  }
  metricMan_.shutdown();
  return endSucceeded;
}

bool artdaq::AggregatorCore::soft_initialize(fhicl::ParameterSet const& pset)
{
  mf::LogDebug(name_) << "soft_initialize method called with DAQ "
                             << "ParameterSet = \"" << pset.to_string()
                             << "\".";
  return true;
}

bool artdaq::AggregatorCore::reinitialize(fhicl::ParameterSet const& pset)
{
  mf::LogDebug(name_) << "reinitialize method called with DAQ "
                             << "ParameterSet = \"" << pset.to_string()
                             << "\".";
  return true;
}

size_t artdaq::AggregatorCore::process_fragments()
{
  processing_fragments_.store(true);
  size_t true_data_sender_count = data_sender_count_;
  if (is_online_monitor_) {
    true_data_sender_count = 1;
  }

  size_t eodFragmentsReceived = 0;
  bool process_fragments = true;
  size_t senderSlot;
  std::vector<size_t> fragments_received(true_data_sender_count + first_data_sender_rank_, 0);
  std::vector<size_t> fragments_sent(true_data_sender_count + first_data_sender_rank_, 0);
  artdaq::FragmentPtr endSubRunMsg(nullptr);
  bool eodWasCopied = false;
  bool esrWasCopied = false;
  time_t last_filesize_check_time = subrun_start_time_;

  if (is_data_logger_) {
    receiver_ptr_.reset(new artdaq::RHandles(mpi_buffer_count_,
                                             max_fragment_size_words_,
                                             true_data_sender_count,
                                             first_data_sender_rank_));
    attachToSharedMemory_(false);
  }
  else {
    attachToSharedMemory_(true);
  }

  mf::LogDebug(name_) << "Waiting for first fragment.";
  artdaq::MonitoredQuantity::TIME_POINT_T startTime;
  while (process_fragments) {
    artdaq::FragmentPtr fragmentPtr(new artdaq::Fragment);

    size_t recvTimeout = inrun_recv_timeout_usec_;
    if (stop_requested_.load()) {recvTimeout = endrun_recv_timeout_usec_;}
    else if (local_pause_requested_.load()) {recvTimeout = pause_recv_timeout_usec_;}

    startTime = artdaq::MonitoredQuantity::getCurrentTime();
    if (is_data_logger_) {
      senderSlot = receiver_ptr_->recvFragment(*fragmentPtr, recvTimeout);
    }
    else if (is_online_monitor_) {
      senderSlot = receiveFragmentFromSharedMemory_(*fragmentPtr, recvTimeout);
    }
    else {
      usleep(recvTimeout);
      senderSlot = artdaq::RHandles::RECV_TIMEOUT;
    }
    stats_helper_.addSample(INPUT_WAIT_STAT_KEY,
                            (artdaq::MonitoredQuantity::getCurrentTime() - startTime));
    if (senderSlot == (size_t) MPI_ANY_SOURCE) {
      if (endSubRunMsg != nullptr) {
        mf::LogInfo(name_)
          << "The receiving of data has stopped - ending the run.";
        event_store_ptr_->flushData();
        artdaq::RawEvent_ptr subRunEvent(new artdaq::RawEvent(run_id_.run(), 1, 0));
        subRunEvent->insertFragment(std::move(endSubRunMsg));
        bool enqStatus = event_queue_.enqTimedWait(subRunEvent, enq_timeout_);
        if (! enqStatus) {
          mf::LogError(name_) << "Failed to enqueue SubRun event.";
        }
      }
      else {
        mf::LogError(name_)
          << "The receiving of data has stopped, but no endSubRun message "
          << "is available to send to art.";
      }
      process_fragments = false;
      continue;
    }
    else if (senderSlot == artdaq::RHandles::RECV_TIMEOUT) {
      if (stop_requested_.load() &&
          recvTimeout == endrun_recv_timeout_usec_) {
        if (endSubRunMsg != nullptr) {
          mf::LogWarning(name_)
            << "Stop timeout expired - forcibly ending the run.";
          event_store_ptr_->flushData();
          artdaq::RawEvent_ptr subRunEvent(new artdaq::RawEvent(run_id_.run(), 1, 0));
          subRunEvent->insertFragment(std::move(endSubRunMsg));
          bool enqStatus = event_queue_.enqTimedWait(subRunEvent, enq_timeout_);
          if (! enqStatus) {
            mf::LogError(name_) << "Failed to enqueue SubRun event.";
          }
        }
        else {
          if (event_count_in_subrun_ > 0) {
            mf::LogError(name_)
              << "Timeout receiving fragments after stop, but no endSubRun message "
              << "is available to send to art.";
          }
          else {
            std::string msg("Timeout receiving fragments after stop, but no ");
            msg.append("endSubRun message is available to send to art.");
            logMessage_(msg);
          }
        }
        process_fragments = false;
      }
      else if (local_pause_requested_.load() &&
               recvTimeout == pause_recv_timeout_usec_) {
        if (endSubRunMsg != nullptr) {
          mf::LogWarning(name_)
            << "Pause timeout expired - forcibly pausing the run.";
          event_store_ptr_->flushData();
          artdaq::RawEvent_ptr subRunEvent(new artdaq::RawEvent(run_id_.run(), 1, 0));
          subRunEvent->insertFragment(std::move(endSubRunMsg));
          bool enqStatus = event_queue_.enqTimedWait(subRunEvent, enq_timeout_);
          if (! enqStatus) {
            mf::LogError(name_) << "Failed to enqueue SubRun event.";
          }
        }
        else {
          mf::LogError(name_)
            << "Timeout receiving fragments after pause, but no endSubRun message "
            << "is available to send to art.";
        }
        process_fragments = false;
      }
      continue;
    }
    if (senderSlot >= fragments_received.size()) {
      mf::LogError(name_)
        << "Invalid senderSlot received from RHandles::recvFragment: "
        << senderSlot;
      continue;
    }
    fragments_received[senderSlot] += 1;
    if (artdaq::Fragment::isSystemFragmentType(fragmentPtr->type()) &&
        fragmentPtr->type() != artdaq::Fragment::DataFragmentType) {
      mf::LogDebug(name_)
        << "Sender slot = " << senderSlot
        << ", fragment type = " << ((int)fragmentPtr->type())
        << ", sequence ID = " << fragmentPtr->sequenceID();
    }

    // 11-Sep-2013, KAB - protect against invalid fragments
    if (fragmentPtr->type() == artdaq::Fragment::InvalidFragmentType) {
      size_t fragSize = fragmentPtr->size() * sizeof(artdaq::RawDataType);
      mf::LogError(name_) << "Fragment received with type of "
                                 << "INVALID.  Size = " << fragSize
                                 << ", sequence ID = " << fragmentPtr->sequenceID()
                                 << ", fragment ID = " << fragmentPtr->fragmentID()
                                 << ", and type = " << ((int) fragmentPtr->type());
      continue;
    }

    if (artdaq::Fragment::isUserFragmentType(fragmentPtr->type()) ||
        fragmentPtr->type() == artdaq::Fragment::DataFragmentType) {
      ++event_count_in_run_;
      ++event_count_in_subrun_;
      if (event_count_in_run_ == 1) {
        logMessage_("Received event " +
                    boost::lexical_cast<std::string>(event_count_in_run_) +
                    " with sequence id " +
                    boost::lexical_cast<std::string>(fragmentPtr->sequenceID()) +
                    ".");
      }
      stats_helper_.addSample(INPUT_EVENTS_STAT_KEY, fragmentPtr->size());
      if (stats_helper_.readyToReport(event_count_in_run_)) {
        std::string statString = buildStatisticsString_();
        logMessage_(statString);
        logMessage_("Received event " +
                    boost::lexical_cast<std::string>(event_count_in_run_) +
                    " with sequence id " +
                    boost::lexical_cast<std::string>(fragmentPtr->sequenceID()) +
                    " (run " +
                    boost::lexical_cast<std::string>(run_id_.run()) +
                    ", subrun " +
                    boost::lexical_cast<std::string>(event_store_ptr_->subrunID()) +
                    ").");
      }
    }
    if (stats_helper_.statsRollingWindowHasMoved()) {sendMetrics_();}

    startTime = artdaq::MonitoredQuantity::getCurrentTime();
    bool fragmentWasCopied = false;
    if (is_data_logger_ && (event_count_in_run_ % onmon_event_prescale_) == 0) {
      copyFragmentToSharedMemory_(fragmentWasCopied,
                                  esrWasCopied, eodWasCopied,
                                  *fragmentPtr, 0);
    }
    stats_helper_.addSample(SHM_COPY_TIME_STAT_KEY,
                            (artdaq::MonitoredQuantity::getCurrentTime() - startTime));

    //----------------------------------------------------------------------------

    artdaq::Fragment::sequence_id_t seq=fragmentPtr->sequenceID();
    TRACE( 21, "%s::process_fragments seq=%lu isLogger=%d type=%d"
	   , name_.c_str(), seq, is_data_logger_, fragmentPtr->type() );
    startTime = artdaq::MonitoredQuantity::getCurrentTime();
    if (!art_initialized_) {
      /* The init fragment should always be the first fragment out of the
         EventBuilder. */
      if (fragmentPtr->type() == artdaq::Fragment::InitFragmentType) {
        mf::LogDebug(name_) << "Init";
        if (is_data_logger_) {
          copyFragmentToSharedMemory_(fragmentWasCopied,
                                      esrWasCopied, eodWasCopied,
                                      *fragmentPtr, 500000);
        }
        artdaq::RawEvent_ptr initEvent(new artdaq::RawEvent(run_id_.run(), 1, fragmentPtr->sequenceID()));
        initEvent->insertFragment(std::move(fragmentPtr));
        bool enqStatus = event_queue_.enqTimedWait(initEvent, enq_timeout_);
        if (! enqStatus) {
          mf::LogError(name_) << "Failed to enqueue INIT event.";
        }
        art_initialized_ = true;
      }
    } else {
      /* Note that in the currently implementation of the NetMon output/input
         modules there are no EndOfRun or Shutdown fragments. */
      if (fragmentPtr->type() == artdaq::Fragment::DataFragmentType) {
        if (is_data_logger_) {
          artdaq::FragmentPtr rejectedFragment;
          bool try_again = true;
          while (try_again) {
            if (event_store_ptr_->insert(std::move(fragmentPtr), rejectedFragment)) {
              try_again = false;
            }
            else if (stop_requested_.load()) {
              try_again = false;
              process_fragments = false;
              fragmentPtr = std::move(rejectedFragment);
              mf::LogWarning("EventBuilderCore")
                << "Unable to process event " << fragmentPtr->sequenceID()
                << " because of back-pressure - forcibly ending the run.";
            }
            else if (local_pause_requested_.load()) {
              try_again = false;
              process_fragments = false;
              fragmentPtr = std::move(rejectedFragment);
              mf::LogWarning("EventBuilderCore")
                << "Unable to process event " << fragmentPtr->sequenceID()
                << " because of back-pressure - forcibly pausing the run.";
            }
            else {
              fragmentPtr = std::move(rejectedFragment);
              mf::LogWarning("EventBuilderCore")
                << "Unable to process event " << fragmentPtr->sequenceID()
                << " because of back-pressure - retrying...";
            }
          }
        }
        else {
          event_store_ptr_->insert(std::move(fragmentPtr), false);
        }
      } else if (fragmentPtr->type() == artdaq::Fragment::EndOfSubrunFragmentType) {
        if (is_data_logger_) {
          copyFragmentToSharedMemory_(fragmentWasCopied,
                                      esrWasCopied, eodWasCopied,
                                      *fragmentPtr, 1000000);
        }
        /* We inject the EndSubrun fragment after all other data has been
           received.  The SHandles and RHandles classes do not guarantee that 
           data will be received in the same order it is sent.  We'll hold on to
           this fragment and inject it once we've received all EOD fragments. */
        endSubRunMsg = std::move(fragmentPtr);
      } else if (fragmentPtr->type() == artdaq::Fragment::EndOfDataFragmentType) {
        if (is_data_logger_) {
          copyFragmentToSharedMemory_(fragmentWasCopied,
                                      esrWasCopied, eodWasCopied,
                                      *fragmentPtr, 1000000);
        }
        eodFragmentsReceived++;
        /* We count the EOD fragment as a fragment received but the SHandles class
           does not count it as a fragment sent which means we need to add one to
           the total expected fragments. */
        fragments_sent[senderSlot] = *fragmentPtr->dataBegin() + 1;
      }
    }
    float delta=artdaq::MonitoredQuantity::getCurrentTime() - startTime;
    stats_helper_.addSample(STORE_EVENT_WAIT_STAT_KEY, delta );
    TRACE( (delta>3.0)?0:22, "%s::process_fragments seq=%lu isLogger=%d delta=%f start=%f"
	   ,name_.c_str(), seq, is_data_logger_, delta, startTime );

    // 27-Sep-2013, KAB - added automatic file closing
    startTime = artdaq::MonitoredQuantity::getCurrentTime();
    if (is_data_logger_ && disk_writing_directory_.size() > 0 &&
        ! stop_requested_.load() && ! system_pause_requested_.load()) {
      bool threshold_reached = false;
      if (file_close_event_count_ > 0 &&
          event_count_in_subrun_ >= file_close_event_count_) {
        threshold_reached = true;
      }
      else {
        time_t now = time(0);
        if (file_close_timeout_secs_ > 0 &&
            (now - subrun_start_time_) >= file_close_timeout_secs_) {
          threshold_reached = true;
        }
        else {
          if (filesize_check_interval_seconds_ > 0 &&
              filesize_check_interval_events_ > 0 &&
              (now - last_filesize_check_time) >= filesize_check_interval_seconds_ &&
              (event_count_in_run_ % filesize_check_interval_events_) == 0) {
            if (file_close_threshold_bytes_ > 0 &&
                getLatestFileSize_() >= file_close_threshold_bytes_) {
              threshold_reached = true;
            }
            last_filesize_check_time = now;
          }
        }
      }
      if (threshold_reached) {
        system_pause_requested_.store(true);
        if (pause_thread_.get() != 0) {
          pause_thread_->join();
        }
        mf::LogDebug(name_) << "Starting sendPauseAndResume thread "
                                       << ", event count in subrun = "
                                       << event_count_in_subrun_;
        pause_thread_.reset(new std::thread(&AggregatorCore::sendPauseAndResume_, this));
      }
    }
    stats_helper_.addSample(FILE_CHECK_TIME_STAT_KEY,
                            (artdaq::MonitoredQuantity::getCurrentTime() - startTime));

    /* If we've received EOD fragments from all of the EventBuilders we can
       verify that we've also received every fragment that they have sent.  If
       all fragments are accounted for we can flush the EventStoreand exit out 
       of this thread.*/
    if (eodFragmentsReceived >= true_data_sender_count && endSubRunMsg != nullptr) {
      bool fragmentsOutstanding = false;
      if (is_data_logger_) {
        for (size_t i = 0; i < true_data_sender_count + first_data_sender_rank_; i++) {
          if (fragments_received[i] != fragments_sent[i]) {
            fragmentsOutstanding = true;
            break;
          }
        }
      }

      if (!fragmentsOutstanding) {
        event_store_ptr_->flushData();
        artdaq::RawEvent_ptr subRunEvent(new artdaq::RawEvent(run_id_.run(), 1, 0));
        subRunEvent->insertFragment(std::move(endSubRunMsg));
        bool enqStatus = event_queue_.enqTimedWait(subRunEvent, enq_timeout_);
        if (! enqStatus) {
          mf::LogError(name_) << "Failed to enqueue SubRun event.";
        }
        process_fragments = false;
      }
    }
  }

  logMessage_("Subrun " +
              boost::lexical_cast<std::string>(event_store_ptr_->subrunID()) +
              " in run " + boost::lexical_cast<std::string>(run_id_.run()) +
              " has ended.  There were " +
              boost::lexical_cast<std::string>(event_count_in_subrun_) +
              " events in this subrun, and there have been " +
              boost::lexical_cast<std::string>(event_count_in_run_) +
              " events so far in this run.");

  artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_EVENTS_STAT_KEY);
  if (mqPtr.get() != 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    std::ostringstream oss;
    oss << "Run " << run_id_.run() << " has an overall event rate of ";
    oss << std::fixed << std::setprecision(1) << stats.fullSampleRate;
    oss << " events/sec.";
    logMessage_(oss.str());
    previous_run_duration_ = stats.fullDuration;
  }

  // 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
  // processing fragments loop so that metrics correctly go to zero when
  // there is no data flowing
  metricMan_.do_stop();

  receiver_ptr_.reset(nullptr);
  if (is_online_monitor_) {
    detachFromSharedMemory_(true);
  }
  else {
    detachFromSharedMemory_(false);
  }
  processing_fragments_.store(false);
  return 0;
}

std::string artdaq::AggregatorCore::report(std::string const& which) const
{
  if (which == "event_count") {
    artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
      getMonitoredQuantity(INPUT_EVENTS_STAT_KEY);
    if (mqPtr.get() != 0) {
      return boost::lexical_cast<std::string>(mqPtr->fullSampleCount());
    }
    else {
      return "-1";
    }
  }

  if (which == "run_duration") {
    // 17-Jan-2014, KAB: if we are not processing fragments, return
    // the previous run duration
    double duration = previous_run_duration_;
    if (processing_fragments_.load()) {
      artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
        getMonitoredQuantity(INPUT_EVENTS_STAT_KEY);
      if (mqPtr.get() != 0) {
        duration = mqPtr->fullDuration();
      }
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << duration;
    return oss.str();
  }

  if (which == "file_size") {
    size_t latestFileSize = getLatestFileSize_();
    return boost::lexical_cast<std::string>(latestFileSize);
  }

  if (which == "subrun_number") {
    if (event_store_ptr_.get() != nullptr) {
      return boost::lexical_cast<std::string>(event_store_ptr_->subrunID());
    }
    else {
      return "-1";
    }
  }

  if (which == "incomplete_event_count") {
    if (event_store_ptr_ != nullptr) {
      return boost::lexical_cast<std::string>(event_store_ptr_->incompleteEventCount());
    }
    else {
      return "-1";
    }
  }

  // lots of cool stuff that we can do here
  // - report on the number of fragments received and the number
  //   of events built (in the current or previous run
  // - report on the number of incomplete events in the EventStore
  //   (if running)
  std::string tmpString = name_ + " run number = ";
  tmpString.append(boost::lexical_cast<std::string>(run_id_.run()));
  tmpString.append(". Command=\"" + which + "\" is not currently supported.");
  return tmpString;
}

size_t artdaq::AggregatorCore::getLatestFileSize_() const
{
  if (disk_writing_directory_.size() == 0) {
    mf::LogDebug(name_) << "Latest file size = 0 (no directory)";
    return 0;
  }
  BFS::path outputDir(disk_writing_directory_);
  BFS::directory_iterator endIter;

  std::time_t latestFileTime = 0;
  size_t latestFileSize = 0;
  if (BFS::exists(outputDir) && BFS::is_directory(outputDir)) {
    for (BFS::directory_iterator dirIter(outputDir); dirIter != endIter; ++dirIter) {
      BFS::path pathObj = dirIter->path();
      if (pathObj.filename().string().find("RootOutput") != std::string::npos &&
          pathObj.filename().string().find("root") != std::string::npos) {
        if (BFS::last_write_time(pathObj) >= latestFileTime) {
          latestFileTime = BFS::last_write_time(pathObj);
          latestFileSize = BFS::file_size(pathObj);
        }
      }
    }
  }
  time_t now = time(0);
  if ((now - latestFileTime) < 60) {
    mf::LogDebug(name_) << "Latest file size = "
                                   << latestFileSize;
    return latestFileSize;
  }
  else {
    mf::LogDebug(name_) << "Latest file size = 0 (too old)";
    return 0;
  }
}

bool artdaq::AggregatorCore::sendPauseAndResume_()
{
  xmlrpc_c::clientSimple myClient;
  mf::LogInfo(name_) << "Starting automatic pause...";
  for (size_t igrp = 0; igrp < xmlrpc_client_lists_.size(); ++igrp) {
    for (size_t idx = 0; idx < xmlrpc_client_lists_[igrp].size(); ++idx) {
      for (size_t iAttempt = 0; iAttempt < 5; ++iAttempt) {
        xmlrpc_c::value result;
        myClient.call((xmlrpc_client_lists_[igrp])[idx], "daq.pause", &result);
        std::string const resultString = xmlrpc_c::value_string(result);
        mf::LogDebug(name_) << "Pause: "
                                       << (xmlrpc_client_lists_[igrp])[idx]
                                       << " " << resultString;
        if (std::string::npos !=
            boost::algorithm::to_lower_copy(resultString).find("success")) {
          break;
        }
        else {
          sleep(2);
          mf::LogWarning(name_) << "Retrying pause command to "
                                           << (xmlrpc_client_lists_[igrp])[idx]
                                           << " (" << resultString << ")";
        }
      }
    }
  }
  mf::LogInfo(name_) << "Starting automatic resume...";
  for (int igrp = (xmlrpc_client_lists_.size()-1); igrp >= 0; --igrp) {
    for (size_t idx = 0; idx < xmlrpc_client_lists_[igrp].size(); ++idx) {
      for (size_t iAttempt = 0; iAttempt < 5; ++iAttempt) {
        xmlrpc_c::value result;
        myClient.call((xmlrpc_client_lists_[igrp])[idx], "daq.resume", &result);
        std::string const resultString = xmlrpc_c::value_string(result);
        mf::LogDebug(name_) << "Resume: "
                                       << (xmlrpc_client_lists_[igrp])[idx]
                                       << " " << resultString;
        if (std::string::npos !=
            boost::algorithm::to_lower_copy(resultString).find("success")) {
          break;
        }
        else {
          sleep(2);
          mf::LogWarning(name_) << "Retrying resume command to "
                                           << (xmlrpc_client_lists_[igrp])[idx]
                                           << " (" << resultString << ")";
        }
      }
    }
  }
  mf::LogInfo(name_) << "Done with automatic resume...";
  system_pause_requested_.store(false);
  return true;
}

void artdaq::AggregatorCore::logMessage_(std::string const& text)
{
  if (is_data_logger_) {
    mf::LogInfo(name_) << text;
  }
  else {
    mf::LogDebug(name_) << text;
  }
}

std::string artdaq::AggregatorCore::buildStatisticsString_()
{
  std::ostringstream oss;
  double eventCount = 1.0;
  artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_EVENTS_STAT_KEY);
  if (mqPtr.get() != 0) {
    //mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    oss << "Input statistics: "
        << stats.recentSampleCount << " events received at "
        << stats.recentSampleRate  << " events/sec, data rate = "
        << (stats.recentValueRate * sizeof(artdaq::RawDataType)
            / 1024.0 / 1024.0) << " MB/sec, monitor window = "
        << stats.recentDuration << " sec, min::max event size = "
        << (stats.recentValueMin * sizeof(artdaq::RawDataType)
            / 1024.0 / 1024.0)
        << "::"
        << (stats.recentValueMax * sizeof(artdaq::RawDataType)
            / 1024.0 / 1024.0)
        << " MB" << std::endl;
    eventCount = std::max(double(stats.recentSampleCount), 1.0);
    oss << "Average times per event: ";
    if (stats.recentSampleRate > 0.0) {
      oss << " elapsed time = "
          << (1.0 / stats.recentSampleRate) << " sec";
    }
  }

  // 13-Jan-2015, KAB - Just a reminder that using "eventCount" in the
  // denominator of the calculations below is important so that the sum
  // of the different "average" times adds up to the overall average time
  // per event.  In some (but not all) cases, using recentValueAverage()
  // would be equivalent.

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    oss << ", input wait time = "
        << (mqPtr->recentValueSum() / eventCount) << " sec";
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(STORE_EVENT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    oss << ", avg::max event store wait time = "
        << (stats.recentValueSum / eventCount)
        << "::" << stats.recentValueMax
        << " sec";
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(SHM_COPY_TIME_STAT_KEY);
  if (mqPtr.get() != 0) {
    oss << ", shared memory copy time = "
        << (mqPtr->recentValueSum() / eventCount) << " sec";
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(FILE_CHECK_TIME_STAT_KEY);
  if (mqPtr.get() != 0) {
    oss << ", file size test time = "
        << (mqPtr->recentValueSum() / eventCount) << " sec";
  }

  return oss.str();
}

void artdaq::AggregatorCore::sendMetrics_()
{
  //mf::LogDebug("AggregatorCore") << "Sending metrics ";
  double eventCount = 1.0;
  artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_EVENTS_STAT_KEY);
  if (mqPtr.get() != 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    eventCount = std::max(double(stats.recentSampleCount), 1.0);
    metricMan_.sendMetric("Event Rate",
                          stats.recentSampleRate, "events/sec", 1);
    metricMan_.sendMetric("Average Event Size",
                          (stats.recentValueAverage * sizeof(artdaq::RawDataType)
                           / 1024.0 / 1024.0), "MB/event", 2);
    metricMan_.sendMetric("Data Rate",
                          (stats.recentValueRate * sizeof(artdaq::RawDataType)
                           / 1024.0 / 1024.0), "MB/sec", 2);
  }

  // 13-Jan-2015, KAB - Just a reminder that using "eventCount" in the
  // denominator of the calculations below is important so that the sum
  // of the different "average" times adds up to the overall average time
  // per event.  In some (but not all) cases, using recentValueAverage()
  // would be equivalent.

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric("Average Input Wait Time",
                          (mqPtr->recentValueSum() / eventCount),
                          "seconds/event", 3);
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(STORE_EVENT_WAIT_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric("Avg art Queue Wait Time",
                          (mqPtr->recentValueSum() / eventCount),
                          "seconds/event", 3);
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(SHM_COPY_TIME_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric("Avg Shared Memory Copy Time",
                          (mqPtr->recentValueSum() / eventCount),
                          "seconds/event", 4);
  }

  mqPtr = artdaq::StatisticsCollection::getInstance().
    getMonitoredQuantity(FILE_CHECK_TIME_STAT_KEY);
  if (mqPtr.get() != 0) {
    metricMan_.sendMetric("Average File Check Time",
                          (mqPtr->recentValueSum() / eventCount),
                          "seconds/event", 4);
  }
}

void artdaq::AggregatorCore::attachToSharedMemory_(bool initialize)
{
  shm_segment_id_ = -1;
  shm_ptr_ = NULL;

  int shmKey = 0x40470000;
  char* keyChars = getenv("ARTDAQ_SHM_KEY");
  if (keyChars != nullptr) {
    std::string keyString(keyChars);
    try {
      shmKey = boost::lexical_cast<int>(keyString);
    }
    catch (...) {}
  }

  shm_segment_id_ =
    shmget(shmKey, (max_fragment_size_words_ * sizeof(artdaq::RawDataType)),
           IPC_CREAT | 0666);

  if (shm_segment_id_ > -1) {
    mf::LogDebug(name_)
      << "Created/fetched shared memory segment with ID = " << shm_segment_id_
      << " and size " << (max_fragment_size_words_ * sizeof(artdaq::RawDataType))
      << " bytes";
    shm_ptr_ = (ShmStruct*) shmat(shm_segment_id_, 0, 0);
    if (shm_ptr_ != NULL) {
      if (initialize) {
        shm_ptr_->hasFragment = 0;
      }
      mf::LogDebug(name_)
        << "Attached to shared memory segment at address 0x"
        << std::hex << shm_ptr_ << std::dec;
    }
    else {
      mf::LogError(name_) << "Failed to attach to shared memory segment "
                                 << shm_segment_id_;
    }
  }
  else {
    mf::LogError(name_) << "Failed to connect to shared memory segment"
                               << ", errno = " << errno << ".  Please check "
                               << "if a stale shared memory segment needs to "
                               << "be cleaned up. (ipcs, ipcrm -m <segId>)";
  }
}

void artdaq::AggregatorCore::
copyFragmentToSharedMemory_(bool& fragment_has_been_copied,
                            bool& esr_has_been_copied,
                            bool& eod_has_been_copied,
                            artdaq::Fragment& fragment,
                            size_t send_timeout_usec)
{
  // check if the fragment has already been copied to shared memory
  if (fragment_has_been_copied) {return;}

  // check if a fragment of this type has already been copied to shm
  size_t fragmentType = fragment.type();
  if (fragmentType == artdaq::Fragment::EndOfSubrunFragmentType &&
      esr_has_been_copied) {return;}
  if (fragmentType == artdaq::Fragment::EndOfDataFragmentType &&
      eod_has_been_copied) {return;}

  // verify that we have a shared memory segment
  if (shm_ptr_ == NULL) {return;}

  // wait for the shm to become free, if requested
  if (send_timeout_usec > 0) {
    size_t sleepTime = (send_timeout_usec / 10);
    int loopCount = 0;
    while (shm_ptr_->hasFragment == 1 && loopCount < 10) {
      if (fragmentType != artdaq::Fragment::DataFragmentType) {
        mf::LogDebug(name_) << "Trying to copy fragment of type "
                                   << fragmentType
                                   << ", loopCount = "
                                   << loopCount;
      }
      usleep(sleepTime);
      ++loopCount;
    }
  }

  // copy the fragment if the shm is available
  if (shm_ptr_->hasFragment == 0) {
    artdaq::RawDataType* fragAddr = fragment.headerAddress();
    size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);

    // 10-Sep-2013, KAB - protect against large events and
    // invalid events (and large, invalid events)
    if (fragment.type() != artdaq::Fragment::InvalidFragmentType &&
        fragSize < ((max_fragment_size_words_ *
                     sizeof(artdaq::RawDataType)) -
                    sizeof(ShmStruct))) {
      memcpy(&shm_ptr_->fragmentInnards[0], fragAddr, fragSize);
      shm_ptr_->fragmentSizeWords = fragment.size();

      fragment_has_been_copied = true;
      if (fragmentType == artdaq::Fragment::EndOfSubrunFragmentType) {
        esr_has_been_copied = true;
      }
      if (fragmentType == artdaq::Fragment::EndOfDataFragmentType) {
        eod_has_been_copied = true;
      }

      shm_ptr_->hasFragment = 1;

      ++fragment_count_to_shm_;
      if ((fragment_count_to_shm_ % 250) == 0) {
        mf::LogDebug(name_) << "Copied " << fragment_count_to_shm_
                                   << " fragments to shared memory in this run.";
      }
    }
    else {
      mf::LogWarning(name_) << "Fragment invalid for shared memory! "
                                   << "fragment address and size = "
                                   << fragAddr << " " << fragSize << " "
                                   << "sequence ID, fragment ID, and type = "
                                   << fragment.sequenceID() << " "
                                   << fragment.fragmentID() << " "
                                   << ((int) fragment.type());
    }
  }
}

size_t artdaq::AggregatorCore::
receiveFragmentFromSharedMemory_(artdaq::Fragment& fragment,
                                 size_t receiveTimeout)
{
  if (shm_ptr_ != NULL) {
    int loopCount = 0;
    size_t sleepTime = receiveTimeout / 10;
    while (shm_ptr_->hasFragment == 0 && loopCount < 10) {
      usleep(sleepTime);
      ++loopCount;
    }
    if (shm_ptr_->hasFragment == 1) {
      fragment.resize(shm_ptr_->fragmentSizeWords); // Slight overshoot on memory, resized below
      artdaq::RawDataType* fragAddr = fragment.headerAddress();
      size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);
      memcpy(fragAddr, &shm_ptr_->fragmentInnards[0], fragSize);
      shm_ptr_->hasFragment = 0;

      auto wordsOfHeaderAndMetadata = &*fragment.dataBegin() - &*fragment.headerBegin();
      fragment.resize( shm_ptr_->fragmentSizeWords - wordsOfHeaderAndMetadata);

      if (fragment.type() != artdaq::Fragment::DataFragmentType) {
        mf::LogDebug(name_)
          << "Received fragment from shared memory, type ="
          << ((int)fragment.type()) << ", sequenceID = "
          << fragment.sequenceID();
      }

      return first_data_sender_rank_;
    }
    else {
      return artdaq::RHandles::RECV_TIMEOUT;
    }
  }
  else {
    usleep(receiveTimeout);
    return artdaq::RHandles::RECV_TIMEOUT;
  }
}

void artdaq::AggregatorCore::detachFromSharedMemory_(bool destroy)
{
  if (shm_ptr_ != NULL) {
    shmdt(shm_ptr_);
    shm_ptr_ = NULL;
  }
  if (destroy && shm_segment_id_ > -1) {
    shmctl(shm_segment_id_, IPC_RMID, NULL);
  }
}
