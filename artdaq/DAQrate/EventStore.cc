
#include "artdaq/DAQrate/EventStore.hh"
#include <utility>
#include <cstring>
#include <dlfcn.h>
#include <iomanip>
#include <fstream>
#include <sstream>

#include "cetlib/exception.h"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "artdaq/DAQrate/Utils.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "tracelib.h"

// jbk note about performance measurement collection -
// We should no longer need this "Perf" performance measurement.
// The event store is used in applications that do not use MPI,
// and the Perf performance measurement collector requires MPI
// to be initialized.
#if 0
#include "artdaq/DAQrate/Perf.hh"
#endif

using namespace std;

namespace artdaq {
  const std::string EventStore::EVENT_RATE_STAT_KEY("EventStoreEventRate");
  const std::string EventStore::INCOMPLETE_EVENT_STAT_KEY("EventStoreIncompleteEvents");

  EventStore::EventStore(size_t num_fragments_per_event,
                         run_id_t run,
                         int store_id,
                         int argc,
                         char * argv[],
                         ART_CMDLINE_FCN * reader,
                         bool printSummaryStats) :
    id_(store_id),
    num_fragments_per_event_(num_fragments_per_event),
    max_queue_size_(50),
    run_id_(run),
    subrun_id_(0),
    events_(),
    queue_(getGlobalQueue(max_queue_size_)),
    reader_thread_(std::async(std::launch::async, reader, argc, argv)),
    seqIDModulus_(1),
    lastFlushedSeqID_(0),
    highestSeqIDSeen_(0),
    enq_timeout_(5.0),
    printSummaryStats_(printSummaryStats)
  {
    initStatistics_();
    TRACE( 12, "artdaq::EventStore::EventStore ctor - reader_thread_ initialized" );
  }

  EventStore::EventStore(size_t num_fragments_per_event,
                         run_id_t run,
                         int store_id,
                         const std::string& configString,
                         ART_CFGSTRING_FCN * reader,
                         bool printSummaryStats) :
    id_(store_id),
    num_fragments_per_event_(num_fragments_per_event),
    max_queue_size_(50),
    run_id_(run),
    subrun_id_(0),
    events_(),
    queue_(getGlobalQueue(max_queue_size_)),
    reader_thread_(std::async(std::launch::async, reader, configString)),
    seqIDModulus_(1),
    lastFlushedSeqID_(0),
    highestSeqIDSeen_(0),
    enq_timeout_(5.0),
    printSummaryStats_(printSummaryStats)
  {
    initStatistics_();
  }

  EventStore::EventStore(size_t num_fragments_per_event,
                         run_id_t run,
                         int store_id,
                         int argc,
                         char * argv[],
                         ART_CMDLINE_FCN * reader,
                         size_t max_art_queue_size,
                         double enq_timeout_sec,
                         bool printSummaryStats) :
    id_(store_id),
    num_fragments_per_event_(num_fragments_per_event),
    max_queue_size_(max_art_queue_size),
    run_id_(run),
    subrun_id_(0),
    events_(),
    queue_(getGlobalQueue(max_queue_size_)),
    reader_thread_(std::async(std::launch::async, reader, argc, argv)),
    seqIDModulus_(1),
    lastFlushedSeqID_(0),
    highestSeqIDSeen_(0),
    enq_timeout_(enq_timeout_sec),
    printSummaryStats_(printSummaryStats)
  {
    initStatistics_();
  }

  EventStore::EventStore(size_t num_fragments_per_event,
                         run_id_t run,
                         int store_id,
                         const std::string& configString,
                         ART_CFGSTRING_FCN * reader,
                         size_t max_art_queue_size,
                         double enq_timeout_sec,
                         bool printSummaryStats) :
    id_(store_id),
    num_fragments_per_event_(num_fragments_per_event),
    max_queue_size_(max_art_queue_size),
    run_id_(run),
    subrun_id_(0),
    events_(),
    queue_(getGlobalQueue(max_queue_size_)),
    reader_thread_(std::async(std::launch::async, reader, configString)),
    seqIDModulus_(1),
    lastFlushedSeqID_(0),
    highestSeqIDSeen_(0),
    enq_timeout_(enq_timeout_sec),
    printSummaryStats_(printSummaryStats)
  {
    initStatistics_();
  }

  EventStore::~EventStore()
  {
    if (printSummaryStats_) {
      reportStatistics_();
    }
  }

  void EventStore::insert(FragmentPtr pfrag,
                          bool printWarningWhenFragmentIsDropped)
  {
    // We should never get a null pointer, nor should we get a
    // Fragment without a good fragment ID.
    assert(pfrag != nullptr);
    assert(pfrag->fragmentID() != Fragment::InvalidFragmentID);

    // find the event being built and put the fragment into it,
    // start new event if not already present
    // if the event is complete, delete it and report timing

    // The sequenceID is expected to be correct in the incoming fragment.
    // The EventStore will divide it by the seqIDModulus to support the use case
    // of the aggregator which needs to bunch groups of serialized events with
    // continuous sequence IDs together.
    if (pfrag->sequenceID() > highestSeqIDSeen_) {
      highestSeqIDSeen_ = pfrag->sequenceID();
    }
    Fragment::sequence_id_t sequence_id = ((pfrag->sequenceID() - (1 + lastFlushedSeqID_)) / seqIDModulus_) + 1;
    TRACE( 13, "EventStore::insert seq=%lu fragID=%d id=%d lastFlushed=%lu seqIDMod=%d seq=%lu"
	  , pfrag->sequenceID(), pfrag->fragmentID(), id_, lastFlushedSeqID_, seqIDModulus_, sequence_id );

    // Find if the right event id is already known to events_ and, if so, where
    // it is.
    EventMap::iterator loc = events_.lower_bound(sequence_id);

    if (loc == events_.end() || events_.key_comp()(sequence_id, loc->first)) {
      // We don't have an event with this id; create one and insert it at loc,
      // and ajust loc to point to the newly inserted event.
      RawEvent_ptr newevent(new RawEvent(run_id_, subrun_id_, pfrag->sequenceID()));
      loc =
        events_.insert(loc, EventMap::value_type(sequence_id, newevent));
    }

    // Now insert the fragment into the event we have located.
    loc->second->insertFragment(std::move(pfrag));
    if (loc->second->numFragments() == num_fragments_per_event_) {
      // This RawEvent is complete; capture it, remove it from the
      // map, report on statistics, and put the shared pointer onto
      // the event queue.
      RawEvent_ptr complete_event(loc->second);
      complete_event->markComplete();
#if 0
      // jbk - see note at top of file
      PerfWriteEvent(EventMeas::END, sequence_id);
#endif

      events_.erase(loc);
      // 13-Dec-2012, KAB - this monitoring needs to come before
      // the enqueueing of the event lest it be empty by the
      // time that we ask for the word count.
      MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
        getMonitoredQuantity(EVENT_RATE_STAT_KEY);
      if (mqPtr.get() != 0) {
        mqPtr->addSample(complete_event->wordCount());
      }
      TRACE( 14, "EventStore::insert seq=%lu enqTimedWait start", sequence_id );
      bool enqSuccess = queue_.enqTimedWait(complete_event, enq_timeout_);
      TRACE( enqSuccess?14:0, "EventStore::insert seq=%lu enqTimedWait complete", sequence_id );
      if (! enqSuccess) {
	  //TRACE_CNTL( "modeM", 0 );
        if (printWarningWhenFragmentIsDropped) {
          mf::LogWarning("EventStore") << "Enqueueing event " << sequence_id
                                       << " FAILED, queue size = "
                                       << queue_.size();
        }
        else {
          mf::LogDebug("EventStore") << "Enqueueing event " << sequence_id
                                     << " FAILED, queue size = "
                                     << queue_.size();
        }
      }
    }
    MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
      getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
    if (mqPtr.get() != 0) {
      mqPtr->addSample(events_.size());
    }
  }

  bool EventStore::insert(FragmentPtr pfrag, FragmentPtr& rejectedFragment)
  {
    // Test whether this fragment can be safely accepted. If we accept
    // it, and it completes an event, then we want to be sure that it
    // can be pushed onto the event queue.  If not, we return it and
    // let the caller know that we didn't accept it.
    TRACE(12, "EventStore: Testing if queue is full");
    if (queue_.full()) {
      size_t fragSize = pfrag->size();
      size_t sleepTime = 10 * fragSize * (enq_timeout_.count() / 10.0);
      TRACE(12, "EventStore: sleepTime is %lu and pfrag->size() is %lu.",sleepTime, fragSize);
      int loopCount = 0;
      while (loopCount < 10 && queue_.full()) {
        ++loopCount;
        usleep(sleepTime);
      }
      if (queue_.full()) {
        rejectedFragment = std::move(pfrag);
        return false;
      }
    }

    TRACE(12, "EventStore: Performing insert");
    insert(std::move(pfrag));
    return true;
  }

  bool
  EventStore::endOfData(int& readerReturnValue)
  {
    RawEvent_ptr end_of_data(nullptr);
    bool enqSuccess = queue_.enqTimedWait(end_of_data, enq_timeout_);
    if (! enqSuccess) {
      return false;
    }
    readerReturnValue = reader_thread_.get();
    return true;
  }

  void EventStore::setSeqIDModulus(unsigned int seqIDModulus)
  {
    seqIDModulus_ = seqIDModulus;
  }

  bool EventStore::flushData()
  {
    bool enqSuccess;
    size_t initialStoreSize = events_.size();
    mf::LogDebug("EventStore") << "Flushing " << initialStoreSize
                               << " stale events from the EventStore.";
    EventMap::iterator loc;
    std::vector<sequence_id_t> flushList;
    for (loc = events_.begin(); loc != events_.end(); ++loc) {
      RawEvent_ptr complete_event(loc->second);
      MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
        getMonitoredQuantity(EVENT_RATE_STAT_KEY);
      if (mqPtr.get() != 0) {
        mqPtr->addSample(complete_event->wordCount());
      }
      enqSuccess = queue_.enqTimedWait(complete_event, enq_timeout_);
      if (! enqSuccess) {
        break;
      }
      else {
        flushList.push_back(loc->first);
      }
    }
    for (size_t idx=0; idx < flushList.size(); ++idx) {
      events_.erase(flushList[idx]);
    }
    mf::LogDebug("EventStore") << "Done flushing " << flushList.size()
                               << " stale events from the EventStore.";

    lastFlushedSeqID_ = highestSeqIDSeen_;
    return (flushList.size() >= initialStoreSize);
  }

  void EventStore::startRun(run_id_t runID)
  {
    run_id_ = runID;
    subrun_id_ = 1;
    lastFlushedSeqID_ = 0;
    highestSeqIDSeen_ = 0;
    mf::LogDebug("EventStore") << "Starting run " << run_id_
                               << ", max queue size = "
                               << max_queue_size_
                               << ", queue capacity = "
                               << queue_.capacity()
                               << ", queue size = "
                               << queue_.size();
  }

  void EventStore::startSubrun()
  {
    ++subrun_id_;
  }

  bool EventStore::endRun()
  {
    RawEvent_ptr endOfRunEvent(new RawEvent(run_id_, subrun_id_, 0));
    std::unique_ptr<artdaq::Fragment>
      endOfRunFrag(new
                   Fragment(static_cast<size_t>
                            (ceil(sizeof(id_) /
                                  static_cast<double>(sizeof(Fragment::value_type))))));

    endOfRunFrag->setSystemType(Fragment::EndOfRunFragmentType);
    *endOfRunFrag->dataBegin() = id_;
    endOfRunEvent->insertFragment(std::move(endOfRunFrag));

    return queue_.enqTimedWait(endOfRunEvent, enq_timeout_);
  }

  bool EventStore::endSubrun()
  {
    RawEvent_ptr endOfSubrunEvent(new RawEvent(run_id_, subrun_id_, 0));
    std::unique_ptr<artdaq::Fragment>
      endOfSubrunFrag(new
                      Fragment(static_cast<size_t>
                               (ceil(sizeof(id_) /
                                     static_cast<double>(sizeof(Fragment::value_type))))));

    endOfSubrunFrag->setSystemType(Fragment::EndOfSubrunFragmentType);
    *endOfSubrunFrag->dataBegin() = id_;
    endOfSubrunEvent->insertFragment(std::move(endOfSubrunFrag));

    return queue_.enqTimedWait(endOfSubrunEvent, enq_timeout_);
  }

  void
  EventStore::initStatistics_()
  {
    MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
      getMonitoredQuantity(EVENT_RATE_STAT_KEY);
    if (mqPtr.get() == 0) {
      mqPtr.reset(new MonitoredQuantity(3.0, 300.0));
      StatisticsCollection::getInstance().
        addMonitoredQuantity(EVENT_RATE_STAT_KEY, mqPtr);
    }
    mqPtr->reset();

    mqPtr = StatisticsCollection::getInstance().
      getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
    if (mqPtr.get() == 0) {
      mqPtr.reset(new MonitoredQuantity(3.0, 300.0));
      StatisticsCollection::getInstance().
        addMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY, mqPtr);
    }
    mqPtr->reset();
  }

  void
  EventStore::reportStatistics_()
  {
    MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
      getMonitoredQuantity(EVENT_RATE_STAT_KEY);
    if (mqPtr.get() != 0) {
      ostringstream oss;
      oss << EVENT_RATE_STAT_KEY << "_" << setfill('0') << setw(4) << run_id_
          << "_" << setfill('0') << setw(4) << id_ << ".txt";
      std::string filename = oss.str();
      ofstream outStream(filename.c_str());
      mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
      artdaq::MonitoredQuantity::Stats stats;
      mqPtr->getStats(stats);
      outStream << "EventStore rank " << id_ << ": events processed = "
                << stats.fullSampleCount << " at " << stats.fullSampleRate
                << " events/sec, data rate = "
                << (stats.fullValueRate * sizeof(RawDataType)
                    / 1024.0 / 1024.0) << " MB/sec, duration = "
                << stats.fullDuration << " sec" << std::endl
                << "    minimum event size = "
                << (stats.fullValueMin * sizeof(RawDataType)
                    / 1024.0 / 1024.0)
                << " MB, maximum event size = "
                << (stats.fullValueMax * sizeof(RawDataType)
                    / 1024.0 / 1024.0)
                << " MB" << std::endl;
      bool foundTheStart = false;
      for (int idx = 0; idx < (int) stats.recentBinnedDurations.size(); ++idx) {
        if (stats.recentBinnedDurations[idx] > 0.0) {
          foundTheStart = true;
        }
        if (foundTheStart) {
          outStream << "  " << std::fixed << std::setprecision(3)
                    << stats.recentBinnedEndTimes[idx]
                    << ": " << stats.recentBinnedSampleCounts[idx]
                    << " events at "
                    << (stats.recentBinnedSampleCounts[idx] /
                        stats.recentBinnedDurations[idx])
                    << " events/sec, data rate = "
                    << (stats.recentBinnedValueSums[idx] *
                        sizeof(RawDataType) / 1024.0 / 1024.0 /
                        stats.recentBinnedDurations[idx])
                    << " MB/sec, bin size = "
                    << stats.recentBinnedDurations[idx]
                    << " sec" << std::endl;
        }
      }
      outStream.close();
    }

    mqPtr = StatisticsCollection::getInstance().
      getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
    if (mqPtr.get() != 0) {
      ostringstream oss;
      oss << INCOMPLETE_EVENT_STAT_KEY << "_" << setfill('0')
          << setw(4) << run_id_
          << "_" << setfill('0') << setw(4) << id_ << ".txt";
      std::string filename = oss.str();
      ofstream outStream(filename.c_str());
      mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
      artdaq::MonitoredQuantity::Stats stats;
      mqPtr->getStats(stats);
      outStream << "EventStore rank " << id_ << ": fragments processed = "
                << stats.fullSampleCount << " at " << stats.fullSampleRate
                << " fragments/sec, average incomplete event count = "
                << stats.fullValueAverage << " duration = "
                << stats.fullDuration << " sec" << std::endl
                << "    minimum incomplete event count = "
                << stats.fullValueMin << ", maximum incomplete event count = "
                << stats.fullValueMax << std::endl;
      bool foundTheStart = false;
      for (int idx = 0; idx < (int) stats.recentBinnedDurations.size(); ++idx) {
        if (stats.recentBinnedDurations[idx] > 0.0) {
          foundTheStart = true;
        }
        if (foundTheStart && stats.recentBinnedSampleCounts[idx] > 0.0) {
          outStream << "  " << std::fixed << std::setprecision(3)
                    << stats.recentBinnedEndTimes[idx]
                    << ": " << stats.recentBinnedSampleCounts[idx]
                    << " fragments at "
                    << (stats.recentBinnedSampleCounts[idx] /
                        stats.recentBinnedDurations[idx])
                    << " fragments/sec, average incomplete event count = "
                    << (stats.recentBinnedValueSums[idx] /
                        stats.recentBinnedSampleCounts[idx])
                    << ", bin size = "
                    << stats.recentBinnedDurations[idx]
                    << " sec" << std::endl;
        }
      }
      outStream << "Incomplete count now = " << events_.size() << std::endl;
      outStream.close();
    }
  }
}
