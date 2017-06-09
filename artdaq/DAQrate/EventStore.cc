#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq/DAQrate/EventStore.hh"
#include <utility>
#include <cstring>
#include <dlfcn.h>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>

#include "cetlib_except/exception.h"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq/DAQdata/TCPConnect.hh"

namespace artdaq
{
	const std::string EventStore::EVENT_RATE_STAT_KEY("EventStoreEventRate");
	const std::string EventStore::INCOMPLETE_EVENT_STAT_KEY("EventStoreIncompleteEvents");

	EventStore::EventStore(const fhicl::ParameterSet& pset, size_t num_fragments_per_event, run_id_t run,
						   size_t event_queue_depth, size_t max_incomplete_event_count)
		: num_fragments_per_event_(num_fragments_per_event)
		, max_queue_size_(pset.get<size_t>("event_queue_depth", event_queue_depth))
		, max_incomplete_count_(pset.get<size_t>("max_incomplete_events", max_incomplete_event_count))
		, run_id_(run)
		, subrun_id_(0)
		, events_()
		, queue_(getGlobalQueue(max_queue_size_))
		, requests_(pset)
		, reader_thread_launch_time_(std::chrono::steady_clock::now())
		, seqIDModulus_(1)
		, lastFlushedSeqID_(0)
		, highestSeqIDSeen_(0)
		, enq_timeout_(pset.get<double>("event_queue_wait_time", 5.0))
		, enq_check_count_(pset.get<size_t>("event_queue_check_count", 5000))
		, printSummaryStats_(pset.get<bool>("print_event_store_stats", false))
		, incomplete_event_report_interval_ms_(pset.get<int>("incomplete_event_report_interval_ms", -1))
		, last_incomplete_event_report_time_(std::chrono::steady_clock::now())
		, art_thread_wait_ms_(pset.get<int>("art_thread_wait_ms", 4000))
	{
		TLOG_DEBUG("EventStore") << "EventStore CONSTRUCTOR" << TLOG_ENDL;
		initStatistics_();
		TRACE(12, "artdaq::EventStore::EventStore ctor - reader_thread_ initialized");
	}

	EventStore::EventStore(const fhicl::ParameterSet& pset,
						   size_t num_fragments_per_event,
						   run_id_t run,
						   int argc,
						   char* argv[],
						   ART_CMDLINE_FCN* reader)
		: EventStore(pset, num_fragments_per_event, run, 50, 50)
	{
		reader_thread_ = (std::async(std::launch::async, reader, argc, argv));
	}

	EventStore::EventStore(const fhicl::ParameterSet& pset,
						   size_t num_fragments_per_event,
						   run_id_t run,
						   const std::string& configString,
						   ART_CFGSTRING_FCN* reader)
		: EventStore(pset, num_fragments_per_event, run, 20, 20)
	{
		reader_thread_ = (std::async(std::launch::async, reader, configString));
	}

	EventStore::~EventStore()
	{
		TLOG_DEBUG("EventStore") << "Shutting down EventStore" << TLOG_ENDL;
		if (printSummaryStats_)
		{
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
		if (pfrag->sequenceID() > highestSeqIDSeen_)
		{
			highestSeqIDSeen_ = pfrag->sequenceID();

			// Get the timestamp of this fragment, in experiment-defined clocks
			Fragment::timestamp_t timestamp = pfrag->timestamp();

			// Send a request to the board readers!
			requests_.AddRequest(highestSeqIDSeen_, timestamp);
		}

		// When we're in the "EndOfRun" condition, send requests for EVERY fragment inserted!
		// This helps to make sure that all BoardReaders get the EndOfRun request and send all their data.
		if (requests_.GetRequestMode() == detail::RequestMessageMode::EndOfRun)
		{
			requests_.SendRequest();
		}
		Fragment::sequence_id_t sequence_id = ((pfrag->sequenceID() - (1 + lastFlushedSeqID_)) / seqIDModulus_) + 1;
		TRACE(13, "EventStore::insert seq=%lu fragID=%d id=%d lastFlushed=%lu seqIDMod=%d seq=%lu"
			  , pfrag->sequenceID(), pfrag->fragmentID(), my_rank, lastFlushedSeqID_, seqIDModulus_, sequence_id);


		// Find if the right event id is already known to events_ and, if so, where
		// it is.
		EventMap::iterator loc = events_.lower_bound(sequence_id);

		if (loc == events_.end() || events_.key_comp()(sequence_id, loc->first))
		{
			// We don't have an event with this id; create one and insert it at loc,
			// and ajust loc to point to the newly inserted event.
			RawEvent_ptr newevent(new RawEvent(run_id_, subrun_id_, pfrag->sequenceID()));
			loc =
				events_.insert(loc, EventMap::value_type(sequence_id, newevent));
		}

		// Now insert the fragment into the event we have located.
		loc->second->insertFragment(std::move(pfrag));
		if (loc->second->numFragments() == num_fragments_per_event_)
		{
			// This RawEvent is complete; capture it, remove it from the
			// map, report on statistics, and put the shared pointer onto
			// the event queue.
			RawEvent_ptr complete_event(loc->second);
			complete_event->markComplete();

			events_.erase(loc);

			requests_.RemoveRequest(sequence_id);
			
			// 13-Dec-2012, KAB - this monitoring needs to come before
			// the enqueueing of the event lest it be empty by the
			// time that we ask for the word count.
			MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
				getMonitoredQuantity(EVENT_RATE_STAT_KEY);
			if (mqPtr.get() != 0)
			{
				mqPtr->addSample(complete_event->wordCount());
			}
			TRACE(14, "EventStore::insert seq=%lu enqTimedWait start", sequence_id);
			bool enqSuccess = queue_.enqTimedWait(complete_event, enq_timeout_);
			TRACE(enqSuccess ? 14 : 0, "EventStore::insert seq=%lu enqTimedWait complete", sequence_id);
			if (!enqSuccess)
			{
				//TRACE_CNTL( "modeM", 0 );
				if (printWarningWhenFragmentIsDropped)
				{
					TLOG_WARNING("EventStore") << "Enqueueing event " << sequence_id
						<< " FAILED, queue size = "
						<< queue_.size() <<
						"; apparently no events were removed from this process's queue during the " << std::to_string(enq_timeout_.count())
						<< "-second timeout period" << TLOG_ENDL;
				}
				else
				{
					TLOG_DEBUG("EventStore") << "Enqueueing event " << sequence_id
						<< " FAILED, queue size = "
						<< queue_.size() <<
						"; apparently no events were removed from this process's queue during the " << std::to_string(enq_timeout_.count())
						<< "-second timeout period" << TLOG_ENDL;
				}
			}
			else
			{
				requests_.SendRoutingToken(1);
			}
		}
		MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
			getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
		if (mqPtr.get() != 0)
		{
			mqPtr->addSample(events_.size());
		}
	}

	EventStore::EventStoreInsertResult EventStore::insert(FragmentPtr pfrag, FragmentPtr& rejectedFragment)
	{
		// Test whether this fragment can be safely accepted. If we accept
		// it, and it completes an event, then we want to be sure that it
		// can be pushed onto the event queue.  If not, we return it and
		// let the caller know that we didn't accept it.
		TRACE(12, "EventStore: Testing if queue is full");
		if (queue_.full())
		{
			size_t sleepTime = 1000000 * (enq_timeout_.count() / enq_check_count_);
			TRACE(12, "EventStore: sleepTime is %lu.", sleepTime);
			size_t loopCount = 0;
			while (loopCount < enq_check_count_ && queue_.full())
			{
				++loopCount;
				usleep(sleepTime);
			}
			if (queue_.full())
			{
				rejectedFragment = std::move(pfrag);
				return EventStoreInsertResult::REJECT_QUEUEFULL;
			}
		}
		TRACE(12, "EventStore: Testing if there's room in the EventStore");
		auto incomplete_full = events_.size() >= max_incomplete_count_;
		if (incomplete_full)
		{
			EventMap::iterator loc = events_.lower_bound(pfrag->sequenceID());

			if (loc == events_.end() || events_.key_comp()(pfrag->sequenceID(), loc->first))
			{
				rejectedFragment = std::move(pfrag);
				return EventStoreInsertResult::REJECT_STOREFULL;
			}
		}

		TRACE(12, "EventStore: Performing insert");
		insert(std::move(pfrag));
		return incomplete_full ? EventStoreInsertResult::SUCCESS_STOREFULL : EventStoreInsertResult::SUCCESS;
	}

	bool
		EventStore::endOfData(int& readerReturnValue)
	{
		TLOG_DEBUG("EventStore") << "EventStore::endOfData" << TLOG_ENDL;
		RawEvent_ptr end_of_data(nullptr);
		TRACE(4, "EventStore::endOfData: Enqueuing end_of_data event");
		bool enqSuccess = queue_.enqTimedWait(end_of_data, enq_timeout_);
		if (!enqSuccess)
		{
			return false;
		}
		TRACE(4, "EventStore::endOfData: Getting return code from art thread");
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
		TLOG_DEBUG("EventStore") << "Flushing " << initialStoreSize
			<< " stale events from the EventStore." << TLOG_ENDL;
		EventMap::iterator loc;
		std::vector<sequence_id_t> flushList;
		for (loc = events_.begin(); loc != events_.end(); ++loc)
		{
			RawEvent_ptr complete_event(loc->second);
			MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
				getMonitoredQuantity(EVENT_RATE_STAT_KEY);
			if (mqPtr.get() != 0)
			{
				mqPtr->addSample(complete_event->wordCount());
			}
			enqSuccess = queue_.enqTimedWait(complete_event, enq_timeout_);
			if (!enqSuccess)
			{
				break;
			}
			else
			{
				flushList.push_back(loc->first);
			}
		}
		for (size_t idx = 0; idx < flushList.size(); ++idx)
		{
			events_.erase(flushList[idx]);
		}
		TLOG_DEBUG("EventStore") << "Done flushing " << flushList.size()
			<< " stale events from the EventStore." << TLOG_ENDL;

		lastFlushedSeqID_ = highestSeqIDSeen_;
		return (flushList.size() >= initialStoreSize);
	}

	void EventStore::startRun(run_id_t runID)
	{
		if (!queue_.queueReaderIsReady())
		{
			TLOG_WARNING("EventStore") << "Run start requested, but the art thread is not yet ready, waiting up to " << art_thread_wait_ms_ << " msec..." << TLOG_ENDL;
			while (!queue_.queueReaderIsReady() && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - reader_thread_launch_time_).count() < art_thread_wait_ms_)
			{
				usleep(1000); // wait 1 ms
			}
			if (queue_.queueReaderIsReady())
			{
				auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(queue_.getReadyTime() - reader_thread_launch_time_).count();
				TLOG_INFO("EventStore") << "art initialization took (roughly) " << std::setw(4) << std::to_string(dur) << " ms." << TLOG_ENDL;
			}
			else
			{
				auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - reader_thread_launch_time_).count();
				TLOG_ERROR("EventStore") << "art thread still not ready after " << dur << " ms. Continuing to start..." << TLOG_ENDL;
			}
		}
		run_id_ = runID;
		subrun_id_ = 1;
		lastFlushedSeqID_ = 0;
		highestSeqIDSeen_ = 0;
		requests_.SendRoutingToken(max_queue_size_);
		TLOG_DEBUG("EventStore") << "Starting run " << run_id_
			<< ", max queue size = "
			<< max_queue_size_
			<< ", queue capacity = "
			<< queue_.capacity()
			<< ", queue size = "
			<< queue_.size() << TLOG_ENDL;
		if (metricMan)
		{
			double runSubrun = run_id_ + ((double)subrun_id_ / 10000);
			metricMan->sendMetric("Run Number", runSubrun, "Run:Subrun", 1, false);
		}
	}

	void EventStore::startSubrun()
	{
		++subrun_id_;
		if (metricMan)
		{
			double runSubrun = run_id_ + ((double)subrun_id_ / 10000);
			metricMan->sendMetric("Run Number", runSubrun, "Run:Subrun", 1, false);
		}
	}

	bool EventStore::endRun()
	{
		RawEvent_ptr endOfRunEvent(new RawEvent(run_id_, subrun_id_, 0));
		std::unique_ptr<artdaq::Fragment>
			endOfRunFrag(new
						 Fragment(static_cast<size_t>
						 (ceil(sizeof(my_rank) /
							   static_cast<double>(sizeof(Fragment::value_type))))));

		endOfRunFrag->setSystemType(Fragment::EndOfRunFragmentType);
		*endOfRunFrag->dataBegin() = my_rank;
		endOfRunEvent->insertFragment(std::move(endOfRunFrag));

		return queue_.enqTimedWait(endOfRunEvent, enq_timeout_);
	}

	bool EventStore::endSubrun()
	{
		RawEvent_ptr endOfSubrunEvent(new RawEvent(run_id_, subrun_id_, 0));
		std::unique_ptr<artdaq::Fragment>
			endOfSubrunFrag(new
							Fragment(static_cast<size_t>
							(ceil(sizeof(my_rank) /
								  static_cast<double>(sizeof(Fragment::value_type))))));

		endOfSubrunFrag->setSystemType(Fragment::EndOfSubrunFragmentType);
		*endOfSubrunFrag->dataBegin() = my_rank;
		endOfSubrunEvent->insertFragment(std::move(endOfSubrunFrag));

		return queue_.enqTimedWait(endOfSubrunEvent, enq_timeout_);
	}

	void
		EventStore::initStatistics_()
	{
		MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
			getMonitoredQuantity(EVENT_RATE_STAT_KEY);
		if (mqPtr.get() == 0)
		{
			mqPtr.reset(new MonitoredQuantity(3.0, 300.0));
			StatisticsCollection::getInstance().
				addMonitoredQuantity(EVENT_RATE_STAT_KEY, mqPtr);
		}
		mqPtr->reset();

		mqPtr = StatisticsCollection::getInstance().
			getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
		if (mqPtr.get() == 0)
		{
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
		if (mqPtr.get() != 0)
		{
			std::ostringstream oss;
			oss << EVENT_RATE_STAT_KEY << "_" << std::setfill('0') << std::setw(4) << run_id_
				<< "_" << std::setfill('0') << std::setw(4) << my_rank << ".txt";
			std::string filename = oss.str();
			std::ofstream outStream(filename.c_str());
			mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
			artdaq::MonitoredQuantityStats stats;
			mqPtr->getStats(stats);
			outStream << "EventStore rank " << my_rank << ": events processed = "
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
			for (int idx = 0; idx < (int)stats.recentBinnedDurations.size(); ++idx)
			{
				if (stats.recentBinnedDurations[idx] > 0.0)
				{
					foundTheStart = true;
				}
				if (foundTheStart)
				{
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
		if (mqPtr.get() != 0)
		{
			std::ostringstream oss;
			oss << INCOMPLETE_EVENT_STAT_KEY << "_" << std::setfill('0')
				<< std::setw(4) << run_id_
				<< "_" << std::setfill('0') << std::setw(4) << my_rank << ".txt";
			std::string filename = oss.str();
			std::ofstream outStream(filename.c_str());
			mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
			artdaq::MonitoredQuantityStats stats;
			mqPtr->getStats(stats);
			outStream << "EventStore rank " << my_rank << ": fragments processed = "
				<< stats.fullSampleCount << " at " << stats.fullSampleRate
				<< " fragments/sec, average incomplete event count = "
				<< stats.fullValueAverage << " duration = "
				<< stats.fullDuration << " sec" << std::endl
				<< "    minimum incomplete event count = "
				<< stats.fullValueMin << ", maximum incomplete event count = "
				<< stats.fullValueMax << std::endl;
			bool foundTheStart = false;
			for (int idx = 0; idx < (int)stats.recentBinnedDurations.size(); ++idx)
			{
				if (stats.recentBinnedDurations[idx] > 0.0)
				{
					foundTheStart = true;
				}
				if (foundTheStart && stats.recentBinnedSampleCounts[idx] > 0.0)
				{
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
	
	void
		EventStore::sendMetrics()
	{
		if (metricMan)
		{
			metricMan->sendMetric("Incomplete Event Count", events_.size(),
								  "events", 1);
		}
		if (incomplete_event_report_interval_ms_ > 0 && events_.size())
		{
			if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_incomplete_event_report_time_).count() < incomplete_event_report_interval_ms_) return;
			last_incomplete_event_report_time_ = std::chrono::steady_clock::now();
			std::ostringstream oss;
			oss << "Incomplete Events (" << num_fragments_per_event_ << "): ";
			for (auto& ev : events_)
			{
				oss << ev.first << " (" << ev.second->numFragments() << "), ";
			}
			TLOG_DEBUG("EventStore") << oss.str() << TLOG_ENDL;
		}
	}
}
