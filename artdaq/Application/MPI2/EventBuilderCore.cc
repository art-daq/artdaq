#include "artdaq/Application/MPI2/EventBuilderCore.hh"
#include "canvas/Utilities/Exception.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "artdaq/DAQrate/EventStore.hh"
#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME "EventBuilderCore"
#include "trace.h"

#include <iomanip>

const std::string artdaq::EventBuilderCore::INPUT_FRAGMENTS_STAT_KEY("EventBuilderCoreInputFragments");
const std::string artdaq::EventBuilderCore::INPUT_WAIT_STAT_KEY("EventBuilderCoreInputWaitTime");
const std::string artdaq::EventBuilderCore::STORE_EVENT_WAIT_STAT_KEY("EventBuilderCoreStoreEventWaitTime");

/**
 * Constructor.
 */
artdaq::EventBuilderCore::EventBuilderCore(int mpi_rank, MPI_Comm local_group_comm, std::string name) :
                                                                                                      local_group_comm_(local_group_comm)
                                                                                                      , name_(name)
                                                                                                      , art_initialized_(false)
                                                                                                      , stop_requested_(false)
                                                                                                      , pause_requested_(false)
                                                                                                      , run_is_paused_(false)
                                                                                                      , processing_fragments_(false)
{
	mf::LogDebug(name_) << "Constructor";
	statsHelper_.addMonitoredQuantityName(INPUT_FRAGMENTS_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(STORE_EVENT_WAIT_STAT_KEY);
	my_rank = mpi_rank;
	metricMan = &metricMan_;
}

/**
 * Destructor.
 */
artdaq::EventBuilderCore::~EventBuilderCore()
{
	mf::LogDebug(name_) << "Destructor";
}

void artdaq::EventBuilderCore::initializeEventStore(fhicl::ParameterSet pset)
{
	if (use_art_)
	{
		artdaq::EventStore::ART_CFGSTRING_FCN* reader = &artapp_string_config;
		TRACE(36, "Creating EventStore and Starting art thread");
		event_store_ptr_.reset(new artdaq::EventStore(pset, expected_fragments_per_event_, 1,
		                                              init_string_, reader));
		TRACE(36, "Done Creating EventStore");
		art_initialized_ = true;
	}
	else
	{
		const char* dummyArgs[1]{"SimpleQueueReader"};
		artdaq::EventStore::ART_CMDLINE_FCN* reader = &artdaq::simpleQueueReaderApp;
		event_store_ptr_.reset(new artdaq::EventStore(pset, expected_fragments_per_event_, 1,
		                                              1, const_cast<char**>(dummyArgs), reader));
	}
}

/**
 * Processes the initialize request.
 */
bool artdaq::EventBuilderCore::initialize(fhicl::ParameterSet const& pset)
{
	init_string_ = pset.to_string();
	mf::LogDebug(name_) << "initialize method called with DAQ "
		<< "ParameterSet = \"" << init_string_ << "\".";

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		mf::LogError(name_)
			<< "Unable to find the DAQ parameters in the initialization "
			<< "ParameterSet: \"" + pset.to_string() + "\".";
		return false;
	}
	fhicl::ParameterSet evb_pset;
	try
	{
		evb_pset = daq_pset.get<fhicl::ParameterSet>("event_builder");
		data_pset_ = evb_pset;
	}
	catch (...)
	{
		mf::LogError(name_)
			<< "Unable to find the event_builder parameters in the DAQ "
			<< "initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
		return false;
	}
	try
	{
		expected_fragments_per_event_ =
			evb_pset.get<size_t>("expected_fragments_per_event");
	}
	catch (...)
	{
		mf::LogError(name_)
			<< "The expected_fragments_per_event parameter was not specified "
			<< "in the event_builder initialization PSet: \"" << pset.to_string()
			<< "\".";
		return false;
	}

	// other parameters
	try { use_art_ = evb_pset.get<bool>("use_art"); }
	catch (...)
	{
		mf::LogError(name_)
			<< "The use_art parameter was not specified "
			<< "in the event_builder initialization PSet: \""
			<< evb_pset.to_string() << "\".";
		return false;
	}
	inrun_recv_timeout_usec_ = evb_pset.get<size_t>("inrun_recv_timeout_usec", 100000);
	endrun_recv_timeout_usec_ = evb_pset.get<size_t>("endrun_recv_timeout_usec", 20000000);
	pause_recv_timeout_usec_ = evb_pset.get<size_t>("pause_recv_timeout_usec", 3000000);
	verbose_ = evb_pset.get<bool>("verbose", false);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(evb_pset, 100, 20.0, 60.0, INPUT_FRAGMENTS_STAT_KEY);

	// initialize the MetricManager and the names of our metrics
	std::string metricsReportingInstanceName = "EventBuilder." +
	                                           boost::lexical_cast<std::string>(my_rank);
	fhicl::ParameterSet metric_pset;
	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL

	if (metric_pset.is_empty())
	{
		mf::LogInfo(name_) << "No metric plugins appear to be defined";
	}
	try
	{
		metricMan_.initialize(metric_pset, metricsReportingInstanceName);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
		                 "Error loading metrics in EventBuilderCore::initialize()");
	}

	/* Once art has been initialized we can't tear it down or change it's
	   configuration.  We'll keep track of when we have initialized it.  Once it
	   has been initialized we need to verify that the configuration is the same
	   every subsequent time we transition through the init state.  If the
	   config changes we have to throw up our hands and bail out.
	*/
	if (art_initialized_ == false)
	{
		this->initializeEventStore(evb_pset);
		fhicl::ParameterSet tmp = pset;
		tmp.erase("daq");
		previous_pset_ = tmp;
	}
	else
	{
		fhicl::ParameterSet tmp = pset;
		tmp.erase("daq");
		if (tmp != previous_pset_)
		{
			mf::LogError(name_)
				<< "The art configuration can not be altered after art "
				<< "has been configured.";
			return false;
		}
	}

	return true;
}

bool artdaq::EventBuilderCore::start(art::RunID id)
{
	stop_requested_.store(false);
	pause_requested_.store(false);
	run_is_paused_.store(false);
	run_id_ = id;
	eod_fragments_received_ = 0;
	fragment_count_in_run_ = 0;
	statsHelper_.resetStatistics();
	flush_mutex_.lock();
	metricMan_.do_start();
	event_store_ptr_->startRun(id.run());

	logMessage_("Started run " + boost::lexical_cast<std::string>(run_id_.run()));
	return true;
}

bool artdaq::EventBuilderCore::stop()
{
	logMessage_("Stopping run " + boost::lexical_cast<std::string>(run_id_.run()) +
	            ", subrun " + boost::lexical_cast<std::string>(event_store_ptr_->subrunID()));
	bool endSucceeded;
	int attemptsToEnd;

	// 21-Jun-2013, KAB - the stop_requested_ variable must be set
	// before the flush lock so that the processFragments loop will
	// exit (after the timeout), the lock will be released (in the
	// processFragments method), and this method can continue.
	stop_requested_.store(true);

	flush_mutex_.lock();
	if (!run_is_paused_.load())
	{
		endSucceeded = false;
		attemptsToEnd = 1;
		endSucceeded = event_store_ptr_->endSubrun();
		while (!endSucceeded && attemptsToEnd < 3)
		{
			++attemptsToEnd;
			mf::LogDebug(name_) << "Retrying EventStore::endSubrun()";
			endSucceeded = event_store_ptr_->endSubrun();
		}
		if (!endSucceeded)
		{
			mf::LogError(name_)
				<< "EventStore::endSubrun in stop method failed after three tries.";
		}
	}

	endSucceeded = false;
	attemptsToEnd = 1;
	endSucceeded = event_store_ptr_->endRun();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		mf::LogDebug(name_) << "Retrying EventStore::endRun()";
		endSucceeded = event_store_ptr_->endRun();
	}
	if (!endSucceeded)
	{
		mf::LogError(name_)
			<< "EventStore::endRun in stop method failed after three tries.";
	}

	flush_mutex_.unlock();
	run_is_paused_.store(false);
	return true;
}

bool artdaq::EventBuilderCore::pause()
{
	logMessage_("Pausing run " + boost::lexical_cast<std::string>(run_id_.run()) +
	            ", subrun " + boost::lexical_cast<std::string>(event_store_ptr_->subrunID()));
	pause_requested_.store(true);
	flush_mutex_.lock();

	bool endSucceeded = false;
	int attemptsToEnd = 1;
	endSucceeded = event_store_ptr_->endSubrun();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		mf::LogDebug(name_) << "Retrying EventStore::endSubrun()";
		endSucceeded = event_store_ptr_->endSubrun();
	}
	if (!endSucceeded)
	{
		mf::LogError(name_)
			<< "EventStore::endSubrun in pause method failed after three tries.";
	}

	flush_mutex_.unlock();
	run_is_paused_.store(true);
	return true;
}

bool artdaq::EventBuilderCore::resume()
{
	logMessage_("Resuming run " + boost::lexical_cast<std::string>(run_id_.run()));
	eod_fragments_received_ = 0;
	pause_requested_.store(false);
	flush_mutex_.lock();
	metricMan_.do_start();
	event_store_ptr_->startSubrun();
	run_is_paused_.store(false);
	return true;
}

bool artdaq::EventBuilderCore::shutdown()
{
	/* We don't care about flushing data here.  The only way to transition to the
	   shutdown state is from a state where there is no data taking.  All we have
	   to do is signal the art input module that we're done taking data so that
	   it can wrap up whatever it needs to do. */
	int readerReturnValue;
	bool endSucceeded = false;
	int attemptsToEnd = 1;
	TRACE(4, "EventBuilderCore::shutdown: Calling EventStore::endOfData");
	endSucceeded = event_store_ptr_->endOfData(readerReturnValue);
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TRACE(4, "EventBuilderCore::shutdown: Retrying endOfData call");
		mf::LogDebug(name_) << "Retrying EventStore::endOfData()";
		endSucceeded = event_store_ptr_->endOfData(readerReturnValue);
	}
	TRACE(4, "EventBuilderCore::shutdown: Shutting down MetricManager");
	metricMan_.shutdown();
	TRACE(4, "EventBuilderCore::shutdown: Complete");
	return endSucceeded;
}

bool artdaq::EventBuilderCore::soft_initialize(fhicl::ParameterSet const& pset)
{
	mf::LogDebug(name_) << "soft_initialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";
	return true;
}

bool artdaq::EventBuilderCore::reinitialize(fhicl::ParameterSet const& pset)
{
	mf::LogDebug(name_) << "reinitialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";
	event_store_ptr_.reset(nullptr);
	art_initialized_ = false;
	initialize(pset);
	return true;
}

size_t artdaq::EventBuilderCore::process_fragments()
{
	processing_fragments_.store(true);
	bool process_fragments = true;
	int senderSlot;
	detail::FragCounter fragments_received;
	detail::FragCounter fragments_sent;

	receiver_ptr_.reset(new artdaq::DataReceiverManager(data_pset_));
	receiver_ptr_->start_threads();

	MPI_Barrier(local_group_comm_);

	mf::LogDebug(name_) << "Waiting for first fragment.";
	artdaq::MonitoredQuantityStats::TIME_POINT_T startTime;
	while (process_fragments)
	{
		size_t recvTimeout = inrun_recv_timeout_usec_;
		if (stop_requested_.load()) { recvTimeout = endrun_recv_timeout_usec_; }
		else if (pause_requested_.load()) { recvTimeout = pause_recv_timeout_usec_; }
		startTime = artdaq::MonitoredQuantity::getCurrentTime();
		artdaq::FragmentPtr pfragment = receiver_ptr_->recvFragment(senderSlot, recvTimeout);
		statsHelper_.addSample(INPUT_WAIT_STAT_KEY,
		                       (artdaq::MonitoredQuantity::getCurrentTime() - startTime));
		if (senderSlot == artdaq::TransferInterface::RECV_TIMEOUT)
		{
			if (stop_requested_.load() &&
			    recvTimeout == endrun_recv_timeout_usec_)
			{
				mf::LogWarning(name_)
					<< "Timeout occurred in attempt to receive data, but as a stop has been requested, will forcibly end the run.";
				event_store_ptr_->flushData();
				flush_mutex_.unlock();
				process_fragments = false;
			}
			else if (pause_requested_.load() &&
			         recvTimeout == pause_recv_timeout_usec_)
			{
				mf::LogWarning(name_)
					<< "Timeout occurred in attempt to receive data, but as a pause has been requested, will forcibly pause the run.";
				event_store_ptr_->flushData();
				flush_mutex_.unlock();
				process_fragments = false;
			}
			continue;
		}
		else if (!pfragment)
		{
			mf::LogError(name_) << "Received invalid fragment from " << senderSlot << ". This is usually the case when a timeout has occurred, but sender was not set to RECV_TIMEOUT as expected.";
			continue;
		}
		if (!receiver_ptr_->enabled_sources().count(senderSlot))
		{
			mf::LogError(name_)
				<< "Invalid senderSlot received from recvFragment: "
				<< senderSlot;
			continue;
		}
		fragments_received.incSlot(senderSlot);
		if (artdaq::Fragment::isSystemFragmentType(pfragment->type()))
		{
			mf::LogDebug(name_)
				<< "Sender slot = " << senderSlot
				<< ", fragment type = " << ((int)pfragment->type())
				<< ", sequence ID = " << pfragment->sequenceID();
		}

		++fragment_count_in_run_;
		TRACE(18, "process_fragments %lu=fragment_count_in_run_ %lu=pfragment->size()"
			, fragment_count_in_run_, pfragment->size());
		statsHelper_.addSample(INPUT_FRAGMENTS_STAT_KEY, pfragment->size());
		if (statsHelper_.readyToReport(fragment_count_in_run_))
		{
			std::string statString = buildStatisticsString_();
			logMessage_(statString);
			logMessage_("Received fragment " +
			            boost::lexical_cast<std::string>(fragment_count_in_run_) +
			            " with sequence ID " +
			            boost::lexical_cast<std::string>(pfragment->sequenceID()) +
			            " (run " +
			            boost::lexical_cast<std::string>(run_id_.run()) +
			            ", subrun " +
			            boost::lexical_cast<std::string>(event_store_ptr_->subrunID()) +
			            ").");
		}
		if (statsHelper_.statsRollingWindowHasMoved())
		{
			sendMetrics_();
			event_store_ptr_->sendMetrics();
		}

		startTime = artdaq::MonitoredQuantity::getCurrentTime();
		if (pfragment->type() != artdaq::Fragment::EndOfDataFragmentType)
		{
			artdaq::FragmentPtr rejectedFragment;
			auto seqId = pfragment->sequenceID();
			auto fragId = pfragment->fragmentID();
			bool try_again = true;
			while (try_again)
			{
				auto ret = event_store_ptr_->insert(std::move(pfragment), rejectedFragment);
				if (ret == EventStore::EventStoreInsertResult::SUCCESS)
				{
					receiver_ptr_->unsuppressAll();
					try_again = false;
				}
				else if (ret == EventStore::EventStoreInsertResult::SUCCESS_STOREFULL)
				{
					try_again = false;
				}
				else if (stop_requested_.load())
				{
					try_again = false;
					flush_mutex_.unlock();
					process_fragments = false;
					receiver_ptr_->reject_fragment(senderSlot, std::move(rejectedFragment));
					mf::LogWarning(name_)
						<< "Unable to process fragment " << fragId
						<< " in event " << seqId
						<< " because of back-pressure - forcibly ending the run.";
				}
				else if (pause_requested_.load())
				{
					try_again = false;
					flush_mutex_.unlock();
					process_fragments = false;
					receiver_ptr_->reject_fragment(senderSlot, std::move(rejectedFragment));
					mf::LogWarning(name_)
						<< "Unable to process fragment " << fragId
						<< " in event " << seqId
						<< " because of back-pressure - forcibly pausing the run.";
				}
				else if (ret == EventStore::EventStoreInsertResult::REJECT_QUEUEFULL)
				{
					pfragment = std::move(rejectedFragment);
					mf::LogWarning(name_)
						<< "Unable to process fragment " << fragId
						<< " in event " << seqId
						<< " because of back-pressure from art - retrying...";
				}
				else
				{
					try_again = false;
					receiver_ptr_->reject_fragment(senderSlot, std::move(rejectedFragment));
					mf::LogWarning(name_)
						<< "Unable to process fragment " << fragId
						<< " in event " << seqId
						<< " because the EventStore has reached the maximum number of incomplete events." << std::endl
						<< " Will retry when the EventStore is ready for new events.";
				}
			}
		}
		else
		{
			eod_fragments_received_++;
			/* We count the EOD fragment as a fragment received but the SHandles class
		   does not count it as a fragment sent which means we need to add one to
		   the total expected fragments. */
			fragments_sent.setSlot(senderSlot, *pfragment->dataBegin() + 1);
		}
		statsHelper_.addSample(STORE_EVENT_WAIT_STAT_KEY,
		                       artdaq::MonitoredQuantity::getCurrentTime() - startTime);

		/* If we've received EOD fragments from all of the BoardReaders we can
		   verify that we've also received every fragment that they have sent.  If
		   all fragments are accounted for we can flush the EventStore, unlock the
		   mutex and exit out of this thread.*/
		if (eod_fragments_received_ == receiver_ptr_->enabled_sources().size())
		{
			bool fragmentsOutstanding = false;
			for (auto& i : receiver_ptr_->enabled_sources())
			{
				if (fragments_received[i] != fragments_sent[i])
				{
					fragmentsOutstanding = true;
					break;
				}
			}

			if (!fragmentsOutstanding)
			{
				event_store_ptr_->flushData();
				flush_mutex_.unlock();
				process_fragments = false;
			}
			else
			{
				mf::LogWarning(name_) << "All EndOfData fragments received but more data expected";
			}
		}
	}

	// 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
	// processing fragments loop so that metrics correctly go to zero when
	// there is no data flowing
	metricMan_.do_stop();

	receiver_ptr_.reset(nullptr);
	processing_fragments_.store(false);
	return 0;
}

std::string artdaq::EventBuilderCore::report(std::string const& which) const
{
	if (which == "incomplete_event_count")
	{
		if (event_store_ptr_ != nullptr)
		{
			return boost::lexical_cast<std::string>(event_store_ptr_->incompleteEventCount());
		}
		else
		{
			return "-1";
		}
	}
	if (which == "event_count")
	{
		artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
			getMonitoredQuantity(STORE_EVENT_WAIT_STAT_KEY);
		if (mqPtr.get() != 0)
		{
			return boost::lexical_cast<std::string>(mqPtr->getFullSampleCount());
		}
		else
		{
			return "-1";
		}
	}

	if (which == "run_duration")
	{
		// 03-Feb-2017, ELF: if we are not processing fragments, return 0 (not adding data members)
		double duration = 0;
		if (processing_fragments_.load())
		{
			artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
				getMonitoredQuantity(STORE_EVENT_WAIT_STAT_KEY);
			if (mqPtr.get() != 0)
			{
				duration = mqPtr->getFullDuration();
			}
		}
		std::ostringstream oss;
		oss << std::fixed << std::setprecision(1) << duration;
		return oss.str();
	}

	// lots of cool stuff that we can do here
	// - report on the number of fragments received and the number
	//   of events built (in the current or previous run
	// - report on the number of incomplete events in the EventStore
	//   (if running)
	std::string tmpString = name_ + " run number = ";
	tmpString.append(boost::lexical_cast<std::string>(run_id_.run()));
	tmpString.append(". Command \"" + which + "\" is not currently supported.");
	return tmpString;
}


std::string artdaq::EventBuilderCore::buildStatisticsString_()
{
	std::ostringstream oss;
	double eventCount = 1.0;
	artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(INPUT_FRAGMENTS_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		//mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "Input statistics: "
			<< stats.recentSampleCount << " fragments received at "
			<< stats.recentSampleRate << " fragments/sec, data rate = "
			<< (stats.recentValueRate * sizeof(artdaq::RawDataType)
			    / 1024.0 / 1024.0) << " MB/sec, monitor window = "
			<< stats.recentDuration << " sec, min::max fragment size = "
			<< (stats.recentValueMin * sizeof(artdaq::RawDataType)
			    / 1024.0 / 1024.0)
			<< "::"
			<< (stats.recentValueMax * sizeof(artdaq::RawDataType)
			    / 1024.0 / 1024.0)
			<< " MB" << std::endl;
		eventCount = std::max(double(stats.recentSampleCount), 1.0);
		oss << "Average times per fragment: ";
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
				<< (1.0 / stats.recentSampleRate) << " sec";
		}
	}

	// 13-Jan-2015, KAB - Just a reminder that using "fragmentCount" in the
	// denominator of the calculations below is important so that the sum
	// of the different "average" times adds up to the overall average time
	// per fragment.  In some (but not all) cases, using recentValueAverage()
	// would be equivalent.

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", input wait time = "
			<< (mqPtr->getRecentValueSum() / eventCount) << " sec";
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(STORE_EVENT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", event store wait time = "
			<< (mqPtr->getRecentValueSum() / eventCount) << " sec";
	}

	return oss.str();
}

void artdaq::EventBuilderCore::sendMetrics_()
{
	//mf::LogDebug("EventBuilderCore") << "Sending metrics ";
	double fragmentCount = 1.0;
	artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(INPUT_FRAGMENTS_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		fragmentCount = std::max(double(stats.recentSampleCount), 1.0);
		metricMan_.sendMetric("Fragment Count",
		                      static_cast<unsigned long>(stats.fullSampleCount),
		                      "fragments", 1);
		metricMan_.sendMetric("Fragment Rate",
		                      stats.recentSampleRate, "fragments/sec", 1);
		metricMan_.sendMetric("Average Fragment Size",
		                      (stats.recentValueAverage * sizeof(artdaq::RawDataType)
		                      ), "bytes/fragment", 2);
		metricMan_.sendMetric("Data Rate",
		                      (stats.recentValueRate * sizeof(artdaq::RawDataType)
		                      ), "bytes/sec", 2);
	}

	// 13-Jan-2015, KAB - Just a reminder that using "fragmentCount" in the
	// denominator of the calculations below is important so that the sum
	// of the different "average" times adds up to the overall average time
	// per fragment.  In some (but not all) cases, using recentValueAverage()
	// would be equivalent.

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Average Input Wait Time",
		                      (mqPtr->getRecentValueSum() / fragmentCount),
		                      "seconds/fragment", 3);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(STORE_EVENT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg Event Store Wait Time",
		                      (mqPtr->getRecentValueSum() / fragmentCount),
		                      "seconds/fragment", 3);
	}
}

void artdaq::EventBuilderCore::logMessage_(std::string const& text)
{
	if (verbose_)
	{
		mf::LogInfo(name_) << text;
	}
	else
	{
		mf::LogDebug(name_) << text;
	}
}
