#define TRACE_NAME "BoardReaderCore"
#include "tracemf.h"
#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/BoardReaderCore.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/Application/makeCommandableFragmentGenerator.hh"
#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include <pthread.h>
#include <sched.h>
#include <algorithm>

const std::string artdaq::BoardReaderCore::
FRAGMENTS_PROCESSED_STAT_KEY("BoardReaderCoreFragmentsProcessed");
const std::string artdaq::BoardReaderCore::
INPUT_WAIT_STAT_KEY("BoardReaderCoreInputWaitTime");
const std::string artdaq::BoardReaderCore::
BRSYNC_WAIT_STAT_KEY("BoardReaderCoreBRSyncWaitTime");
const std::string artdaq::BoardReaderCore::
OUTPUT_WAIT_STAT_KEY("BoardReaderCoreOutputWaitTime");
const std::string artdaq::BoardReaderCore::
FRAGMENTS_PER_READ_STAT_KEY("BoardReaderCoreFragmentsPerRead");

std::unique_ptr<artdaq::DataSenderManager> artdaq::BoardReaderCore::sender_ptr_ = nullptr;

artdaq::BoardReaderCore::BoardReaderCore(Commandable& parent_application) :
	parent_application_(parent_application)
	/*, local_group_comm_(local_group_comm)*/
	, generator_ptr_(nullptr)
	, stop_requested_(false)
	, pause_requested_(false)
{
	TLOG_DEBUG(app_name) << "Constructor" << TLOG_ENDL;
	statsHelper_.addMonitoredQuantityName(FRAGMENTS_PROCESSED_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(BRSYNC_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(OUTPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(FRAGMENTS_PER_READ_STAT_KEY);
	metricMan = &metricMan_;
}

artdaq::BoardReaderCore::~BoardReaderCore()
{
	TLOG_DEBUG(app_name) << "Destructor" << TLOG_ENDL;
}

bool artdaq::BoardReaderCore::initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(app_name) << "initialize method called with " << "ParameterSet = \"" << pset.to_string() << "\"." << TLOG_ENDL;

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG_ERROR(app_name)
			<< "Unable to find the DAQ parameters in the initialization "
			<< "ParameterSet: \"" + pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}
	fhicl::ParameterSet fr_pset;
	try
	{
		fr_pset = daq_pset.get<fhicl::ParameterSet>("fragment_receiver");
		data_pset_ = fr_pset;
	}
	catch (...)
	{
		TLOG_ERROR(app_name)
			<< "Unable to find the fragment_receiver parameters in the DAQ "
			<< "initialization ParameterSet: \"" + daq_pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}

	// pull out the Metric part of the ParameterSet
	fhicl::ParameterSet metric_pset;
	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL 

	if (metric_pset.is_empty())
	{
		TLOG_INFO(app_name) << "No metric plugins appear to be defined" << TLOG_ENDL;
	}
	try
	{
		metricMan_.initialize(metric_pset, app_name);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
			"Error loading metrics in BoardReaderCore::initialize()");
	}

	if (daq_pset.has_key("rank"))
	{
		if (my_rank >= 0 && daq_pset.get<int>("rank") != my_rank) {
			TLOG_WARNING(app_name) << "BoardReader rank specified at startup is different than rank specified at configure! Using rank received at configure!";
		}
		my_rank = daq_pset.get<int>("rank");
	}
	if (my_rank == -1)
	{
		TLOG_ERROR(app_name) << "BoardReader rank not specified at startup or in configuration! Aborting";
		exit(1);
	}


	// create the requested CommandableFragmentGenerator
	std::string frag_gen_name = fr_pset.get<std::string>("generator", "");
	if (frag_gen_name.length() == 0)
	{
		TLOG_ERROR(app_name)
			<< "No fragment generator (parameter name = \"generator\") was "
			<< "specified in the fragment_receiver ParameterSet.  The "
			<< "DAQ initialization PSet was \"" << daq_pset.to_string() << "\"." << TLOG_ENDL;
		return false;
	}

	try
	{
		generator_ptr_ = artdaq::makeCommandableFragmentGenerator(frag_gen_name, fr_pset);
	}
	catch (...)
	{
		std::stringstream exception_string;
		exception_string << "Exception thrown during initialization of fragment generator of type \""
			<< frag_gen_name << "\"";

		ExceptionHandler(ExceptionHandlerRethrow::no, exception_string.str());

		TLOG_DEBUG(app_name) << "FHiCL parameter set used to initialize the fragment generator which threw an exception: " << fr_pset.to_string() << TLOG_ENDL;

		return false;
	}
	metricMan_.setPrefix(generator_ptr_->metricsReportingInstanceName());

	rt_priority_ = fr_pset.get<int>("rt_priority", 0);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(fr_pset, 100, 30.0, 60.0, FRAGMENTS_PROCESSED_STAT_KEY);

	// check if we should skip the sequence ID test...
	skip_seqId_test_ = (generator_ptr_->fragmentIDs().size() > 1);

	verbose_ = fr_pset.get<bool>("verbose", true);

	return true;
}

bool artdaq::BoardReaderCore::start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	logMessage_("Starting run " + boost::lexical_cast<std::string>(id.run()));
	stop_requested_.store(false);
	pause_requested_.store(false);

	fragment_count_ = 0;
	prev_seq_id_ = 0;
	statsHelper_.resetStatistics();

	metricMan_.do_start();
	generator_ptr_->StartCmd(id.run(), timeout, timestamp);
	run_id_ = id;

	logMessage_("Completed the Start transition (Started run) for run " +
	            boost::lexical_cast<std::string>(run_id_.run()) +
	            ", timeout = " + boost::lexical_cast<std::string>(timeout) +
	            ", timestamp = " +  boost::lexical_cast<std::string>(timestamp));
	return true;
}

bool artdaq::BoardReaderCore::stop(uint64_t timeout, uint64_t timestamp)
{
	logMessage_("Stopping run " + boost::lexical_cast<std::string>(run_id_.run()) +
	            " after " + boost::lexical_cast<std::string>(fragment_count_) + " fragments.");
	stop_requested_.store(true);
	generator_ptr_->StopCmd(timeout, timestamp);
	logMessage_("Completed the Stop transition for run " + boost::lexical_cast<std::string>(run_id_.run()));
	return true;
}

bool artdaq::BoardReaderCore::pause(uint64_t timeout, uint64_t timestamp)
{
	logMessage_("Pausing run " + boost::lexical_cast<std::string>(run_id_.run()) +
	            " after " + boost::lexical_cast<std::string>(fragment_count_) + " fragments.");
	pause_requested_.store(true);
	generator_ptr_->PauseCmd(timeout, timestamp);
	logMessage_("Completed the Pause transition for run " + boost::lexical_cast<std::string>(run_id_.run()));
	return true;
}

bool artdaq::BoardReaderCore::resume(uint64_t timeout, uint64_t timestamp)
{
	logMessage_("Resuming run " + boost::lexical_cast<std::string>(run_id_.run()));
	pause_requested_.store(false);
	metricMan_.do_start();
	generator_ptr_->ResumeCmd(timeout, timestamp);
	logMessage_("Completed the Resume transition for run " + boost::lexical_cast<std::string>(run_id_.run()));
	return true;
}

bool artdaq::BoardReaderCore::shutdown(uint64_t)
{
	logMessage_("Starting Shutdown transition");
	generator_ptr_->joinThreads(); // Cleanly shut down the CommandableFragmentGenerator
	generator_ptr_.reset(nullptr);
	metricMan_.shutdown();
	logMessage_("Completed Shutdown transition");
	return true;
}

bool artdaq::BoardReaderCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(app_name) << "soft_initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return true;
}

bool artdaq::BoardReaderCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(app_name) << "reinitialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return true;
}

void artdaq::BoardReaderCore::process_fragments()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param))
			TLOG_WARNING(app_name) << "setting realtime priority failed" << TLOG_ENDL;
#pragma GCC diagnostic pop
	}

	// try-catch block here?

	// how to turn RT PRI off?
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
		if (status != 0)
		{
			TLOG_ERROR(app_name)
				<< "Failed to set realtime priority to " << rt_priority_
				<< ", return code = " << status << TLOG_ENDL;
		}
#pragma GCC diagnostic pop
	}

	TLOG_DEBUG(app_name) << "Initializing DataSenderManager. my_rank=" << my_rank << TLOG_ENDL;
	sender_ptr_.reset(new artdaq::DataSenderManager(data_pset_));

	TLOG_DEBUG(app_name) << "Waiting for first fragment." << TLOG_ENDL;
	artdaq::MonitoredQuantityStats::TIME_POINT_T startTime;
	double delta_time;
	artdaq::FragmentPtrs frags;
	bool active = true;

	while (active)
	{
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		TRACE(18, app_name + "::process_fragments getNext start");
		active = generator_ptr_->getNext(frags);
		TRACE(18, app_name + "::process_fragments getNext done (active=%i)", active);
		// 08-May-2015, KAB & JCF: if the generator getNext() method returns false
		// (which indicates that the data flow has stopped) *and* the reason that
		// it has stopped is because there was an exception that wasn't handled by
		// the experiment-specific FragmentGenerator class, we move to the
		// InRunError state so that external observers (e.g. RunControl or
		// DAQInterface) can see that there was a problem.
		if (!active && generator_ptr_ && generator_ptr_->exception())
		{
			parent_application_.in_run_failure();
		}

		delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
		statsHelper_.addSample(INPUT_WAIT_STAT_KEY, delta_time);

		TLOG_ARB(16, app_name) << "process_fragments INPUT_WAIT=" << std::to_string(delta_time) << TLOG_ENDL;

		if (!active) { break; }
		statsHelper_.addSample(FRAGMENTS_PER_READ_STAT_KEY, frags.size());

		for (auto& fragPtr : frags)
		{
			if (!fragPtr.get())
			{
				TLOG_WARNING(app_name) << "Encountered a bad fragment pointer in fragment " << fragment_count_ << ". "
					<< "This is most likely caused by a problem with the Fragment Generator!" << TLOG_ENDL;
				continue;
			}
			artdaq::Fragment::sequence_id_t sequence_id = fragPtr->sequenceID();
			statsHelper_.addSample(FRAGMENTS_PROCESSED_STAT_KEY, fragPtr->size());

			if ((fragment_count_ % 250) == 0)
			{
				TLOG_DEBUG(app_name)
					<< "Sending fragment " << fragment_count_
					<< " (%250) with sequence id " << sequence_id << "." << TLOG_ENDL;
			}

			// check for continous sequence IDs
			if (!skip_seqId_test_ && abs(static_cast<int64_t>(sequence_id) - static_cast<int64_t>(prev_seq_id_)) > 1)
			{
				TLOG_WARNING(app_name)
					<< "Missing sequence IDs: current sequence ID = "
					<< sequence_id << ", previous sequence ID = "
					<< prev_seq_id_ << "." << TLOG_ENDL;
			}
			prev_seq_id_ = sequence_id;

			startTime = artdaq::MonitoredQuantity::getCurrentTime();
			TLOG_ARB(17, app_name) << "process_fragments seq=" << std::to_string(sequence_id) << " sendFragment start" << TLOG_ENDL;
			auto res = sender_ptr_->sendFragment(std::move(*fragPtr));
			TLOG_ARB(17, app_name) << "process_fragments seq=" << std::to_string(sequence_id) << " sendFragment done (res=" << res << ")" << TLOG_ENDL;
			++fragment_count_;
			statsHelper_.addSample(OUTPUT_WAIT_STAT_KEY,
				artdaq::MonitoredQuantity::getCurrentTime() - startTime);

			bool readyToReport = statsHelper_.readyToReport(fragment_count_);
			if (readyToReport)
			{
				std::string statString = buildStatisticsString_();
				TLOG_DEBUG(app_name) << statString << TLOG_ENDL;
			}
			if (fragment_count_ == 1 || readyToReport)
			{
				TLOG_DEBUG(app_name)
					<< "Sending fragment " << fragment_count_
					<< " with sequence id " << sequence_id << "." << TLOG_ENDL;
			}
		}
		if (statsHelper_.statsRollingWindowHasMoved()) { sendMetrics_(); }
		frags.clear();
	}

	// 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
	// processing fragments loop so that metrics correctly go to zero when
	// there is no data flowing
	metricMan_.do_stop();

	sender_ptr_.reset(nullptr);
}

std::string artdaq::BoardReaderCore::report(std::string const& which) const
{
	std::string resultString;

	// pass the request to the FragmentGenerator instance, if it's available
	if (generator_ptr_.get() != 0)
	{
		resultString = generator_ptr_->ReportCmd(which);
		if (resultString.length() > 0) { return resultString; }
	}

	// handle the request at this level, if we can
	// --> nothing here yet

	// if we haven't been able to come up with any report so far, say so
	std::string tmpString = app_name + " run number = ";
	tmpString.append(boost::lexical_cast<std::string>(run_id_.run()));
	tmpString.append(". Command=\"" + which + "\" is not currently supported.");
	return tmpString;
}

bool artdaq::BoardReaderCore::metaCommand(std::string const& command, std::string const& arg)
{
	TLOG_DEBUG(app_name) << "metaCommand method called with "
		<< "command = \"" << command << "\""
		<< ", arg = \"" << arg << "\""
		<< "." << TLOG_ENDL;

	if (generator_ptr_) return generator_ptr_->metaCommand(command, arg);

	return true;
}

std::string artdaq::BoardReaderCore::buildStatisticsString_()
{
	std::ostringstream oss;
	oss << app_name << " statistics:" << std::endl;

	double fragmentCount = 1.0;
	artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(FRAGMENTS_PROCESSED_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Fragment statistics: "
			<< stats.recentSampleCount << " fragments received at "
			<< stats.recentSampleRate << " fragments/sec, effective data rate = "
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
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
				<< (1.0 / stats.recentSampleRate) << " sec";
		}
	}

	// 31-Dec-2014, KAB - Just a reminder that using "fragmentCount" in the
	// denominator of the calculations below is important because the way that
	// the accumulation of these statistics is done is not fragment-by-fragment
	// but read-by-read (where each read can contain multiple fragments).
	// 29-Aug-2016, KAB - BRSYNC_WAIT and OUTPUT_WAIT are now done fragment-by-
	// fragment, but we'll leave the calculation the same. (The alternative
	// would be to use recentValueAverage().)

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", input wait time = "
			<< (mqPtr->getRecentValueSum() / fragmentCount) << " sec";
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(BRSYNC_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", BRsync wait time = "
			<< (mqPtr->getRecentValueSum() / fragmentCount) << " sec";
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", output wait time = "
			<< (mqPtr->getRecentValueSum() / fragmentCount) << " sec";
	}

	oss << std::endl << "  Fragments per read: ";
	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		artdaq::MonitoredQuantityStats stats;
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
	//TLOG_DEBUG("BoardReaderCore") << "Sending metrics " << __LINE__ << TLOG_ENDL;
	double fragmentCount = 1.0;
	artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(FRAGMENTS_PROCESSED_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		fragmentCount = std::max(double(stats.recentSampleCount), 1.0);
		metricMan_.sendMetric("Fragment Count", static_cast<unsigned long>(stats.fullSampleCount), "fragments", 1, MetricMode::LastPoint);
		metricMan_.sendMetric("Fragment Rate", stats.recentSampleRate, "fragments/sec", 1, MetricMode::Average);
		metricMan_.sendMetric("Average Fragment Size", (stats.recentValueAverage * sizeof(artdaq::RawDataType)), "bytes/fragment", 2, MetricMode::Average);
		metricMan_.sendMetric("Data Rate", (stats.recentValueRate * sizeof(artdaq::RawDataType)), "bytes/sec", 2, MetricMode::Average);
	}

	// 31-Dec-2014, KAB - Just a reminder that using "fragmentCount" in the
	// denominator of the calculations below is important because the way that
	// the accumulation of these statistics is done is not fragment-by-fragment
	// but read-by-read (where each read can contain multiple fragments).
	// 29-Aug-2016, KAB - BRSYNC_WAIT and OUTPUT_WAIT are now done fragment-by-
	// fragment, but we'll leave the calculation the same. (The alternative
	// would be to use recentValueAverage().)

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg Input Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(BRSYNC_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg BoardReader Sync Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg Output Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg Frags Per Read", mqPtr->getRecentValueAverage(), "fragments/read", 4, MetricMode::Average);
	}
}

void artdaq::BoardReaderCore::logMessage_(std::string const& text)
{
	if (verbose_)
	{
		TLOG_INFO(app_name) << text << TLOG_ENDL;
	}
	else
	{
		TLOG_DEBUG(app_name) << text << TLOG_ENDL;
	}
}
