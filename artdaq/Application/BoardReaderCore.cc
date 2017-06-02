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

#define TRACE_NAME "BoardReaderCore"

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

artdaq::BoardReaderCore::BoardReaderCore(Commandable& parent_application,
										 int rank, std::string name) :
																					  parent_application_(parent_application)
																					  /*, local_group_comm_(local_group_comm)*/
																					  , generator_ptr_(nullptr)
																					  , name_(name)
																					  , stop_requested_(false)
																					  , pause_requested_(false)
{
	TLOG_DEBUG(name_) << "Constructor" << TLOG_ENDL;
	statsHelper_.addMonitoredQuantityName(FRAGMENTS_PROCESSED_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(BRSYNC_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(OUTPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(FRAGMENTS_PER_READ_STAT_KEY);
	metricMan = &metricMan_;
	my_rank = rank;
}

artdaq::BoardReaderCore::~BoardReaderCore()
{
	TLOG_DEBUG(name_) << "Destructor" << TLOG_ENDL;
}

bool artdaq::BoardReaderCore::initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG_ERROR(name_)
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
		TLOG_ERROR(name_)
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
		TLOG_INFO(name_) << "No metric plugins appear to be defined" << TLOG_ENDL;
	}
	try
	{
		metricMan_.initialize(metric_pset, name_);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
						 "Error loading metrics in BoardReaderCore::initialize()");
	}

	// create the requested CommandableFragmentGenerator
	std::string frag_gen_name = fr_pset.get<std::string>("generator", "");
	if (frag_gen_name.length() == 0)
	{
		TLOG_ERROR(name_)
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

		TLOG_DEBUG(name_) << "FHiCL parameter set used to initialize the fragment generator which threw an exception: " << fr_pset.to_string() << TLOG_ENDL;

		return false;
	}
	metricMan_.setPrefix(generator_ptr_->metricsReportingInstanceName());

	rt_priority_ = fr_pset.get<int>("rt_priority", 0);
	/* ELF 5/10/2017 Removing in favor of DataReceiverManager source suppression logic
	mpi_sync_fragment_interval_ = fr_pset.get<int>("mpi_sync_interval", 0);
	if (mpi_sync_fragment_interval_ > 0)
	{
		mpi_sync_wait_threshold_fraction_ = fr_pset.get<double>("mpi_sync_wait_threshold", 0.5);
		mpi_sync_wait_threshold_count_ = mpi_sync_fragment_interval_ * mpi_sync_wait_threshold_fraction_;
		if (mpi_sync_wait_threshold_count_ >= mpi_sync_fragment_interval_)
		{
			TLOG_WARNING(name_) << "The calculated mpi_sync wait threshold "
				<< "(" << mpi_sync_wait_threshold_count_ << " fragments) "
				<< "is too large, setting it to "
				<< (mpi_sync_fragment_interval_ - 1) << "." << TLOG_ENDL;
			mpi_sync_wait_threshold_count_ = mpi_sync_fragment_interval_ - 1;
		}
		if (mpi_sync_wait_threshold_count_ < 0)
		{
			TLOG_WARNING(name_) << "The calculated mpi_sync wait threshold "
				<< "(" << mpi_sync_wait_threshold_count_ << " fragments) "
				<< "is too small, setting it to zero." << TLOG_ENDL;
			mpi_sync_wait_threshold_count_ = 0;
		}
		mpi_sync_wait_interval_usec_ = fr_pset.get<size_t>("mpi_sync_wait_interval_usec", 100);
		mpi_sync_wait_log_level_ = fr_pset.get<int>("mpi_sync_wait_log_level", 2);
		mpi_sync_wait_log_interval_sec_ = fr_pset.get<int>("mpi_sync_wait_log_interval_sec", 10);
	}
	else
	{
		mpi_sync_wait_threshold_fraction_ = 0.0;
		mpi_sync_wait_threshold_count_ = 0;
		mpi_sync_wait_interval_usec_ = 1000000;
		mpi_sync_wait_log_level_ = 0;
		mpi_sync_wait_log_interval_sec_ = 10;
	}
	TLOG_DEBUG(name_)
		<< "mpi_sync_fragment_interval is " << mpi_sync_fragment_interval_
		<< ", mpi_sync_wait_threshold_fraction is " << mpi_sync_wait_threshold_fraction_
		<< ", mpi_sync_wait_threshold_count is " << mpi_sync_wait_threshold_count_
		<< ", mpi_sync_wait_interval_usec is " << mpi_sync_wait_interval_usec_
		<< ", mpi_sync_wait_log_level is " << mpi_sync_wait_log_level_
		<< ", mpi_sync_wait_log_interval_sec is " << mpi_sync_wait_log_interval_sec_ << TLOG_ENDL;
		*/
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

	metricMan_.do_start();
	generator_ptr_->StartCmd(id.run(), timeout, timestamp);
	run_id_ = id;

	TLOG_DEBUG(name_) << "Started run " << run_id_.run() <<
		", timeout = " << timeout << ", timestamp = " << timestamp << TLOG_ENDL;
	return true;
}

bool artdaq::BoardReaderCore::stop(uint64_t timeout, uint64_t timestamp)
{
	TLOG_DEBUG(name_) << "Stopping run " << run_id_.run()
		<< " after " << fragment_count_
		<< " fragments." << TLOG_ENDL;
	stop_requested_.store(true);
	generator_ptr_->StopCmd(timeout, timestamp);
	return true;
}

bool artdaq::BoardReaderCore::pause(uint64_t timeout, uint64_t timestamp)
{
	TLOG_DEBUG(name_) << "Pausing run " << run_id_.run()
		<< " after " << fragment_count_
		<< " fragments." << TLOG_ENDL;
	pause_requested_.store(true);
	generator_ptr_->PauseCmd(timeout, timestamp);
	return true;
}

bool artdaq::BoardReaderCore::resume(uint64_t timeout, uint64_t timestamp)
{
	TLOG_DEBUG(name_) << "Resuming run " << run_id_.run() << TLOG_ENDL;
	pause_requested_.store(false);
	metricMan_.do_start();
	generator_ptr_->ResumeCmd(timeout, timestamp);
	return true;
}

bool artdaq::BoardReaderCore::shutdown(uint64_t)
{
	generator_ptr_.reset(nullptr);
	metricMan_.shutdown();
	return true;
}

bool artdaq::BoardReaderCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "soft_initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return true;
}

bool artdaq::BoardReaderCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "reinitialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return true;
}

size_t artdaq::BoardReaderCore::process_fragments()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param))
			TLOG_WARNING(name_) << "setting realtime priority failed" << TLOG_ENDL;
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
			TLOG_ERROR(name_)
				<< "Failed to set realtime priority to " << rt_priority_
				<< ", return code = " << status << TLOG_ENDL;
		}
#pragma GCC diagnostic pop
	}

	TLOG_DEBUG(name_) << "Initializing DataSenderManager. my_rank=" << my_rank << TLOG_ENDL;
	sender_ptr_.reset(new artdaq::DataSenderManager(data_pset_));

	//MPI_Barrier(local_group_comm_);

	TLOG_DEBUG(name_) << "Waiting for first fragment." << TLOG_ENDL;
	artdaq::MonitoredQuantityStats::TIME_POINT_T startTime;
	double delta_time;
	artdaq::FragmentPtrs frags;
	bool active = true;
	//MPI_Request mpi_request;
	//bool barrier_is_pending = false;
	while (active)
	{
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		active = generator_ptr_->getNext(frags);
		// 08-May-2015, KAB & JCF: if the generator getNext() method returns false
		// (which indicates that the data flow has stopped) *and* the reason that
		// it has stopped is because there was an exception that wasn't handled by
		// the experiment-specific FragmentGenerator class, we move to the
		// InRunError state so that external observers (e.g. RunControl or
		// DAQInterface) can see that there was a problem.
		if (!active && generator_ptr_->exception())
		{
			parent_application_.in_run_failure();
		}

		delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
		statsHelper_.addSample(INPUT_WAIT_STAT_KEY, delta_time);

		TRACE(16, "%s::process_fragments INPUT_WAIT=%f", name_.c_str(), delta_time);

		if (!active) { break; }
		statsHelper_.addSample(FRAGMENTS_PER_READ_STAT_KEY, frags.size());

		for (auto& fragPtr : frags)
		{
			if (!fragPtr.get())
			{
				TLOG_WARNING(name_) << "Encountered a bad fragment pointer in fragment " << fragment_count_ << ". "
					<< "This is most likely caused by a problem with the Fragment Generator!" << TLOG_ENDL;
				continue;
			}
			artdaq::Fragment::sequence_id_t sequence_id = fragPtr->sequenceID();
			statsHelper_.addSample(FRAGMENTS_PROCESSED_STAT_KEY, fragPtr->size());

			if ((fragment_count_ % 250) == 0)
			{
				TLOG_DEBUG(name_)
					<< "Sending fragment " << fragment_count_
					<< " with sequence id " << sequence_id << "." << TLOG_ENDL;
			}

			/* ELF 5/10/2017 Removing in favor of DataReceiverManager source suppression logic
			startTime = artdaq::MonitoredQuantity::getCurrentTime();
			// 10-Sep-2015, KAB - added non-blocking synchronization between
			// BoardReader processes.  Ibarrier is called every N fragments
			// by each BoardReader, but each BR is allowed to continue processing
			// fragments until a specified threshold of additional fragments is
			// reached.  Once that threshold is reached, and one or more of the
			// other BoardReaders haven't called Ibarrier, we wait.
			if (mpi_sync_fragment_interval_ > 0 && fragment_count_ > 0 &&
				(fragment_count_ % mpi_sync_fragment_interval_) == 0)
			{
				TRACE(4, "BoardReaderCore: Entering MPI Barrier");
				MPI_Ibarrier(local_group_comm_, &mpi_request);
				barrier_is_pending = true;
			}
			if (barrier_is_pending)
			{
				MPI_Status mpi_status;
				int test_flag;
				int retcode = MPI_Test(&mpi_request, &test_flag, &mpi_status);
				if (retcode != MPI_SUCCESS)
				{
					TLOG_ERROR(name_)
						<< "MPI_Test for Ibarrier completion failed with return code "
						<< retcode << TLOG_ENDL;
				}

				if (test_flag != 0)
				{
					barrier_is_pending = false;
				}
				else
				{
					int tmpVal = (fragment_count_ % mpi_sync_fragment_interval_);
					if (tmpVal >= mpi_sync_wait_threshold_count_)
					{
						int report_interval = mpi_sync_wait_log_interval_sec_;
						time_t last_report_time = time(0);
						while (test_flag == 0 && !stop_requested_.load())
						{
							usleep(mpi_sync_wait_interval_usec_);
							retcode = MPI_Test(&mpi_request, &test_flag, &mpi_status);
							if (retcode != MPI_SUCCESS || test_flag == 0)
							{
								time_t now = time(0);
								if ((now - last_report_time) >= report_interval)
								{
									if (retcode != MPI_SUCCESS)
									{
										TLOG_ERROR(name_)
											<< "MPI_Test for Ibarrier completion failed with return code "
											<< retcode << TLOG_ENDL;
									}
									else
									{
										if (mpi_sync_wait_log_level_ == 2)
										{
											TLOG_WARNING(name_)
												<< "Waiting for one or more BoardReaders to catch up "
												<< "so that the sending of data fragments is reasonably "
												<< "well synchronized (fragment count is currently "
												<< fragment_count_
												<< "). If this situation persists, it may indicate that "
												<< "the data flow from one or more BoardReaders has "
												<< "stopped, possibly because of a problem reading out "
												<< "the associated hardware component(s)." << TLOG_ENDL;
										}
										else if (mpi_sync_wait_log_level_ == 3)
										{
											TLOG_ERROR(name_)
												<< "Waiting for one or more BoardReaders to catch up "
												<< "so that the sending of data fragments is reasonably "
												<< "well synchronized (fragment count is currently "
												<< fragment_count_
												<< "). If this situation persists, it may indicate that "
												<< "the data flow from one or more BoardReaders has "
												<< "stopped, possibly because of a problem reading out "
												<< "the associated hardware component(s)." << TLOG_ENDL;
										}
									}
									last_report_time = now;
									report_interval += mpi_sync_wait_log_interval_sec_;
								}
							}
						}
						if (test_flag != 0)
						{
							barrier_is_pending = false;
						}
					}
				}
			}
			statsHelper_.addSample(BRSYNC_WAIT_STAT_KEY,
								   artdaq::MonitoredQuantity::getCurrentTime() - startTime);
			*/

			// check for continous sequence IDs
			if (!skip_seqId_test_ && abs(sequence_id - prev_seq_id_) > 1)
			{
				TLOG_WARNING(name_)
					<< "Missing sequence IDs: current sequence ID = "
					<< sequence_id << ", previous sequence ID = "
					<< prev_seq_id_ << "." << TLOG_ENDL;
			}
			prev_seq_id_ = sequence_id;

			startTime = artdaq::MonitoredQuantity::getCurrentTime();
			TRACE(17, "%s::process_fragments seq=%lu sendFragment start", name_.c_str(), sequence_id);
			auto res = sender_ptr_->sendFragment(std::move(*fragPtr));
			TRACE(17, "%s::process_fragments seq=%lu sendFragment done (res=%i)", name_.c_str(), sequence_id,res);
			++fragment_count_;
			statsHelper_.addSample(OUTPUT_WAIT_STAT_KEY,
								   artdaq::MonitoredQuantity::getCurrentTime() - startTime);

			bool readyToReport = statsHelper_.readyToReport(fragment_count_);
			if (readyToReport)
			{
				std::string statString = buildStatisticsString_();
				TLOG_DEBUG(name_) << statString << TLOG_ENDL;
			}
			if (fragment_count_ == 1 || readyToReport)
			{
				TLOG_DEBUG(name_)
					<< "Sending fragment " << fragment_count_
					<< " with sequence id " << sequence_id << "." << TLOG_ENDL;
			}
		}
		if (statsHelper_.statsRollingWindowHasMoved()) { sendMetrics_(); }
		frags.clear();
	}

	// 07-Feb-2013, KAB
	// removing this barrier so that we can stop the trigger (V1495)
	// generation and readout before stopping the readout of the other cards
	//MPI_Barrier(local_group_comm_);

	// 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
	// processing fragments loop so that metrics correctly go to zero when
	// there is no data flowing
	metricMan_.do_stop();

	sender_ptr_.reset(nullptr);
	return fragment_count_;
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
	std::string tmpString = name_ + " run number = ";
	tmpString.append(boost::lexical_cast<std::string>(run_id_.run()));
	tmpString.append(". Command=\"" + which + "\" is not currently supported.");
	return tmpString;
}

std::string artdaq::BoardReaderCore::buildStatisticsString_()
{
	std::ostringstream oss;
	oss << name_ << " statistics:" << std::endl;

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
		metricMan_.sendMetric("Avg Input Wait Time",
							  (mqPtr->getRecentValueSum() / fragmentCount),
							  "seconds/fragment", 3, false);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(BRSYNC_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg BoardReader Sync Wait Time",
							  (mqPtr->getRecentValueSum() / fragmentCount),
							  "seconds/fragment", 3, false);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg Output Wait Time",
							  (mqPtr->getRecentValueSum() / fragmentCount),
							  "seconds/fragment", 3, false);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().
		getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan_.sendMetric("Avg Frags Per Read",
							  mqPtr->getRecentValueAverage(), "fragments/read", 4, false);
	}
}