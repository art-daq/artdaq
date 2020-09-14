
#include "artdaq/DAQdata/Globals.hh"  // include these 2 first -
#define TRACE_NAME (app_name + "_BoardReaderCore").c_str()

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/Application/BoardReaderCore.hh"
#include "artdaq/Application/TaskType.hh"
#include "artdaq/Generators/makeCommandableFragmentGenerator.hh"

#include <pthread.h>
#include <sched.h>
#include <algorithm>
#include <thread>
#include <memory>
#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"

const std::string artdaq::BoardReaderCore::
    FRAGMENTS_PROCESSED_STAT_KEY("BoardReaderCoreFragmentsProcessed");
const std::string artdaq::BoardReaderCore::
    INPUT_WAIT_STAT_KEY("BoardReaderCoreInputWaitTime");
const std::string artdaq::BoardReaderCore::BUFFER_WAIT_STAT_KEY("BoardReaderCoreBufferWaitTime");
const std::string artdaq::BoardReaderCore::REQUEST_WAIT_STAT_KEY("BoardReaderCoreRequestWaitTime");
    const std::string artdaq::BoardReaderCore::
    BRSYNC_WAIT_STAT_KEY("BoardReaderCoreBRSyncWaitTime");
const std::string artdaq::BoardReaderCore::
    OUTPUT_WAIT_STAT_KEY("BoardReaderCoreOutputWaitTime");
const std::string artdaq::BoardReaderCore::
    FRAGMENTS_PER_READ_STAT_KEY("BoardReaderCoreFragmentsPerRead");

std::unique_ptr<artdaq::DataSenderManager> artdaq::BoardReaderCore::sender_ptr_ = nullptr;

artdaq::BoardReaderCore::BoardReaderCore(Commandable& parent_application)
    : parent_application_(parent_application)
    /*, local_group_comm_(local_group_comm)*/
    , generator_ptr_(nullptr)
    , run_id_(art::RunID::flushRun())
    , fragment_count_(0)
    , stop_requested_(false)
    , pause_requested_(false)
{
	TLOG(TLVL_DEBUG) << "Constructor";
	statsHelper_.addMonitoredQuantityName(FRAGMENTS_PROCESSED_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(BUFFER_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(REQUEST_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(BRSYNC_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(OUTPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(FRAGMENTS_PER_READ_STAT_KEY);
}

artdaq::BoardReaderCore::~BoardReaderCore()
{
	TLOG(TLVL_DEBUG) << "Destructor";
	TLOG(TLVL_DEBUG) << "Stopping Request Receiver BEGIN";
	request_receiver_ptr_.reset(nullptr);
	TLOG(TLVL_DEBUG) << "Stopping Request Receiver END";
}

bool artdaq::BoardReaderCore::initialize(fhicl::ParameterSet const& pset, uint64_t /*unused*/, uint64_t /*unused*/)
{
	TLOG(TLVL_DEBUG) << "initialize method called with "
	                 << "ParameterSet = \"" << pset.to_string() << "\".";

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the DAQ parameters in the initialization "
		    << "ParameterSet: \"" + pset.to_string() + "\".";
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
		TLOG(TLVL_ERROR)
		    << "Unable to find the fragment_receiver parameters in the DAQ "
		    << "initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
		return false;
	}

	// pull out the Metric part of the ParameterSet
	fhicl::ParameterSet metric_pset;
	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...)
	{}  // OK if there's no metrics table defined in the FHiCL

	if (metric_pset.is_empty())
	{
		TLOG(TLVL_INFO) << "No metric plugins appear to be defined";
	}
	try
	{
		metricMan->initialize(metric_pset, app_name);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
		                 "Error loading metrics in BoardReaderCore::initialize()");
	}

	if (daq_pset.has_key("rank"))
	{
		if (my_rank >= 0 && daq_pset.get<int>("rank") != my_rank)
		{
			TLOG(TLVL_WARNING) << "BoardReader rank specified at startup is different than rank specified at configure! Using rank received at configure!";
		}
		my_rank = daq_pset.get<int>("rank");
	}
	if (my_rank == -1)
	{
		TLOG(TLVL_ERROR) << "BoardReader rank not specified at startup or in configuration! Aborting";
		throw cet::exception("RankNotSpecifiedError") << "BoardReader rank not specified at startup or in configuration! Aborting";
	}

	// create the requested CommandableFragmentGenerator
	auto frag_gen_name = fr_pset.get<std::string>("generator", "");
	if (frag_gen_name.length() == 0)
	{
		TLOG(TLVL_ERROR)
		    << "No fragment generator (parameter name = \"generator\") was "
		    << "specified in the fragment_receiver ParameterSet.  The "
		    << "DAQ initialization PSet was \"" << daq_pset.to_string() << "\".";
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

		TLOG(TLVL_DEBUG) << "FHiCL parameter set used to initialize the fragment generator which threw an exception: " << fr_pset.to_string();

		return false;
	}

	try
	{
		fragment_buffer_ptr_.reset(new FragmentBuffer(fr_pset));
	}
	catch (...)
	{
		std::stringstream exception_string;
		exception_string << "Exception thrown during initialization of Fragment Buffer";
	
		ExceptionHandler(ExceptionHandlerRethrow::no, exception_string.str());

		TLOG(TLVL_DEBUG) << "FHiCL parameter set used to initialize the fragment buffer which threw an exception: " << fr_pset.to_string();

		return false;
	}

	std::shared_ptr<RequestBuffer> request_buffer = std::make_shared<RequestBuffer>(fr_pset.get<artdaq::Fragment::sequence_id_t>("request_increment", 1));

	try
	{
		request_receiver_ptr_.reset(new RequestReceiver(fr_pset, request_buffer));
		generator_ptr_->SetRequestBuffer(request_buffer);
		fragment_buffer_ptr_->SetRequestBuffer(request_buffer);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no, "Exception thrown during initialization of request receiver");

		TLOG(TLVL_DEBUG) << "FHiCL parameter set used to initialize the request receiver which threw an exception: " << fr_pset.to_string();

		return false;

	}
	metricMan->setPrefix(generator_ptr_->metricsReportingInstanceName());

	rt_priority_ = fr_pset.get<int>("rt_priority", 0);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(fr_pset, 100, 30.0, 60.0, FRAGMENTS_PROCESSED_STAT_KEY);

	// check if we should skip the sequence ID test...
	skip_seqId_test_ = (generator_ptr_->fragmentIDs().size() > 1 || fragment_buffer_ptr_->request_mode() != RequestMode::Ignored);

	verbose_ = fr_pset.get<bool>("verbose", true);

	return true;
}

bool artdaq::BoardReaderCore::start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Starting run " << id.run();
	stop_requested_.store(false);
	pause_requested_.store(false);

	fragment_count_ = 0;
	prev_seq_id_ = 0;
	statsHelper_.resetStatistics();

	fragment_buffer_ptr_->Reset(false);

	metricMan->do_start();
	generator_ptr_->StartCmd(id.run(), timeout, timestamp);
	run_id_ = id;

	request_receiver_ptr_->SetRunNumber(static_cast<uint32_t>(id.run()));
	request_receiver_ptr_->startRequestReception();

	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Completed the Start transition (Started run) for run " << run_id_.run()
	                                          << ", timeout = " << timeout << ", timestamp = " << timestamp;
	return true;
}

bool artdaq::BoardReaderCore::stop(uint64_t timeout, uint64_t timestamp)
{
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Stopping run " << run_id_.run() << " after " << fragment_count_ << " fragments.";
	stop_requested_.store(true);

	TLOG(TLVL_DEBUG) << "Stopping Request reception BEGIN";
	request_receiver_ptr_->stopRequestReception();
	TLOG(TLVL_DEBUG) << "Stopping Request reception END";

	TLOG(TLVL_DEBUG) << "Stopping CommandableFragmentGenerator BEGIN";
	generator_ptr_->StopCmd(timeout, timestamp);
	TLOG(TLVL_DEBUG) << "Stopping CommandableFragmentGenerator END";

	TLOG(TLVL_DEBUG) << "Stopping FragmentBuffer";
	fragment_buffer_ptr_->Stop();

	TLOG(TLVL_DEBUG) << "Stopping DataSenderManager";
	if (sender_ptr_)
	{
		sender_ptr_->StopSender();
	}

	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Completed the Stop transition for run " << run_id_.run();
	return true;
}

bool artdaq::BoardReaderCore::pause(uint64_t timeout, uint64_t timestamp)
{
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Pausing run " << run_id_.run() << " after " << fragment_count_ << " fragments.";
	pause_requested_.store(true);
	generator_ptr_->PauseCmd(timeout, timestamp);
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Completed the Pause transition for run " << run_id_.run();
	return true;
}

bool artdaq::BoardReaderCore::resume(uint64_t timeout, uint64_t timestamp)
{
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Resuming run " << run_id_.run();
	pause_requested_.store(false);
	metricMan->do_start();
	generator_ptr_->ResumeCmd(timeout, timestamp);
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Completed the Resume transition for run " << run_id_.run();
	return true;
}

bool artdaq::BoardReaderCore::shutdown(uint64_t /*unused*/)
{
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Starting Shutdown transition";
	generator_ptr_->joinThreads();  // Cleanly shut down the CommandableFragmentGenerator
	generator_ptr_.reset(nullptr);
	metricMan->shutdown();
	TLOG((verbose_ ? TLVL_INFO : TLVL_DEBUG)) << "Completed Shutdown transition";
	return true;
}

bool artdaq::BoardReaderCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG) << "soft_initialize method called with "
	                 << "ParameterSet = \"" << pset.to_string()
	                 << "\". Forwarding to initialize.";
	return initialize(pset, timeout, timestamp);
}

bool artdaq::BoardReaderCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG) << "reinitialize method called with "
	                 << "ParameterSet = \"" << pset.to_string()
	                 << "\". Forwarding to initalize.";
	return initialize(pset, timeout, timestamp);
}

void artdaq::BoardReaderCore::receive_fragments()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param))
			TLOG(TLVL_WARNING) << "setting realtime priority failed";
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
			TLOG(TLVL_ERROR)
			    << "Failed to set realtime priority to " << rt_priority_
			    << ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	TLOG(TLVL_DEBUG) << "Waiting for first fragment.";
	artdaq::MonitoredQuantityStats::TIME_POINT_T startTime, after_input, after_buffer;
	artdaq::FragmentPtrs frags;

	receiver_thread_active_ = true;

	while (receiver_thread_active_)
	{
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		TLOG(18) << "receive_fragments getNext start";
		receiver_thread_active_ = generator_ptr_->getNext(frags);
		TLOG(18) << "receive_fragments getNext done (receiver_thread_active_=" << receiver_thread_active_ << ")";
		// 08-May-2015, KAB & JCF: if the generator getNext() method returns false
		// (which indicates that the data flow has stopped) *and* the reason that
		// it has stopped is because there was an exception that wasn't handled by
		// the experiment-specific FragmentGenerator class, we move to the
		// InRunError state so that external observers (e.g. RunControl or
		// DAQInterface) can see that there was a problem.
		if (!receiver_thread_active_ && generator_ptr_ && generator_ptr_->exception())
		{
			parent_application_.in_run_failure();
		}

		after_input = artdaq::MonitoredQuantity::getCurrentTime();


		if (!receiver_thread_active_) { break; }
		statsHelper_.addSample(FRAGMENTS_PER_READ_STAT_KEY, frags.size());

		if (frags.size() > 0)
		{
			TLOG(18) << "receive_fragments AddFragmentsToBuffer start";
			fragment_buffer_ptr_->AddFragmentsToBuffer(std::move(frags));
			TLOG(18) << "receive_fragments AddFragmentsToBuffer done";
		}

		after_buffer = artdaq::MonitoredQuantity::getCurrentTime();
		TLOG(16) << "receive_fragments INPUT_WAIT=" << (after_input - startTime) << ", BUFFER_WAIT=" << (after_buffer - after_input);
		statsHelper_.addSample(INPUT_WAIT_STAT_KEY, after_input - startTime);
		statsHelper_.addSample(BUFFER_WAIT_STAT_KEY, after_buffer - after_input);
		if (statsHelper_.statsRollingWindowHasMoved()) { sendMetrics_(); }
		frags.clear();
	}

	// 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
	// processing fragments loop so that metrics correctly go to zero when
	// there is no data flowing
	metricMan->do_stop();

	TLOG(TLVL_DEBUG) << "receive_fragments loop end";
}
void artdaq::BoardReaderCore::send_fragments()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param) != 0)
		{
			TLOG(TLVL_WARNING) << "setting realtime priority failed";
		}
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
			TLOG(TLVL_ERROR)
			    << "Failed to set realtime priority to " << rt_priority_
			    << ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	TLOG(TLVL_DEBUG) << "Initializing DataSenderManager. my_rank=" << my_rank;
	sender_ptr_ = std::make_unique<artdaq::DataSenderManager>(data_pset_);

	TLOG(TLVL_DEBUG) << "Waiting for first fragment.";
	artdaq::MonitoredQuantityStats::TIME_POINT_T startTime;
	double delta_time;
	artdaq::FragmentPtrs frags;
	auto targetFragCount = generator_ptr_->fragmentIDs().size();

	sender_thread_active_ = true;

	while (sender_thread_active_)
	{
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		TLOG(18) << "send_fragments applyRequests start";
		sender_thread_active_ = fragment_buffer_ptr_->applyRequests(frags);
		TLOG(18) << "send_fragments applyRequests done (sender_thread_active_=" << sender_thread_active_ << ")";
		// 08-May-2015, KAB & JCF: if the generator getNext() method returns false
		// (which indicates that the data flow has stopped) *and* the reason that
		// it has stopped is because there was an exception that wasn't handled by
		// the experiment-specific FragmentGenerator class, we move to the
		// InRunError state so that external observers (e.g. RunControl or
		// DAQInterface) can see that there was a problem.
		if (!sender_thread_active_ && generator_ptr_ && generator_ptr_->exception())
		{
			parent_application_.in_run_failure();
		}

		delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;

		TLOG(16) << "send_fragments REQUEST_WAIT=" << delta_time;
		statsHelper_.addSample(REQUEST_WAIT_STAT_KEY, delta_time);

		if (!sender_thread_active_) { break; }

		for (auto& fragPtr : frags)
		{
			if (fragPtr == nullptr)
			{
				TLOG(TLVL_WARNING) << "Encountered a bad fragment pointer in fragment " << fragment_count_ << ". "
				                   << "This is most likely caused by a problem with the Fragment Generator!";
				continue;
			}
			if (fragment_count_ == 0)
			{
				TLOG(TLVL_DEBUG) << "Received first Fragment from Fragment Generator, sequence ID " << fragPtr->sequenceID() << ", size = " << fragPtr->sizeBytes() << " bytes.";
			}

			if (fragPtr->type() == Fragment::EndOfRunFragmentType || fragPtr->type() == Fragment::EndOfSubrunFragmentType || fragPtr->type() == Fragment::InitFragmentType)
			{
				// Just broadcast any system Fragments in the output
				artdaq::Fragment::sequence_id_t sequence_id = fragPtr->sequenceID();
				statsHelper_.addSample(FRAGMENTS_PROCESSED_STAT_KEY, fragPtr->sizeBytes());

				startTime = artdaq::MonitoredQuantity::getCurrentTime();
				TLOG(17) << "send_fragments seq=" << sequence_id << " sendFragment start";
				auto res = sender_ptr_->sendFragment(std::move(*fragPtr));
				TLOG(17) << "send_fragments seq=" << sequence_id << " sendFragment done (dest=" << res.first << ", sts=" << TransferInterface::CopyStatusToString(res.second) << ")";
				++fragment_count_;
				statsHelper_.addSample(OUTPUT_WAIT_STAT_KEY,
				                       artdaq::MonitoredQuantity::getCurrentTime() - startTime);
				continue;
			}

			artdaq::Fragment::sequence_id_t sequence_id = fragPtr->sequenceID();
			SetMFIteration("Sequence ID " + std::to_string(sequence_id));
			statsHelper_.addSample(FRAGMENTS_PROCESSED_STAT_KEY, fragPtr->sizeBytes());

			/*if ((fragment_count_ % 250) == 0)
			{
				TLOG(TLVL_DEBUG)
					<< "Sending fragment " << fragment_count_
					<< " with sequence id " << sequence_id << ".";
			}*/

			// check for continous sequence IDs
			if (!skip_seqId_test_ && abs(static_cast<int64_t>(sequence_id) - static_cast<int64_t>(prev_seq_id_)) > 1)
			{
				TLOG(TLVL_WARNING)
				    << "Missing sequence IDs: current sequence ID = "
				    << sequence_id << ", previous sequence ID = "
				    << prev_seq_id_ << ".";
			}
			prev_seq_id_ = sequence_id;

			startTime = artdaq::MonitoredQuantity::getCurrentTime();
			TLOG(17) << "send_fragments seq=" << sequence_id << " sendFragment start";
			auto res = sender_ptr_->sendFragment(std::move(*fragPtr));
			if (sender_ptr_->GetSentSequenceIDCount(sequence_id) == targetFragCount)
			{
				sender_ptr_->RemoveRoutingTableEntry(sequence_id);
			}
			TLOG(17) << "send_fragments seq=" << sequence_id << " sendFragment done (dest=" << res.first << ", sts=" << TransferInterface::CopyStatusToString(res.second) << ")";
			++fragment_count_;
			statsHelper_.addSample(OUTPUT_WAIT_STAT_KEY,
			                       artdaq::MonitoredQuantity::getCurrentTime() - startTime);

			bool readyToReport = statsHelper_.readyToReport();
			if (readyToReport)
			{
				TLOG(TLVL_INFO) << buildStatisticsString_();
			}

			// Turn on lvls (mem and/or slow) 3,13,14 to log every send.
			TLOG(((fragment_count_ == 1) ? TLVL_DEBUG
			                             : (((fragment_count_ % 250) == 0 || readyToReport) ? 13 : 14)))
			    << ((fragment_count_ == 1)
			            ? "Sent first Fragment"
			            : "Sending fragment " + std::to_string(fragment_count_))
			    << " with SeqID " << sequence_id << ".";
		}
		if (statsHelper_.statsRollingWindowHasMoved()) { sendMetrics_(); }
		frags.clear();
		std::this_thread::yield();
	}

	sender_ptr_.reset(nullptr);

	// 11-May-2015, KAB: call MetricManager::do_stop whenever we exit the
	// processing fragments loop so that metrics correctly go to zero when
	// there is no data flowing
	metricMan->do_stop();

	TLOG(TLVL_DEBUG) << "send_fragments loop end";
}

std::string artdaq::BoardReaderCore::report(std::string const& which) const
{
	std::string resultString;

	// pass the request to the FragmentGenerator instance, if it's available
	if (generator_ptr_ != nullptr && which != "core")
	{
		resultString = generator_ptr_->ReportCmd(which);
		if (resultString.length() > 0) { return resultString; }
	}

	// handle the request at this level, if we can
	// --> nothing here yet

	// if we haven't been able to come up with any report so far, say so
	std::string tmpString = app_name + " run number = ";
	tmpString.append(boost::lexical_cast<std::string>(run_id_.run()));

	tmpString.append(", Sent Fragment count = ");
	tmpString.append(boost::lexical_cast<std::string>(fragment_count_));

	if (!which.empty() && which != "core")
	{
		tmpString.append(". Command=\"" + which + "\" is not currently supported.");
	}
	return tmpString;
}

bool artdaq::BoardReaderCore::metaCommand(std::string const& command, std::string const& arg)
{
	TLOG(TLVL_DEBUG) << "metaCommand method called with "
	                 << "command = \"" << command << "\""
	                 << ", arg = \"" << arg << "\""
	                 << ".";

	if (generator_ptr_)
	{
		return generator_ptr_->metaCommand(command, arg);
	}

	return true;
}

std::string artdaq::BoardReaderCore::buildStatisticsString_()
{
	std::ostringstream oss;
	double fragmentsGeneratedCount = 1.0;
	double fragmentsOutputCount = 1.0;
	oss << app_name << " statistics:" << std::endl;

	oss    << "  Fragments read: ";
	artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << stats.recentSampleCount << " fragments generated at "
		                                    << stats.recentSampleRate << " reads/sec, fragment rate = "
		                                    << stats.recentValueRate  << " fragments/sec, monitor window = "
		                                    << stats.recentDuration << " sec, min::max read size = "
		                                    << stats.recentValueMin 
		                                    << "::"
		                                    << stats.recentValueMax 
		                                    << " fragments";
		fragmentsGeneratedCount = std::max(double(stats.recentSampleCount), 1.0);
		oss << "  Average times per fragment: ";
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
			    << (1.0 / stats.recentSampleRate) << " sec";
		}
	}

	oss << std::endl;
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(FRAGMENTS_PROCESSED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Fragment output statistics: "
		    << stats.recentSampleCount << " fragments sent at "
		    << stats.recentSampleRate << " fragments/sec, effective data rate = "
		    << (stats.recentValueRate * sizeof(artdaq::RawDataType) / 1024.0 / 1024.0) << " MB/sec, monitor window = "
		    << stats.recentDuration << " sec, min::max event size = "
		    << (stats.recentValueMin * sizeof(artdaq::RawDataType) / 1024.0 / 1024.0)
		    << "::"
		    << (stats.recentValueMax * sizeof(artdaq::RawDataType) / 1024.0 / 1024.0)
		    << " MB" << std::endl;
		fragmentsOutputCount = std::max(double(stats.recentSampleCount), 1.0);
	}

	// 31-Dec-2014, KAB - Just a reminder that using "fragmentCount" in the
	// denominator of the calculations below is important because the way that
	// the accumulation of these statistics is done is not fragment-by-fragment
	// but read-by-read (where each read can contain multiple fragments).
	// 29-Aug-2016, KAB - BRSYNC_WAIT and OUTPUT_WAIT are now done fragment-by-
	// fragment, but we'll leave the calculation the same. (The alternative
	// would be to use recentValueAverage().)

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		oss << "  Input wait time = "
		    << (mqPtr->getRecentValueSum() / fragmentsGeneratedCount) << " s/fragment";
	}
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(BUFFER_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", buffer wait time = "
		    << (mqPtr->getRecentValueSum() / fragmentsGeneratedCount) << " s/fragment";
	}
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(REQUEST_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		oss << ", request wait time = "
		    << (mqPtr->getRecentValueSum() / fragmentsOutputCount) << " s/fragment";
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(BRSYNC_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		oss << ", BRsync wait time = "
		    << (mqPtr->getRecentValueSum() / fragmentsOutputCount) << " s/fragment";
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		oss << ", output wait time = "
		    << (mqPtr->getRecentValueSum() / fragmentsOutputCount) << " s/fragment";
	}


	return oss.str();
}

void artdaq::BoardReaderCore::sendMetrics_()
{
	//TLOG(TLVL_DEBUG) << "Sending metrics " << __LINE__ ;
	double fragmentCount = 1.0;
	artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(FRAGMENTS_PROCESSED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		fragmentCount = std::max(double(stats.recentSampleCount), 1.0);
		metricMan->sendMetric("Fragment Count", stats.fullSampleCount, "fragments", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Fragment Rate", stats.recentSampleRate, "fragments/sec", 1, MetricMode::Average);
		metricMan->sendMetric("Average Fragment Size", (stats.recentValueAverage * sizeof(artdaq::RawDataType)), "bytes/fragment", 2, MetricMode::Average);
		metricMan->sendMetric("Data Rate", (stats.recentValueRate * sizeof(artdaq::RawDataType)), "bytes/sec", 2, MetricMode::Average);
	}

	// 31-Dec-2014, KAB - Just a reminder that using "fragmentCount" in the
	// denominator of the calculations below is important because the way that
	// the accumulation of these statistics is done is not fragment-by-fragment
	// but read-by-read (where each read can contain multiple fragments).
	// 29-Aug-2016, KAB - BRSYNC_WAIT and OUTPUT_WAIT are now done fragment-by-
	// fragment, but we'll leave the calculation the same. (The alternative
	// would be to use recentValueAverage().)

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		metricMan->sendMetric("Avg Input Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(BUFFER_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan->sendMetric("Avg Buffer Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(REQUEST_WAIT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		metricMan->sendMetric("Avg Request Response Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(BRSYNC_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		metricMan->sendMetric("Avg BoardReader Sync Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(OUTPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		metricMan->sendMetric("Avg Output Wait Time", (mqPtr->getRecentValueSum() / fragmentCount), "seconds/fragment", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(FRAGMENTS_PER_READ_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		metricMan->sendMetric("Avg Frags Per Read", mqPtr->getRecentValueAverage(), "fragments/read", 4, MetricMode::Average);
	}
}
