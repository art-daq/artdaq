#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/MPI2/RoutingMasterCore.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include <pthread.h>
#include <sched.h>
#include <algorithm>
#include "trace.h"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/Routing/makeRoutingMasterPolicy.hh"

#define TRACE_NAME "RoutingMasterCore"

const std::string artdaq::RoutingMasterCore::
TABLE_UPDATES_STAT_KEY("RoutingMasterCoreTableUpdates");
const std::string artdaq::RoutingMasterCore::
TOKENS_RECEIVED_STAT_KEY("RoutingMasterCoreTokensReceived");
const std::string artdaq::RoutingMasterCore::
INPUT_WAIT_STAT_KEY("RoutingMasterCoreInputWaitTime");
const std::string artdaq::RoutingMasterCore::
ACK_WAIT_STAT_KEY("RoutingMasterCoreAckWaitTime");

/**
* Default constructor.
*/
artdaq::RoutingMasterCore::RoutingMasterCore(Commandable& parent_application,
											 MPI_Comm local_group_comm, std::string name) :
	parent_application_(parent_application)
	, local_group_comm_(local_group_comm)
	, name_(name)
	, stop_requested_(false)
	, pause_requested_(false)
{
	mf::LogDebug(name_) << "Constructor";
	statsHelper_.addMonitoredQuantityName(TABLE_UPDATES_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(TOKENS_RECEIVED_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(INPUT_WAIT_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(ACK_WAIT_STAT_KEY);
	metricMan = &metricMan_;
}

/**
* Destructor.
*/
artdaq::RoutingMasterCore::~RoutingMasterCore()
{
	mf::LogDebug(name_) << "Destructor";
}

/**
* Processes the initialize request.
*/
bool artdaq::RoutingMasterCore::initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";

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
	try
	{
		policy_pset_ = daq_pset.get<fhicl::ParameterSet>("policy");
	}
	catch (...)
	{
		mf::LogError(name_)
			<< "Unable to find the policy parameters in the DAQ initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
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
		mf::LogInfo(name_) << "No metric plugins appear to be defined";
	}
	try
	{
		metricMan_.initialize(metric_pset, name_);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
						 "Error loading metrics in RoutingMasterCore::initialize()");
	}

	// create the requested CommandableFragmentGenerator
	auto policy_plugin_spec = policy_pset_.get<std::string>("policy", "");
	if (policy_plugin_spec.length() == 0)
	{
		mf::LogError(name_)
			<< "No fragment generator (parameter name = \"generator\") was "
			<< "specified in the fragment_receiver ParameterSet.  The "
			<< "DAQ initialization PSet was \"" << daq_pset.to_string() << "\".";
		return false;
	}
	try
	{
		policy_ = artdaq::makeRoutingMasterPolicy(policy_plugin_spec, policy_pset_);
	}
	catch (...)
	{
		std::stringstream exception_string;
		exception_string << "Exception thrown during initialization of policy of type \""
			<< policy_plugin_spec << "\"";

		ExceptionHandler(ExceptionHandlerRethrow::no, exception_string.str());

		mf::LogDebug(name_) << "FHiCL parameter set used to initialize the policy which threw an exception: " << policy_pset_.to_string();

		return false;
	}

	rt_priority_ = daq_pset.get<int>("rt_priority", 0);
	br_ranks_ = daq_pset.get<std::vector<int>>("boardreader_ranks");
	table_update_interval_ms_ = daq_pset.get<size_t>("table_update_interval_ms", 1000);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(daq_pset, 100, 30.0, 60.0, TABLE_UPDATES_STAT_KEY);

	return true;
}

bool artdaq::RoutingMasterCore::start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	stop_requested_.store(false);
	pause_requested_.store(false);

	statsHelper_.resetStatistics();

	metricMan_.do_start();
	run_id_ = id;
	table_update_count_ = 0;
	received_token_count_ = 0;

	mf::LogDebug(name_) << "Started run " << run_id_.run() <<
		", timeout = " << timeout << ", timestamp = " << timestamp << std::endl;
	return true;
}

bool artdaq::RoutingMasterCore::stop(uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "Stopping run " << run_id_.run()
		<< " after " << table_update_count_ << " table updates."
		<< " and " << received_token_count_ << " received tokens.";
	stop_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::pause(uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "Pausing run " << run_id_.run()
		<< " after " << table_update_count_ << " table updates."
		<< " and " << received_token_count_ << " received tokens.";
	pause_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::resume(uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "Resuming run " << run_id_.run();
	pause_requested_.store(false);
	metricMan_.do_start();
	return true;
}

bool artdaq::RoutingMasterCore::shutdown(uint64_t)
{
	policy_.reset(nullptr);
	metricMan_.shutdown();
	return true;
}

bool artdaq::RoutingMasterCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	mf::LogDebug(name_) << "soft_initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";
	return initialize(pset, e, f);
}

bool artdaq::RoutingMasterCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	mf::LogDebug(name_) << "reinitialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";
	return initialize(pset, e, f);
}

size_t artdaq::RoutingMasterCore::process_event_table()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param))
			mf::LogWarning(name_) << "setting realtime priority failed";
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
			mf::LogError(name_)
				<< "Failed to set realtime priority to " << rt_priority_
				<< ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	start_recieve_token_thread();

	MPI_Barrier(local_group_comm_);

	mf::LogDebug(name_) << "Sending initial table.";
	auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
	auto nextSendTime = startTime;
	double delta_time;
	while (true)
	{
		if (stop_requested_ || pause_requested_) { break; }
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		if (startTime >= nextSendTime)
		{
			send_event_table(policy_->GetCurrentTable());
			statsHelper_.addSample(TABLE_UPDATES_STAT_KEY, 1);
			nextSendTime = startTime + table_update_interval_ms_ / 1000;
		}

		delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
		statsHelper_.addSample(ACK_WAIT_STAT_KEY, delta_time);

		TRACE(16, "%s::process_fragments ACK_WAIT_STAT_KEY=%f", name_.c_str(), delta_time);

	}

	if (ev_token_receive_thread_.joinable()) ev_token_receive_thread_.join();
	metricMan_.do_stop();

	policy_.reset(nullptr);
	return table_update_count_;
}

void artdaq::RoutingMasterCore::send_event_table(detail::RoutingPacket ) {}
void artdaq::RoutingMasterCore::receive_tokens() {}
void artdaq::RoutingMasterCore::start_recieve_token_thread()
{

}

std::string artdaq::RoutingMasterCore::report(std::string const& ) const
{
	std::string resultString;

	// if we haven't been able to come up with any report so far, say so
	auto tmpString = name_ + " run number = " + std::to_string(run_id_.run())
		+ ", table updates sent = " + std::to_string(table_update_count_)
		+ ", EventBuilder tokens received = " + std::to_string(received_token_count_);
	return tmpString;
}

std::string artdaq::RoutingMasterCore::buildStatisticsString_()
{
	std::ostringstream oss;
	oss << name_ << " statistics:" << std::endl;

	auto mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TABLE_UPDATES_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Table Update statistics: "
			<< stats.recentSampleCount << " table updates sent at "
			<< stats.recentSampleRate << " table updates/sec, , monitor window = "
			<< stats.recentDuration << " sec" << std::endl;
		oss << "  Average times per table update: ";
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
				<< (1.0 / stats.recentSampleRate) << " sec";
		}
	}
	
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(ACK_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		oss << ", table acknowledgement wait time = "
			<< (mqPtr->getRecentValueSum() / br_ranks_.size()) << " sec" << std::endl;
	}
	
	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TOKENS_RECEIVED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Received Token statistics: "
			<< stats.recentSampleCount << " tokens received at "
			<< stats.recentSampleRate << " tokens/sec, , monitor window = "
			<< stats.recentDuration << " sec" << std::endl;
		oss << "  Average times per token: ";
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
				<< (1.0 / stats.recentSampleRate) << " sec";
		}
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		oss << ", input token wait time = "
			<< mqPtr->getRecentValueSum() << " sec" << std::endl;
	}

	return oss.str();
}

void artdaq::RoutingMasterCore::sendMetrics_()
{
	auto mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TABLE_UPDATES_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		metricMan_.sendMetric("Table Update Count",
							  static_cast<unsigned long>(stats.fullSampleCount),
							  "updates", 1);
		metricMan_.sendMetric("Table Update Rate",
							  stats.recentSampleRate, "updates/sec", 1);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(ACK_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		metricMan_.sendMetric("Average BoardReader Acknowledgement Time", 
			(mqPtr->getRecentValueSum() / br_ranks_.size()),
							  "seconds", 3, false);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TOKENS_RECEIVED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		metricMan_.sendMetric("EventBuilder Token Count",
							  static_cast<unsigned long>(stats.fullSampleCount),
							  "updates", 1);
		metricMan_.sendMetric("EventBuilder Token Rate",
							  stats.recentSampleRate, "updates/sec", 1);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(INPUT_WAIT_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		metricMan_.sendMetric("Total EventBuilder Token Wait Time",
			mqPtr->getRecentValueSum(),
							  "seconds", 3, false);
	}
}
