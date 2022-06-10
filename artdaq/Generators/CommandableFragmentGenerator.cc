#include "TRACE/tracemf.h" // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_CommandableFragmentGenerator").c_str()  // include these 2 first -

#include "artdaq/Generators/CommandableFragmentGenerator.hh"

#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#include "fhiclcpp/ParameterSet.h"
#include "cetlib_except/exception.h"

#include <boost/exception/all.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include <sys/poll.h>
#include <algorithm>
#include <chrono>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>

#define TLVL_GETNEXT 35
#define TLVL_GETNEXT_VERBOSE 36
#define TLVL_CHECKSTOP 37
#define TLVL_EVCOUNTERINC 38
#define TLVL_GETDATALOOP 39
#define TLVL_GETDATALOOP_DATABUFFWAIT 40
#define TLVL_GETDATALOOP_VERBOSE 41
#define TLVL_WAITFORBUFFERREADY 42
#define TLVL_GETBUFFERSTATS 43
#define TLVL_CHECKDATABUFFER 44
#define TLVL_GETMONITORINGDATA 45
#define TLVL_APPLYREQUESTS 46
#define TLVL_SENDEMPTYFRAGMENTS 47
#define TLVL_CHECKWINDOWS 48
#define TLVL_EMPTYFRAGMENT 49

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(const fhicl::ParameterSet& ps)
    : mutex_()
    , useMonitoringThread_(ps.get<bool>("separate_monitoring_thread", false))
    , monitoringInterval_(ps.get<int64_t>("hardware_poll_interval_us", 0))
    , isHardwareOK_(true)
    , run_number_(-1)
    , subrun_number_(-1)
    , timeout_(std::numeric_limits<uint64_t>::max())
    , timestamp_(std::numeric_limits<uint64_t>::max())
    , should_stop_(true)
    , exception_(false)
    , latest_exception_report_("none")
    , ev_counter_(1)
    , board_id_(-1)
    , sleep_on_stop_us_(0)
{
	board_id_ = ps.get<int>("board_id");
	instance_name_for_metrics_ = "BoardReader." + boost::lexical_cast<std::string>(board_id_);

	auto fragment_ids = ps.get<std::vector<artdaq::Fragment::fragment_id_t>>("fragment_ids", std::vector<artdaq::Fragment::fragment_id_t>());

	TLOG(TLVL_DEBUG + 33) << "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)";
	int fragment_id = ps.get<int>("fragment_id", -99);

	if (fragment_id != -99)
	{
		if (!fragment_ids.empty())
		{
			latest_exception_report_ = R"(Error in CommandableFragmentGenerator: can't both define "fragment_id" and "fragment_ids" in FHiCL document)";
			TLOG(TLVL_ERROR) << latest_exception_report_;
			throw cet::exception(latest_exception_report_);
		}

		fragment_ids.emplace_back(fragment_id);
	}

	auto generated_fragments_per_event = ps.get<size_t>("generated_fragments_per_event", 1);
	if (generated_fragments_per_event != fragment_ids.size())
	{
		latest_exception_report_ = R"(Error in CommandableFragmentGenerator: "generated_fragments_per_event" disagrees with size of "fragment_ids" list!)";
		TLOG(TLVL_ERROR) << latest_exception_report_;
		throw cet::exception(latest_exception_report_);
	}

	for (auto& id : fragment_ids)
	{
		expectedTypes_[id] = artdaq::Fragment::EmptyFragmentType;
	}

	sleep_on_stop_us_ = ps.get<int>("sleep_on_stop_us", 0);
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	joinThreads();
}

void artdaq::CommandableFragmentGenerator::joinThreads()
{
	should_stop_ = true;
	TLOG(TLVL_DEBUG + 32) << "Joining monitoringThread";
	try
	{
		if (monitoringThread_.joinable())
		{
			monitoringThread_.join();
		}
	}
	catch (...)
	{
		// IGNORED
	}
	TLOG(TLVL_DEBUG + 32) << "joinThreads complete";
}

bool artdaq::CommandableFragmentGenerator::getNext(FragmentPtrs& output)
{
	bool result = true;

	if (check_stop()) usleep(sleep_on_stop_us_);
	if (exception() || should_stop_) return false;

	if (!useMonitoringThread_ && monitoringInterval_ > 0)
	{
		TLOG(TLVL_GETNEXT) << "getNext: Checking whether to collect Monitoring Data";
		auto now = std::chrono::steady_clock::now();

		if (TimeUtils::GetElapsedTimeMicroseconds(lastMonitoringCall_, now) >= static_cast<size_t>(monitoringInterval_))
		{
			TLOG(TLVL_GETNEXT) << "getNext: Collecting Monitoring Data";
			isHardwareOK_ = checkHWStatus_();
			TLOG(TLVL_GETNEXT) << "getNext: isHardwareOK_ is now " << std::boolalpha << isHardwareOK_;
			lastMonitoringCall_ = now;
		}
	}

	try
	{
		std::lock_guard<std::mutex> lk(mutex_);
		if (!isHardwareOK_)
		{
			TLOG(TLVL_ERROR) << "Stopping CFG because the hardware reports bad status!";
			return false;
		}
		TLOG(TLVL_DEBUG + 33) << "getNext: Calling getNext_ w/ ev_counter()=" << ev_counter();
		try
		{
			result = getNext_(output);
		}
		catch (...)
		{
			throw;
		}
		TLOG(TLVL_DEBUG + 33) << "getNext: Done with getNext_ - ev_counter() now " << ev_counter();
		for (auto& dataIter : output)
		{
			TLOG(TLVL_GETNEXT_VERBOSE) << "getNext: getNext_() returned fragment with sequenceID = " << dataIter->sequenceID()
			                           << ", type = " << dataIter->typeString() << ", id = " << std::to_string(dataIter->fragmentID())
			                           << ", timestamp = " << dataIter->timestamp() << ", and sizeBytes = " << dataIter->sizeBytes();

			auto fragId = dataIter->fragmentID();
			auto type = dataIter->type();

			// ELF, 2020 July 16: System Fragments are excluded from these checks
			if (Fragment::isSystemFragmentType(type))
			{
				continue;
			}

			if (!expectedTypes_.count(fragId))
			{
				TLOG(TLVL_ERROR) << "Received Fragment with Fragment ID " << fragId << ", which is not in the declared list of Fragment IDs! Aborting!";
				return false;
			}
			if (expectedTypes_[fragId] == Fragment::EmptyFragmentType)
				expectedTypes_[fragId] = type;
			else if (expectedTypes_[fragId] != type)
			{
				TLOG(TLVL_WARNING) << "Received Fragment with Fragment ID " << fragId << " and type " << dataIter->typeString() << "(" << type << "), which does not match expected type for this ID (" << expectedTypes_[fragId] << ")";
			}
		}
	}
	catch (const cet::exception& e)
	{
		latest_exception_report_ = "cet::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG(TLVL_ERROR) << "getNext: cet::exception caught: " << e;
		set_exception(true);
		return false;
	}
	catch (const boost::exception& e)
	{
		latest_exception_report_ = "boost::exception caught in getNext(): ";
		latest_exception_report_.append(boost::diagnostic_information(e));
		TLOG(TLVL_ERROR) << "getNext: boost::exception caught: " << boost::diagnostic_information(e);
		set_exception(true);
		return false;
	}
	catch (const std::exception& e)
	{
		latest_exception_report_ = "std::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG(TLVL_ERROR) << "getNext: std::exception caught: " << e.what();
		set_exception(true);
		return false;
	}
	catch (...)
	{
		latest_exception_report_ = "Unknown exception caught in getNext().";
		TLOG(TLVL_ERROR) << "getNext: unknown exception caught";
		set_exception(true);
		return false;
	}

	if (!result)
	{
		TLOG(TLVL_DEBUG + 32) << "getNext: Either getNext_ or applyRequests returned false, stopping";
	}

	if (metricMan && !output.empty())
	{
		auto timestamp = output.front()->timestamp();

		if (output.size() > 1)
		{  // Only bother sorting if >1 entry
			for (auto& outputfrag : output)
			{
				if (outputfrag->timestamp() > timestamp)
				{
					timestamp = outputfrag->timestamp();
				}
			}
		}

		metricMan->sendMetric("Last Timestamp", timestamp, "Ticks", 1, MetricMode::LastPoint);
	}

	return result;
}

bool artdaq::CommandableFragmentGenerator::check_stop()
{
	TLOG(TLVL_CHECKSTOP) << "CFG::check_stop: should_stop=" << should_stop() << ", exception status =" << int(exception());

	if (!should_stop()) return false;

	return true;
}

size_t artdaq::CommandableFragmentGenerator::ev_counter_inc(size_t step)
{
	TLOG(TLVL_EVCOUNTERINC) << "ev_counter_inc: Incrementing ev_counter from " << ev_counter() << " by " << step;
	return ev_counter_.fetch_add(step);
}  // returns the prev value

void artdaq::CommandableFragmentGenerator::StartCmd(int run, uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG + 33) << "Start Command received.";
	if (run < 0)
	{
		TLOG(TLVL_ERROR) << "negative run number";
		throw cet::exception("CommandableFragmentGenerator") << "negative run number";  // NOLINT(cert-err60-cpp)
	}

	timeout_ = timeout;
	timestamp_ = timestamp;
	ev_counter_.store(1);

	should_stop_.store(false);
	exception_.store(false);
	run_number_ = run;
	subrun_number_ = 1;
	latest_exception_report_ = "none";

	start();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useMonitoringThread_) startMonitoringThread();
	TLOG(TLVL_DEBUG + 33) << "Start Command complete.";
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG + 33) << "Stop Command received.";

	timeout_ = timeout;
	timestamp_ = timestamp;

	stopNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);
	stop();

	joinThreads();
	TLOG(TLVL_DEBUG + 33) << "Stop Command complete.";
}

void artdaq::CommandableFragmentGenerator::PauseCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG + 33) << "Pause Command received.";
	timeout_ = timeout;
	timestamp_ = timestamp;

	pauseNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);

	pause();
}

void artdaq::CommandableFragmentGenerator::ResumeCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG + 33) << "Resume Command received.";
	timeout_ = timeout;
	timestamp_ = timestamp;

	subrun_number_ += 1;
	should_stop_ = false;

	// no lock required: thread not started yet
	resume();

	std::unique_lock<std::mutex> lk(mutex_);
	//if (useDataThread_) startDataThread();
	//if (useMonitoringThread_) startMonitoringThread();
	TLOG(TLVL_DEBUG + 33) << "Resume Command complete.";
}

std::string artdaq::CommandableFragmentGenerator::ReportCmd(std::string const& which)
{
	TLOG(TLVL_DEBUG + 33) << "Report Command received.";
	std::lock_guard<std::mutex> lk(mutex_);

	// 14-May-2015, KAB: please see the comments associated with the report()
	// methods in the CommandableFragmentGenerator.hh file for more information
	// on the use of those methods in this method.

	// check if the child class has something meaningful for this request
	std::string childReport = reportSpecific(which);
	if (childReport.length() > 0) { return childReport; }

	// handle the requests that we can take care of at this level
	if (which == "latest_exception")
	{
		return latest_exception_report_;
	}

	// check if the child class has provided a catch-all report function
	childReport = report();
	if (childReport.length() > 0) { return childReport; }

	// ELF: 5/31/2019: Let BoardReaderCore's report handle this...
	/*
	// if we haven't been able to come up with any report so far, say so
	std::string tmpString = "The \"" + which + "\" command is not ";
	tmpString.append("currently supported by the ");
	tmpString.append(metricsReportingInstanceName());
	tmpString.append(" fragment generator.");
	*/
	TLOG(TLVL_DEBUG + 33) << "Report Command complete.";
	return "";  //tmpString;
}

// Default implemenetations of state functions
void artdaq::CommandableFragmentGenerator::pauseNoMutex()
{
#pragma message "Using default implementation of CommandableFragmentGenerator::pauseNoMutex()"
}

void artdaq::CommandableFragmentGenerator::pause()
{
#pragma message "Using default implementation of CommandableFragmentGenerator::pause()"
}

void artdaq::CommandableFragmentGenerator::resume(){
#pragma message "Using default implementation of CommandableFragmentGenerator::resume()"
}

std::string artdaq::CommandableFragmentGenerator::report()
{
#pragma message "Using default implementation of CommandableFragmentGenerator::report()"
	return "";
}

std::string artdaq::CommandableFragmentGenerator::reportSpecific(std::string const& /*unused*/)
{
#pragma message "Using default implementation of CommandableFragmentGenerator::reportSpecific(std::string)"
	return "";
}

bool artdaq::CommandableFragmentGenerator::checkHWStatus_()
{
#pragma message "Using default implementation of CommandableFragmentGenerator::checkHWStatus_()"
	return true;
}

bool artdaq::CommandableFragmentGenerator::metaCommand(std::string const& /*unused*/, std::string const& /*unused*/)
{
#pragma message "Using default implementation of CommandableFragmentGenerator::metaCommand(std::string, std::string)"
	return true;
}

void artdaq::CommandableFragmentGenerator::startMonitoringThread()
{
	if (monitoringThread_.joinable())
	{
		monitoringThread_.join();
	}
	TLOG(TLVL_INFO) << "Starting Hardware Monitoring Thread";
	try
	{
		monitoringThread_ = boost::thread(&CommandableFragmentGenerator::getMonitoringDataLoop, this);
		char tname[16];                                            // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-CFGMon", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                           // assure term. snprintf is not too evil :)
		auto handle = monitoringThread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Hardware Monitoring thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		throw cet::exception("ThreadError") << "Caught boost::exception starting Hardware Monitoring thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
	}
}

void artdaq::CommandableFragmentGenerator::getMonitoringDataLoop()
{
	while (!should_stop())
	{
		if (should_stop() || monitoringInterval_ <= 0)
		{
			TLOG(TLVL_DEBUG + 32) << "getMonitoringDataLoop: should_stop() is " << std::boolalpha << should_stop()
			                 << " and monitoringInterval is " << monitoringInterval_ << ", returning";
			return;
		}
		TLOG(TLVL_GETMONITORINGDATA) << "getMonitoringDataLoop: Determining whether to call checkHWStatus_";

		auto now = std::chrono::steady_clock::now();
		if (TimeUtils::GetElapsedTimeMicroseconds(lastMonitoringCall_, now) >= static_cast<size_t>(monitoringInterval_))
		{
			isHardwareOK_ = checkHWStatus_();
			TLOG(TLVL_GETMONITORINGDATA) << "getMonitoringDataLoop: isHardwareOK_ is now " << std::boolalpha << isHardwareOK_;
			lastMonitoringCall_ = now;
		}
		usleep(monitoringInterval_ / 10);
	}
}
