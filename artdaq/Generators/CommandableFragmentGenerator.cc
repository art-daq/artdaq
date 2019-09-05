#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_CommandableFragmentGenerator").c_str()  // include these 2 first -

#include "artdaq/Generators/CommandableFragmentGenerator.hh"

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <iterator>
#include <limits>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Utilities/SimpleLookupPolicy.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#include <sys/poll.h>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include "artdaq/DAQdata/TCPConnect.hh"

#define TLVL_GETNEXT 10
#define TLVL_GETNEXT_VERBOSE 20
#define TLVL_CHECKSTOP 11
#define TLVL_EVCOUNTERINC 12
#define TLVL_GETDATALOOP 13
#define TLVL_GETDATALOOP_DATABUFFWAIT 21
#define TLVL_GETDATALOOP_VERBOSE 20
#define TLVL_WAITFORBUFFERREADY 15
#define TLVL_GETBUFFERSTATS 16
#define TLVL_CHECKDATABUFFER 17
#define TLVL_GETMONITORINGDATA 18
#define TLVL_APPLYREQUESTS 9
#define TLVL_SENDEMPTYFRAGMENTS 19
#define TLVL_CHECKWINDOWS 14

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(const fhicl::ParameterSet& ps)
    : mutex_()
    , requestReceiver_(nullptr)
    , windowOffset_(ps.get<Fragment::timestamp_t>("request_window_offset", 0))
    , windowWidth_(ps.get<Fragment::timestamp_t>("request_window_width", 0))
    , staleTimeout_(ps.get<Fragment::timestamp_t>("stale_request_timeout", 0xFFFFFFFF))
    , expectedType_(ps.get<Fragment::type_t>("expected_fragment_type", Fragment::type_t(Fragment::EmptyFragmentType)))
    , uniqueWindows_(ps.get<bool>("request_windows_are_unique", true))
    , missing_request_window_timeout_us_(ps.get<size_t>("missing_request_window_timeout_us", 5000000))
    , window_close_timeout_us_(ps.get<size_t>("window_close_timeout_us", 2000000))
    , useDataThread_(ps.get<bool>("separate_data_thread", false))
    , circularDataBufferMode_(ps.get<bool>("circular_buffer_mode", false))
    , sleep_on_no_data_us_(ps.get<size_t>("sleep_on_no_data_us", 0))
    , data_thread_running_(false)
    , maxDataBufferDepthFragments_(ps.get<int>("data_buffer_depth_fragments", 1000))
    , maxDataBufferDepthBytes_(ps.get<size_t>("data_buffer_depth_mb", 1000) * 1024 * 1024)
    , useMonitoringThread_(ps.get<bool>("separate_monitoring_thread", false))
    , monitoringInterval_(ps.get<int64_t>("hardware_poll_interval_us", 0))
    , lastMonitoringCall_()
    , isHardwareOK_(true)
    , run_number_(-1)
    , subrun_number_(-1)
    , timeout_(std::numeric_limits<uint64_t>::max())
    , timestamp_(std::numeric_limits<uint64_t>::max())
    , should_stop_(false)
    , exception_(false)
    , force_stop_(false)
    , latest_exception_report_("none")
    , ev_counter_(1)
    , board_id_(-1)
    , sleep_on_stop_us_(0)
{
	board_id_ = ps.get<int>("board_id");
	instance_name_for_metrics_ = "BoardReader." + boost::lexical_cast<std::string>(board_id_);

	auto fragment_ids = ps.get<std::vector<artdaq::Fragment::fragment_id_t>>("fragment_ids", std::vector<artdaq::Fragment::fragment_id_t>());

	TLOG(TLVL_TRACE) << "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)";
	int fragment_id = ps.get<int>("fragment_id", -99);

	if (fragment_id != -99)
	{
		if (fragment_ids.size() != 0)
		{
			latest_exception_report_ = "Error in CommandableFragmentGenerator: can't both define \"fragment_id\" and \"fragment_ids\" in FHiCL document";
			throw cet::exception(latest_exception_report_);
		}
		else
		{
			fragment_ids.emplace_back(fragment_id);
		}
	}

	for (auto& id : fragment_ids)
	{
		dataBuffers_[id].DataBufferDepthBytes = 0;
		dataBuffers_[id].DataBufferDepthFragments = 0;
		dataBuffers_[id].HighestRequestSeen = 0;
		dataBuffers_[id].DataBuffer.emplace_back(FragmentPtr(new Fragment()));
		(*dataBuffers_[id].DataBuffer.begin())->setSystemType(Fragment::EmptyFragmentType);
	}

	sleep_on_stop_us_ = ps.get<int>("sleep_on_stop_us", 0);

	std::string modeString = ps.get<std::string>("request_mode", "ignored");
	if (modeString == "single" || modeString == "Single")
	{
		mode_ = RequestMode::Single;
	}
	else if (modeString.find("buffer") != std::string::npos || modeString.find("Buffer") != std::string::npos)
	{
		mode_ = RequestMode::Buffer;
	}
	else if (modeString == "window" || modeString == "Window")
	{
		mode_ = RequestMode::Window;
	}
	else if (modeString.find("ignore") != std::string::npos || modeString.find("Ignore") != std::string::npos)
	{
		mode_ = RequestMode::Ignored;
	}
	TLOG(TLVL_DEBUG) << "Request mode is " << printMode_();

	if (mode_ != RequestMode::Ignored)
	{
		if (!useDataThread_)
		{
			latest_exception_report_ = "Error in CommandableFragmentGenerator: use_data_thread must be true when request_mode is not \"Ignored\"!";
			throw cet::exception(latest_exception_report_);
		}
		requestReceiver_.reset(new RequestReceiver(ps));
	}
	else
	{
		requestReceiver_.reset(new RequestReceiver(ps, true));
	}
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	joinThreads();
	requestReceiver_.reset(nullptr);
}

void artdaq::CommandableFragmentGenerator::joinThreads()
{
	should_stop_ = true;
	force_stop_ = true;
	TLOG(TLVL_DEBUG) << "Joining dataThread";
	if (dataThread_.joinable()) dataThread_.join();
	TLOG(TLVL_DEBUG) << "Joining monitoringThread";
	if (monitoringThread_.joinable()) monitoringThread_.join();
	TLOG(TLVL_DEBUG) << "joinThreads complete";
}

bool artdaq::CommandableFragmentGenerator::getNext(PostmarkedFragmentPtrs& output)
{
	bool result = true;

	if (check_stop()) usleep(sleep_on_stop_us_);
	if (exception() || force_stop_) return false;

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
		if (useDataThread_)
		{
			TLOG(TLVL_TRACE) << "getNext: Calling applyRequests";
			result = applyRequests(output);
			TLOG(TLVL_TRACE) << "getNext: Done with applyRequests result=" << std::boolalpha << result;
			for (auto dataIter = output.begin(); dataIter != output.end(); ++dataIter)
			{
				TLOG(20) << "getNext: applyRequests() returned fragment with sequenceID = " << (*dataIter).first->sequenceID()
				         << ", type = " << (*dataIter).first->typeString() << ", id = " << std::to_string((*dataIter).first->fragmentID())
				         << ", timestamp = " << (*dataIter).first->timestamp() << ", and sizeBytes = " << (*dataIter).first->sizeBytes();
			}

			if (exception())
			{
				TLOG(TLVL_ERROR) << "Exception found in BoardReader with board ID " << board_id() << "; BoardReader will now return error status when queried";
				throw cet::exception("CommandableFragmentGenerator") << "Exception found in BoardReader with board ID " << board_id() << "; BoardReader will now return error status when queried";
			}
		}
		else
		{
			if (!isHardwareOK_)
			{
				TLOG(TLVL_ERROR) << "Stopping CFG because the hardware reports bad status!";
				return false;
			}
			TLOG(TLVL_TRACE) << "getNext: Calling getNext_ w/ ev_counter()=" << ev_counter();
			try
			{
				FragmentPtrs frags;
				result = getNext_(frags);
				for (auto& fragptr : frags)
				{
					std::pair<artdaq::FragmentPtr, int> fragptr_wdest = std::make_pair(std::move(fragptr), artdaq::Fragment::InvalidDestinationRank);
					output.emplace_back(std::move(fragptr_wdest));
				}
			}
			catch (...)
			{
				throw;
			}
			TLOG(TLVL_TRACE) << "getNext: Done with getNext_ - ev_counter() now " << ev_counter();
			for (auto dataIter = output.begin(); dataIter != output.end(); ++dataIter)
			{
				TLOG(TLVL_GETNEXT_VERBOSE) << "getNext: getNext_() returned fragment with sequenceID = " << (*dataIter).first->sequenceID()
				                           << ", type = " << (*dataIter).first->typeString() << ", id = " << std::to_string((*dataIter).first->fragmentID())
				                           << ", timestamp = " << (*dataIter).first->timestamp() << ", and sizeBytes = " << (*dataIter).first->sizeBytes();
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
		TLOG(TLVL_DEBUG) << "getNext: Either getNext_ or applyRequests returned false, stopping";
	}

	if (metricMan && !output.empty())
	{
		auto timestamp = output.front().first->timestamp();

		if (output.size() > 1)
		{  // Only bother sorting if >1 entry
			for (auto& outputfrag : output)
			{
				if (outputfrag.first->timestamp() > timestamp)
				{
					timestamp = outputfrag.first->timestamp();
				}
			}
		}

		metricMan->sendMetric("Last Timestamp", timestamp, "Ticks", 1,
		                      MetricMode::LastPoint, app_name);
	}

	return result;
}

bool artdaq::CommandableFragmentGenerator::check_stop()
{
	TLOG(TLVL_CHECKSTOP) << "CFG::check_stop: should_stop=" << should_stop() << ", useDataThread_=" << useDataThread_ << ", exception status =" << int(exception());

	if (!should_stop()) return false;
	if (!useDataThread_ || mode_ == RequestMode::Ignored) return true;
	if (force_stop_) return true;

	// check_stop returns true if the CFG should stop. We should wait for the RequestReceiver to stop before stopping.
	TLOG(TLVL_DEBUG) << "should_stop is true, force_stop_ is false, requestReceiver_->isRunning() is " << std::boolalpha << requestReceiver_->isRunning();
	return !requestReceiver_->isRunning();
}

size_t artdaq::CommandableFragmentGenerator::ev_counter_inc(size_t step, bool force)
{
	if (force || mode_ == RequestMode::Ignored)
	{
		TLOG(TLVL_EVCOUNTERINC) << "ev_counter_inc: Incrementing ev_counter from " << ev_counter() << " by " << step;
		return ev_counter_.fetch_add(step);
	}
	return ev_counter_.load();
}  // returns the prev value

void artdaq::CommandableFragmentGenerator::StartCmd(int run, uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_TRACE) << "Start Command received.";
	if (run < 0) throw cet::exception("CommandableFragmentGenerator") << "negative run number";

	timeout_ = timeout;
	timestamp_ = timestamp;
	ev_counter_.store(1);
	{
		std::unique_lock<std::mutex> lk(dataBuffersMutex_);
		for (auto& id : dataBuffers_)
		{
			id.second.DataBufferDepthBytes = 0;
			id.second.DataBufferDepthFragments = 0;
			id.second.HighestRequestSeen = 0;
			id.second.DataBuffer.clear();
			id.second.WindowsSent.clear();
		}
	}

	should_stop_.store(false);
	force_stop_.store(false);
	exception_.store(false);
	run_number_ = run;
	subrun_number_ = 1;
	latest_exception_report_ = "none";

	start();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	//if (mode_ != RequestMode::Ignored)
	//{
        // KAB: temporary change to help with measuring round-trip times
		requestReceiver_->SetRunNumber(static_cast<uint32_t>(run));
		requestReceiver_->startRequestReception();
                //}
	TLOG(TLVL_TRACE) << "Start Command complete.";
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_TRACE) << "Stop Command received.";

	timeout_ = timeout;
	timestamp_ = timestamp;
	if (requestReceiver_)
	{
		TLOG(TLVL_DEBUG) << "Stopping Request reception BEGIN";
		requestReceiver_->stopRequestReception();
		TLOG(TLVL_DEBUG) << "Stopping Request reception END";
	}

	stopNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);
	stop();

	joinThreads();
	TLOG(TLVL_TRACE) << "Stop Command complete.";
}

void artdaq::CommandableFragmentGenerator::PauseCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_TRACE) << "Pause Command received.";
	timeout_ = timeout;
	timestamp_ = timestamp;
	//if (requestReceiver_->isRunning()) requestReceiver_->stopRequestReceiverThread();

	pauseNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);

	pause();
}

void artdaq::CommandableFragmentGenerator::ResumeCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_TRACE) << "Resume Command received.";
	timeout_ = timeout;
	timestamp_ = timestamp;

	subrun_number_ += 1;
	should_stop_ = false;

	{
		std::unique_lock<std::mutex> lk(dataBuffersMutex_);
		for (auto& id : dataBuffers_)
		{
			id.second.DataBufferDepthBytes = 0;
			id.second.DataBufferDepthFragments = 0;
			id.second.DataBuffer.clear();
		}
	}
	// no lock required: thread not started yet
	resume();

	std::unique_lock<std::mutex> lk(mutex_);
	//if (useDataThread_) startDataThread();
	//if (useMonitoringThread_) startMonitoringThread();
	//if (mode_ != RequestMode::Ignored && !requestReceiver_->isRunning()) requestReceiver_->startRequestReceiverThread();
	TLOG(TLVL_TRACE) << "Resume Command complete.";
}

std::string artdaq::CommandableFragmentGenerator::ReportCmd(std::string const& which)
{
	TLOG(TLVL_TRACE) << "Report Command received.";
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

	// if we haven't been able to come up with any report so far, say so
	std::string tmpString = "The \"" + which + "\" command is not ";
	tmpString.append("currently supported by the ");
	tmpString.append(metricsReportingInstanceName());
	tmpString.append(" fragment generator.");
	TLOG(TLVL_TRACE) << "Report Command complete.";
	return tmpString;
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

std::string artdaq::CommandableFragmentGenerator::reportSpecific(std::string const&)
{
#pragma message "Using default implementation of CommandableFragmentGenerator::reportSpecific(std::string)"
	return "";
}

bool artdaq::CommandableFragmentGenerator::checkHWStatus_()
{
#pragma message "Using default implementation of CommandableFragmentGenerator::checkHWStatus_()"
	return true;
}

bool artdaq::CommandableFragmentGenerator::metaCommand(std::string const&, std::string const&)
{
#pragma message "Using default implementation of CommandableFragmentGenerator::metaCommand(std::string, std::string)"
	return true;
}

void artdaq::CommandableFragmentGenerator::startDataThread()
{
	if (dataThread_.joinable()) dataThread_.join();
	TLOG(TLVL_INFO) << "Starting Data Receiver Thread";
	try
	{
		dataThread_ = boost::thread(&CommandableFragmentGenerator::getDataLoop, this);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Data Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Data Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}

void artdaq::CommandableFragmentGenerator::startMonitoringThread()
{
	if (monitoringThread_.joinable()) monitoringThread_.join();
	TLOG(TLVL_INFO) << "Starting Hardware Monitoring Thread";
	try
	{
		monitoringThread_ = boost::thread(&CommandableFragmentGenerator::getMonitoringDataLoop, this);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Hardware Monitoring thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Hardware Monitoring thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}

std::string artdaq::CommandableFragmentGenerator::printMode_()
{
	switch (mode_)
	{
		case RequestMode::Single:
			return "Single";
		case RequestMode::Buffer:
			return "Buffer";
		case RequestMode::Window:
			return "Window";
		case RequestMode::Ignored:
			return "Ignored";
	}

	return "ERROR";
}

//
// The "useDataThread_" thread
//
void artdaq::CommandableFragmentGenerator::getDataLoop()
{
	data_thread_running_ = true;
	while (!force_stop_)
	{
		if (!isHardwareOK_)
		{
			TLOG(TLVL_DEBUG) << "getDataLoop: isHardwareOK is " << isHardwareOK_ << ", aborting data thread";
			data_thread_running_ = false;
			return;
		}

		TLOG(TLVL_GETDATALOOP) << "getDataLoop: calling getNext_";

		bool data = false;
		auto startdata = std::chrono::steady_clock::now();
		FragmentPtrs newData;

		try
		{
			data = getNext_(newData);
		}
		catch (...)
		{
			ExceptionHandler(ExceptionHandlerRethrow::no,
			                 "Exception thrown by fragment generator in CommandableFragmentGenerator::getDataLoop; setting exception state to \"true\"");
			set_exception(true);

			data_thread_running_ = false;
			return;
		}

		if (metricMan)
		{
			metricMan->sendMetric("Avg Data Acquisition Time", TimeUtils::GetElapsedTime(startdata), "s", 3, artdaq::MetricMode::Average);
		}

		if (newData.size() == 0 && sleep_on_no_data_us_ > 0)
		{
			usleep(sleep_on_no_data_us_);
		}

		auto dataIter = newData.begin();
		while (dataIter != newData.end())
		{
			TLOG(TLVL_GETDATALOOP_VERBOSE) << "getDataLoop: getNext_() returned fragment with timestamp = " << (*dataIter)->timestamp() << ", and sizeBytes = " << (*dataIter)->sizeBytes();

			auto frag_id = (*dataIter)->fragmentID();
			if (!dataBuffers_.count(frag_id))
			{
				throw cet::exception("DataBufferError") << "Error in CommandableFragmentGenerator: Recevied Fragment with fragment_id " << frag_id << ", but this ID was not declared in fragment_ids!";
			}

			TLOG(TLVL_GETDATALOOP_DATABUFFWAIT) << "Waiting for data buffer ready";
			if (!waitForDataBufferReady(frag_id)) return;
			TLOG(TLVL_GETDATALOOP_DATABUFFWAIT) << "Done waiting for data buffer ready";

			TLOG(TLVL_GETDATALOOP) << "getDataLoop: processing data";
			if (data && !force_stop_)
			{
				std::unique_lock<std::mutex> lock(dataBuffersMutex_);
				switch (mode_)
				{
					case RequestMode::Single:
						dataBuffers_[frag_id].DataBuffer.clear();
						dataBuffers_[frag_id].DataBufferDepthBytes = (*dataIter)->sizeBytes();
						dataBuffers_[frag_id].DataBuffer.emplace_back(std::move(*dataIter));
						dataIter = newData.erase(dataIter);
						break;
					case RequestMode::Buffer:
					case RequestMode::Ignored:
					case RequestMode::Window:
					default:
						//dataBuffer_.reserve(dataBuffer_.size() + newDataBuffer_.size());
						dataBuffers_[frag_id].DataBufferDepthBytes += (*dataIter)->sizeBytes();
						dataBuffers_[frag_id].DataBuffer.emplace_back(std::move(*dataIter));
						dataIter = newData.erase(dataIter);
						break;
				}
				getDataBufferStats(frag_id);
			}
			else
			{
				break;
			}
		}

		{
			std::unique_lock<std::mutex> lock(dataBuffersMutex_);
			for (auto& id : dataBuffers_)
			{
				if (id.second.DataBuffer.size() > 0)
				{
					dataCondition_.notify_all();
					break;
				}
			}
		}
		if (!data || force_stop_)
		{
			TLOG(TLVL_INFO) << "Data flow has stopped. Ending data collection thread";
			std::unique_lock<std::mutex> lock(dataBuffersMutex_);
			data_thread_running_ = false;
			if (requestReceiver_) requestReceiver_->ClearRequests();
			newData.clear();
			TLOG(TLVL_INFO) << "getDataLoop: Ending thread";
			return;
		}
	}
}

bool artdaq::CommandableFragmentGenerator::waitForDataBufferReady(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		throw cet::exception("DataBufferError") << "Error in CommandableFragmentGenerator: Cannot wait for data buffer for ID " << id << " because it does not exist!";
	}
	auto startwait = std::chrono::steady_clock::now();
	auto first = true;
	auto lastwaittime = 0ULL;

	{
		std::unique_lock<std::mutex> lock(dataBuffersMutex_);
		getDataBufferStats(id);
	}

	while (dataBufferIsTooLarge(id))
	{
		if (!circularDataBufferMode_)
		{
			if (should_stop())
			{
				TLOG(TLVL_DEBUG) << "Run ended while waiting for buffer to shrink!";
				std::unique_lock<std::mutex> lock(dataBuffersMutex_);
				getDataBufferStats(id);
				dataCondition_.notify_all();
				data_thread_running_ = false;
				return false;
			}
			auto waittime = TimeUtils::GetElapsedTimeMilliseconds(startwait);

			if (first || (waittime != lastwaittime && waittime % 1000 == 0))
			{
				TLOG(TLVL_WARNING) << "Bad Omen: Data Buffer has exceeded its size limits. "
				                   << "(seq_id=" << ev_counter() << ", frag_id=" << id
				                   << ", frags=" << dataBuffers_[id].DataBufferDepthFragments << "/" << maxDataBufferDepthFragments_
				                   << ", szB=" << dataBuffers_[id].DataBufferDepthBytes << "/" << maxDataBufferDepthBytes_ << ")";
				TLOG(TLVL_TRACE) << "Bad Omen: Possible causes include requests not getting through or Ignored-mode BR issues";
				first = false;
			}
			if (waittime % 5 && waittime != lastwaittime)
			{
				TLOG(TLVL_WAITFORBUFFERREADY) << "getDataLoop: Data Retreival paused for " << waittime << " ms waiting for data buffer to drain";
			}
			lastwaittime = waittime;
			usleep(1000);
		}
		else
		{
			std::unique_lock<std::mutex> lock(dataBuffersMutex_);
			getDataBufferStats(id);  // Re-check under lock
			if (dataBufferIsTooLarge(id))
			{
				auto begin = dataBuffers_[id].DataBuffer.begin();
				if (begin == dataBuffers_[id].DataBuffer.end())
				{
					TLOG(TLVL_WARNING) << "Data buffer is reported as too large, but doesn't contain any Fragments! Possible corrupt memory!";
					continue;
				}
				if (*begin)
				{
					TLOG(TLVL_WAITFORBUFFERREADY) << "waitForDataBufferReady: Dropping Fragment with timestamp " << (*begin)->timestamp() << " from data buffer (Buffer over-size, circular data buffer mode)";
				}
				dataBuffers_[id].DataBufferDepthBytes -= (*begin)->sizeBytes();
				dataBuffers_[id].DataBuffer.erase(begin);
				getDataBufferStats(id);
			}
		}
	}
	return true;
}

bool artdaq::CommandableFragmentGenerator::dataBufferIsTooLarge(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		throw cet::exception("DataBufferError") << "Error in CommandableFragmentGenerator: Cannot check size of data buffer for ID " << id << " because it does not exist!";
	}
	return (maxDataBufferDepthFragments_ > 0 && dataBuffers_[id].DataBufferDepthFragments > maxDataBufferDepthFragments_) || (maxDataBufferDepthBytes_ > 0 && dataBuffers_[id].DataBufferDepthBytes > maxDataBufferDepthBytes_);
}

void artdaq::CommandableFragmentGenerator::getDataBufferStats(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		throw cet::exception("DataBufferError") << "Error in CommandableFragmentGenerator: Cannot get stats of data buffer for ID " << id << " because it does not exist!";
	}
	/// dataBufferMutex must be owned by the calling thread!
	dataBuffers_[id].DataBufferDepthFragments = dataBuffers_[id].DataBuffer.size();

	if (metricMan)
	{
		TLOG(TLVL_GETBUFFERSTATS) << "getDataBufferStats: Sending Metrics";
		metricMan->sendMetric("Buffer Depth Fragments", dataBuffers_[id].DataBufferDepthFragments.load(), "fragments", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Buffer Depth Bytes", dataBuffers_[id].DataBufferDepthBytes.load(), "bytes", 1, MetricMode::LastPoint);
	}
	TLOG(TLVL_GETBUFFERSTATS) << "getDataBufferStats: frags=" << dataBuffers_[id].DataBufferDepthFragments.load() << "/" << maxDataBufferDepthFragments_
	                          << ", sz=" << dataBuffers_[id].DataBufferDepthBytes.load() << "/" << maxDataBufferDepthBytes_;
}

void artdaq::CommandableFragmentGenerator::checkDataBuffer(Fragment::fragment_id_t id)
{
	if (!dataBuffers_.count(id))
	{
		throw cet::exception("DataBufferError") << "Error in CommandableFragmentGenerator: Cannot check data buffer for ID " << id << " because it does not exist!";
	}

	if (dataBuffers_[id].DataBufferDepthFragments > 0 && mode_ != RequestMode::Single && mode_ != RequestMode::Ignored)
	{
		// Eliminate extra fragments
		getDataBufferStats(id);
		while (dataBufferIsTooLarge(id))
		{
			auto begin = dataBuffers_[id].DataBuffer.begin();
			TLOG(TLVL_CHECKDATABUFFER) << "checkDataBuffer: Dropping Fragment with timestamp " << (*begin)->timestamp() << " from data buffer (Buffer over-size)";
			dataBuffers_[id].DataBufferDepthBytes -= (*begin)->sizeBytes();
			dataBuffers_[id].DataBuffer.erase(begin);
			getDataBufferStats(id);
		}
		if (dataBuffers_[id].DataBuffer.size() > 0)
		{
			TLOG(TLVL_CHECKDATABUFFER) << "Determining if Fragments can be dropped from data buffer";
			Fragment::timestamp_t last = dataBuffers_[id].DataBuffer.back()->timestamp();
			Fragment::timestamp_t min = last > staleTimeout_ ? last - staleTimeout_ : 0;
			for (auto it = dataBuffers_[id].DataBuffer.begin(); it != dataBuffers_[id].DataBuffer.end();)
			{
				if ((*it)->timestamp() < min)
				{
					TLOG(TLVL_CHECKDATABUFFER) << "checkDataBuffer: Dropping Fragment with timestamp " << (*it)->timestamp() << " from data buffer (timeout=" << staleTimeout_ << ", min=" << min << ")";
					dataBuffers_[id].DataBufferDepthBytes -= (*it)->sizeBytes();
					it = dataBuffers_[id].DataBuffer.erase(it);
				}
				else
				{
					break;
				}
			}
			getDataBufferStats(id);
		}
	}
}

void artdaq::CommandableFragmentGenerator::getMonitoringDataLoop()
{
	while (!force_stop_)
	{
		if (should_stop() || monitoringInterval_ <= 0)
		{
			TLOG(TLVL_DEBUG) << "getMonitoringDataLoop: should_stop() is " << std::boolalpha << should_stop()
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

void artdaq::CommandableFragmentGenerator::applyRequestsIgnoredMode(artdaq::PostmarkedFragmentPtrs& frags)
{
	// dataBuffersMutex_ is held by calling function
	// We just copy everything that's here into the output.
	TLOG(TLVL_APPLYREQUESTS) << "Mode is Ignored; Copying data to output";
	for (auto& id : dataBuffers_)
	{
		for (auto& fragptr : id.second.DataBuffer)
		{
			std::pair<artdaq::FragmentPtr, int> fragptr_wdest = std::make_pair(std::move(fragptr), artdaq::Fragment::InvalidDestinationRank);
			frags.emplace_back(std::move(fragptr_wdest));
		}
		id.second.DataBufferDepthBytes = 0;
		id.second.DataBufferDepthFragments = 0;
		id.second.DataBuffer.clear();
	}
}

void artdaq::CommandableFragmentGenerator::applyRequestsSingleMode(artdaq::PostmarkedFragmentPtrs& frags)
{
	// We only care about the latest request received. Send empties for all others.
	auto requests = requestReceiver_->GetRequests();
	while (requests.size() > 1)
	{
		// std::map is ordered by key => Last sequence ID in the map is the one we care about
		requestReceiver_->RemoveRequest(requests.begin()->first);
		requests.erase(requests.begin());
	}
	sendEmptyFragments(frags, requests);

	// If no requests remain after sendEmptyFragments, return
	if (requests.size() == 0 || !requests.count(ev_counter())) return;

	// 12-Jun-2019, KAB: TODO: understand which request RANK should be returned with each fragment

	for (auto& id : dataBuffers_)
	{
		if (id.second.DataBuffer.size() > 0)
		{
			assert(id.second.DataBuffer.size() == 1);
			TLOG(TLVL_APPLYREQUESTS) << "Mode is Single; Sending copy of last event";
			for (auto& fragptr : id.second.DataBuffer)
			{
				// Return the latest data point
				auto frag = fragptr.get();
				auto newfrag = std::unique_ptr<artdaq::Fragment>(new Fragment(ev_counter(), frag->fragmentID()));
				newfrag->resize(frag->size() - detail::RawFragmentHeader::num_words());
				memcpy(newfrag->headerAddress(), frag->headerAddress(), frag->sizeBytes());
				newfrag->setTimestamp(requests[ev_counter()].timestamp);
				newfrag->setSequenceID(ev_counter());
				std::pair<artdaq::FragmentPtr, int> pm_frag = std::make_pair(std::move(newfrag), artdaq::Fragment::InvalidDestinationRank);
				frags.emplace_back(std::move(pm_frag));
			}
		}
		else
		{
			sendEmptyFragment(frags, ev_counter(), id.first, "No data for");
		}
	}
	requestReceiver_->RemoveRequest(ev_counter());
	ev_counter_inc(1, true);
}

void artdaq::CommandableFragmentGenerator::applyRequestsBufferMode(artdaq::PostmarkedFragmentPtrs& frags)
{
	// We only care about the latest request received. Send empties for all others.
	auto requests = requestReceiver_->GetRequests();
	while (requests.size() > 1)
	{
		// std::map is ordered by key => Last sequence ID in the map is the one we care about
		requestReceiver_->RemoveRequest(requests.begin()->first);
		requests.erase(requests.begin());
	}
	sendEmptyFragments(frags, requests);

	// If no requests remain after sendEmptyFragments, return
	if (requests.size() == 0 || !requests.count(ev_counter())) return;

	// 12-Jun-2019, KAB: TODO: understand which request RANK should be returned with each fragment

	for (auto& id : dataBuffers_)
	{
		TLOG(TLVL_DEBUG) << "Creating ContainerFragment for Buffered Fragments";

		auto newfrag = std::unique_ptr<artdaq::Fragment>(new artdaq::Fragment(ev_counter(), id.first));
		std::pair<artdaq::FragmentPtr, int> pm_frag = std::make_pair(std::move(newfrag), artdaq::Fragment::InvalidDestinationRank);
		frags.emplace_back(std::move(pm_frag));
		frags.back().first->setTimestamp(requests[ev_counter()].timestamp);
		ContainerFragmentLoader cfl(*frags.back().first);
		cfl.set_missing_data(false);  // Buffer mode is never missing data, even if there IS no data.

		// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
		// Window mode TFGs must do a little bit more work to decide which fragments to send for a given request
		for (auto it = id.second.DataBuffer.begin(); it != id.second.DataBuffer.end();)
		{
			TLOG(TLVL_APPLYREQUESTS) << "ApplyRequests: Adding Fragment with timestamp " << (*it)->timestamp() << " to Container with sequence ID " << ev_counter();
			cfl.addFragment(*it);
			id.second.DataBufferDepthBytes -= (*it)->sizeBytes();
			it = id.second.DataBuffer.erase(it);
		}
	}
	requestReceiver_->RemoveRequest(ev_counter());
	ev_counter_inc(1, true);
}

void artdaq::CommandableFragmentGenerator::applyRequestsWindowMode_CheckAndFillDataBuffer(artdaq::PostmarkedFragmentPtrs& frags, artdaq::Fragment::fragment_id_t id, artdaq::Fragment::sequence_id_t seq, artdaq::Fragment::timestamp_t ts, int rank)
{
	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Checking that data exists for request window " << seq;
	Fragment::timestamp_t min = ts > windowOffset_ ? ts - windowOffset_ : 0;
	Fragment::timestamp_t max = min + windowWidth_;
	TLOG(TLVL_APPLYREQUESTS) << "ApplyRequestsWindowsMode_CheckAndFillDataBuffer: min is " << min << ", max is " << max
	                         << " and first/last points in buffer are " << (dataBuffers_[id].DataBuffer.size() > 0 ? dataBuffers_[id].DataBuffer.front()->timestamp() : 0)
	                         << "/" << (dataBuffers_[id].DataBuffer.size() > 0 ? dataBuffers_[id].DataBuffer.back()->timestamp() : 0)
	                         << " (sz=" << dataBuffers_[id].DataBuffer.size() << " [" << dataBuffers_[id].DataBufferDepthBytes.load()
	                         << "/" << maxDataBufferDepthBytes_ << "])";
	bool windowClosed = dataBuffers_[id].DataBuffer.size() > 0 && dataBuffers_[id].DataBuffer.back()->timestamp() >= max;
	bool windowTimeout = !windowClosed && TimeUtils::GetElapsedTimeMicroseconds(requestReceiver_->GetRequestTime(seq)) > window_close_timeout_us_;
	if (windowTimeout)
	{
		TLOG(TLVL_WARNING) << "applyRequestsWindowMode_CheckAndFillDataBuffer: A timeout occurred waiting for data to close the request window ({" << min << "-" << max
		                   << "}, buffer={" << (dataBuffers_[id].DataBuffer.size() > 0 ? dataBuffers_[id].DataBuffer.front()->timestamp() : 0) << "-"
		                   << (dataBuffers_[id].DataBuffer.size() > 0 ? dataBuffers_[id].DataBuffer.back()->timestamp() : 0)
		                   << "} ). Time waiting: "
		                   << TimeUtils::GetElapsedTimeMicroseconds(requestReceiver_->GetRequestTime(seq)) << " us "
		                   << "(> " << window_close_timeout_us_ << " us).";
	}
	if (windowClosed || !data_thread_running_ || windowTimeout)
	{
		TLOG(TLVL_DEBUG) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Creating ContainerFragment for Window-requested Fragments";
		auto newfrag = std::unique_ptr<artdaq::Fragment>(new artdaq::Fragment(seq, id));
		std::pair<artdaq::FragmentPtr, int> pm_frag = std::make_pair(std::move(newfrag), rank);
		frags.emplace_back(std::move(pm_frag));
		frags.back().first->setTimestamp(ts);
		ContainerFragmentLoader cfl(*frags.back().first);

		// In the spirit of NOvA's MegaPool: (RS = Request start (min), RE = Request End (max))
		//  --- | Buffer Start | --- | Buffer End | ---
		//1. RS RE |           |     |            |
		//2. RS |              |  RE |            |
		//3. RS |              |     |            | RE
		//4.    |              | RS RE |          |
		//5.    |              | RS  |            | RE
		//6.    |              |     |            | RS RE
		//
		// If RE (or RS) is after the end of the buffer, we wait for window_close_timeout_us_. If we're here, then that means that windowClosed is false, and the missing_data flag should be set.
		// If RS (or RE) is before the start of the buffer, then missing_data should be set to true, as data is assumed to arrive in the buffer in timestamp order
		// If the dataBuffer has size 0, then windowClosed will be false
		if (!windowClosed || (dataBuffers_[id].DataBuffer.size() > 0 && dataBuffers_[id].DataBuffer.front()->timestamp() > min))
		{
			TLOG(TLVL_DEBUG) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Request window starts before and/or ends after the current data buffer, setting ContainerFragment's missing_data flag!"
			                 << " (requestWindowRange=[" << min << "," << max << "], "
			                 << "buffer={" << (dataBuffers_[id].DataBuffer.size() > 0 ? dataBuffers_[id].DataBuffer.front()->timestamp() : 0) << "-"
			                 << (dataBuffers_[id].DataBuffer.size() > 0 ? dataBuffers_[id].DataBuffer.back()->timestamp() : 0) << "}";
			cfl.set_missing_data(true);
		}

		// Do a little bit more work to decide which fragments to send for a given request
		for (auto it = dataBuffers_[id].DataBuffer.begin(); it != dataBuffers_[id].DataBuffer.end();)
		{
			Fragment::timestamp_t fragT = (*it)->timestamp();
			if (fragT < min || fragT > max || (fragT == max && windowWidth_ > 0))
			{
				++it;
				continue;
			}

			TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode_CheckAndFillDataBuffer: Adding Fragment with timestamp " << (*it)->timestamp() << " to Container";
			cfl.addFragment((*it));

			if (uniqueWindows_)
			{
				dataBuffers_[id].DataBufferDepthBytes -= (*it)->sizeBytes();
				it = dataBuffers_[id].DataBuffer.erase(it);
			}
			else
			{
				++it;
			}
		}

		dataBuffers_[id].WindowsSent[seq] = std::chrono::steady_clock::now();
		if (seq > dataBuffers_[id].HighestRequestSeen) dataBuffers_[id].HighestRequestSeen = seq;
	}
}

void artdaq::CommandableFragmentGenerator::applyRequestsWindowMode(artdaq::PostmarkedFragmentPtrs& frags)
{
	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode BEGIN";

	auto requests = requestReceiver_->GetRequests();

	TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode: Starting request processing";
	for (auto req = requests.begin(); req != requests.end();)
	{
		TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode: processing request with sequence ID " << req->first << ", timestamp " << req->second.timestamp;

		while (req->first < ev_counter() && requests.size() > 0)
		{
			TLOG(TLVL_APPLYREQUESTS) << "applyRequestsWindowMode: Clearing passed request for sequence ID " << req->first;
			requestReceiver_->RemoveRequest(req->first);
			req = requests.erase(req);
		}
		if (requests.size() == 0) break;

		for (auto& id : dataBuffers_)
		{
			if (!id.second.WindowsSent.count(req->first))
			{
				applyRequestsWindowMode_CheckAndFillDataBuffer(frags, id.first, req->first, req->second.timestamp, req->second.rank);
			}
		}
		checkSentWindows(req->first);
		++req;
	}

	// Check sent windows for requests that can be removed
	for (auto& id : dataBuffers_)
	{
		std::set<artdaq::Fragment::sequence_id_t> seqs;
		for (auto& seq : id.second.WindowsSent)
		{
			seqs.insert(seq.first);
		}
		for (auto& seq : seqs)
		{
			checkSentWindows(seq);
		}
	}
}

bool artdaq::CommandableFragmentGenerator::applyRequests(artdaq::PostmarkedFragmentPtrs& frags)
{
	if (check_stop() || exception())
	{
		return false;
	}

	// Wait for data, if in ignored mode, or a request otherwise
	if (mode_ == RequestMode::Ignored)
	{
		while (dataBufferFragmentCount_() == 0)
		{
			if (check_stop() || exception() || !isHardwareOK_) return false;
			std::unique_lock<std::mutex> lock(dataBuffersMutex_);
			dataCondition_.wait_for(lock, std::chrono::milliseconds(10), [this]() { return dataBufferFragmentCount_() > 0; });
		}
	}
	else
	{
		if ((check_stop() && requestReceiver_->size() == 0) || exception()) return false;

		std::unique_lock<std::mutex> lock(dataBuffersMutex_);
		dataCondition_.wait_for(lock, std::chrono::milliseconds(10));

		checkDataBuffers();

		// Wait up to 1000 ms for a request...
		auto counter = 0;

		while (requestReceiver_->size() == 0 && counter < 100)
		{
			if (check_stop() || exception()) return false;

			checkDataBuffers();

			requestReceiver_->WaitForRequests(10);  // milliseconds
			counter++;
		}
	}

	{
		std::unique_lock<std::mutex> dlk(dataBuffersMutex_);

		switch (mode_)
		{
			case RequestMode::Single:
				applyRequestsSingleMode(frags);
				break;
			case RequestMode::Window:
				applyRequestsWindowMode(frags);
				break;
			case RequestMode::Buffer:
				applyRequestsBufferMode(frags);
				break;
			case RequestMode::Ignored:
			default:
				applyRequestsIgnoredMode(frags);
				break;
		}

		if (!data_thread_running_ || force_stop_)
		{
			TLOG(TLVL_INFO) << "Data thread has stopped; Clearing data buffers";
			for (auto& id : dataBuffers_)
			{
				id.second.DataBufferDepthBytes = 0;
				id.second.DataBufferDepthFragments = 0;
				id.second.DataBuffer.clear();
			}
		}

		getDataBuffersStats();
	}

	if (frags.size() > 0)
		TLOG(TLVL_APPLYREQUESTS) << "Finished Processing requests, returning " << frags.size() << " fragments, current ev_counter is " << ev_counter();
	return true;
}

bool artdaq::CommandableFragmentGenerator::sendEmptyFragment(artdaq::PostmarkedFragmentPtrs& frags, size_t seqId, Fragment::fragment_id_t fragmentId, std::string desc)
{
	TLOG(TLVL_WARNING) << desc << " sequence ID " << seqId << ", sending empty fragment";
	auto frag = std::unique_ptr<artdaq::Fragment>(new artdaq::Fragment());
	frag->setSequenceID(seqId);
	frag->setFragmentID(fragmentId);
	frag->setSystemType(Fragment::EmptyFragmentType);
	std::pair<artdaq::FragmentPtr, int> pm_frag = std::make_pair(std::move(frag), artdaq::Fragment::InvalidDestinationRank);
	frags.emplace_back(std::move(pm_frag));
	return true;
}

void artdaq::CommandableFragmentGenerator::sendEmptyFragments(artdaq::PostmarkedFragmentPtrs& frags, std::map<Fragment::sequence_id_t, artdaq::detail::RequestPacket>& requests)
{
	if (requests.size() > 0)
	{
		TLOG(TLVL_SENDEMPTYFRAGMENTS) << "Sending Empty Fragments for Sequence IDs from " << ev_counter() << " up to but not including " << requests.begin()->first;
		while (requests.begin()->first > ev_counter())
		{
			for (auto& fid : dataBuffers_)
			{
				sendEmptyFragment(frags, ev_counter(), fid.first, "Missed request for");
			}
			ev_counter_inc(1, true);
		}
	}
}

void artdaq::CommandableFragmentGenerator::checkSentWindows(artdaq::Fragment::sequence_id_t seq)
{
	bool seqComplete = true;
	bool seqTimeout = false;
	for (auto& id : dataBuffers_)
	{
		if (!id.second.WindowsSent.count(seq) || id.second.HighestRequestSeen < seq)
		{
			seqComplete = false;
		}
		if (id.second.WindowsSent.count(seq) && TimeUtils::GetElapsedTimeMicroseconds(id.second.WindowsSent[seq]) > missing_request_window_timeout_us_)
		{
			seqTimeout = true;
		}
	}
	if (seqComplete)
	{
		TLOG(TLVL_CHECKWINDOWS) << "checkSentWindows: Request " << seq << " is complete, removing from requestReceiver.";
		requestReceiver_->RemoveRequest(seq);

		if (ev_counter() == seq)
		{
			TLOG(TLVL_CHECKWINDOWS) << "checkSentWindows: Sequence ID matches ev_counter, incrementing ev_counter (" << ev_counter() << ")";

			for (auto& id : dataBuffers_)
			{
				id.second.WindowsSent.erase(seq);
			}

			ev_counter_inc(1, true);
		}
	}
	if (seqTimeout)
	{
		TLOG(TLVL_CHECKWINDOWS) << "checkSentWindows: Sent Window history indicates that requests between " << ev_counter() << " and " << seq << " have timed out.";
		while (ev_counter() <= seq)
		{
			if (ev_counter() < seq) TLOG(TLVL_WARNING) << "Missed request for sequence ID " << ev_counter() << "! Will not send any data for this sequence ID!";
			requestReceiver_->RemoveRequest(ev_counter());

			for (auto& id : dataBuffers_)
			{
				id.second.WindowsSent.erase(ev_counter());
			}

			ev_counter_inc(1, true);
		}
	}
}
