#define TRACE_NAME (app_name + "_CommandableFragmentGenerator").c_str() // include these 2 first -
#include "artdaq/DAQdata/Globals.hh"

#include "artdaq/Application/CommandableFragmentGenerator.hh"

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <limits>
#include <iterator>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Utilities/SimpleLookupPolicy.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#include <fstream>
#include <iomanip>
#include <iterator>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <sys/poll.h>
#include "artdaq/DAQdata/TCPConnect.hh"

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator()
	: mutex_()
	, requestReceiver_(new RequestReceiver())
	, windowOffset_(0)
	, windowWidth_(0)
	, staleTimeout_(Fragment::InvalidTimestamp)
	, expectedType_(Fragment::EmptyFragmentType)
	, maxFragmentCount_(std::numeric_limits<size_t>::max())
	, uniqueWindows_(true)
	, missing_request_(true)
	, missing_request_time_()
	, last_window_send_time_()
	, last_window_send_time_set_(false)
	, windows_sent_ooo_()
	, missing_request_window_timeout_us_(1000000)
	, window_close_timeout_us_(2000000)
	, useDataThread_(false)
	, sleep_on_no_data_us_(0)
	, data_thread_running_(false)
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(1000)
	, maxDataBufferDepthBytes_(1000)
	, useMonitoringThread_(false)
	, monitoringInterval_(0)
	, lastMonitoringCall_()
	, isHardwareOK_(true)
	, dataBuffer_()
	, newDataBuffer_()
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
	, instance_name_for_metrics_("FragmentGenerator")
	, sleep_on_stop_us_(0)
{}

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(const fhicl::ParameterSet& ps)
	: mutex_()
	, windowOffset_(ps.get<Fragment::timestamp_t>("request_window_offset", 0))
	, windowWidth_(ps.get<Fragment::timestamp_t>("request_window_width", 0))
	, staleTimeout_(ps.get<Fragment::timestamp_t>("stale_request_timeout", 0xFFFFFFFF))
	, expectedType_(ps.get<Fragment::type_t>("expected_fragment_type", Fragment::type_t(Fragment::EmptyFragmentType)))
	, uniqueWindows_(ps.get<bool>("request_windows_are_unique", true))
	, missing_request_(false)
	, missing_request_time_(decltype(missing_request_time_)::max())
	, last_window_send_time_(decltype(last_window_send_time_)::max())
	, last_window_send_time_set_(false)
	, windows_sent_ooo_()
	, missing_request_window_timeout_us_(ps.get<size_t>("missing_request_window_timeout_us", 1000000))
	, window_close_timeout_us_(ps.get<size_t>("window_close_timeout_us", 2000000))
	, useDataThread_(ps.get<bool>("separate_data_thread", false))
	, sleep_on_no_data_us_(ps.get<size_t>("sleep_on_no_data_us", 0))
	, data_thread_running_(false)
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(ps.get<int>("data_buffer_depth_fragments", 1000))
	, maxDataBufferDepthBytes_(ps.get<size_t>("data_buffer_depth_mb", 1000) * 1024 * 1024)
	, useMonitoringThread_(ps.get<bool>("separate_monitoring_thread", false))
	, monitoringInterval_(ps.get<int64_t>("hardware_poll_interval_us", 0))
	, lastMonitoringCall_()
	, isHardwareOK_(true)
	, dataBuffer_()
	, newDataBuffer_()
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

	fragment_ids_ = ps.get<std::vector<artdaq::Fragment::fragment_id_t>>("fragment_ids", std::vector<artdaq::Fragment::fragment_id_t>());

	TLOG(TLVL_TRACE) << "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)" ;
	int fragment_id = ps.get<int>("fragment_id", -99);

	if (fragment_id != -99)
	{
		if (fragment_ids_.size() != 0)
		{
			latest_exception_report_ = "Error in CommandableFragmentGenerator: can't both define \"fragment_id\" and \"fragment_ids\" in FHiCL document";
			throw cet::exception(latest_exception_report_);
		}
		else
		{
			fragment_ids_.emplace_back(fragment_id);
		}
	}

	sleep_on_stop_us_ = ps.get<int>("sleep_on_stop_us", 0);

	dataBuffer_.emplace_back(FragmentPtr(new Fragment()));
	(*dataBuffer_.begin())->setSystemType(Fragment::EmptyFragmentType);

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
	TLOG(TLVL_DEBUG) << "Request mode is " << printMode_() ;

	if (mode_ != RequestMode::Ignored)
	{
		if (!useDataThread_)
		{
			latest_exception_report_ = "Error in CommandableFragmentGenerator: use_data_thread must be true when request_mode is not \"Ignored\"!";
			throw cet::exception(latest_exception_report_);
		}
		requestReceiver_.reset(new RequestReceiver(ps));
	}
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	joinThreads();
}

void artdaq::CommandableFragmentGenerator::joinThreads()
{
	should_stop_ = true;
	force_stop_ = true;
	TLOG(TLVL_DEBUG) << "Joining dataThread" ;
	if (dataThread_.joinable()) dataThread_.join();
	TLOG(TLVL_DEBUG) << "Joining monitoringThread" ;
	if (monitoringThread_.joinable()) monitoringThread_.join();
	requestReceiver_.reset(nullptr);
}

bool artdaq::CommandableFragmentGenerator::getNext(FragmentPtrs& output)
{
	bool result = true;

	if (check_stop()) usleep(sleep_on_stop_us_);
	if (exception() || force_stop_) return false;

	if (!useMonitoringThread_ && monitoringInterval_ > 0)
	{
		TLOG(10) << "getNext: Checking whether to collect Monitoring Data" ;
		auto now = std::chrono::steady_clock::now();

		if (TimeUtils::GetElapsedTimeMicroseconds(lastMonitoringCall_, now) >= static_cast<size_t>(monitoringInterval_))
		{
			TLOG(10) << "getNext: Collecting Monitoring Data" ;
			isHardwareOK_ = checkHWStatus_();
			TLOG(10) << "getNext: isHardwareOK_ is now " << std::boolalpha << isHardwareOK_ ;
			lastMonitoringCall_ = now;
		}
	}

	try
	{
		std::lock_guard<std::mutex> lk(mutex_);
		if (useDataThread_)
		{
			TLOG(TLVL_TRACE) << "getNext: Calling applyRequests" ;
			result = applyRequests(output);
			TLOG(TLVL_TRACE) << "getNext: Done with applyRequests result=" << std::boolalpha << result;

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
				TLOG(TLVL_ERROR) << "Stopping CFG because the hardware reports bad status!" ;
				return false;
			}
			TLOG(TLVL_TRACE) << "getNext: Calling getNext_ " << std::to_string(ev_counter()) ;
			try
			{
				result = getNext_(output);
			}
			catch (...)
			{
				throw;
			}
			TLOG(TLVL_TRACE) << "getNext: Done with getNext_ " << std::to_string(ev_counter()) ;
			for (auto dataIter = output.begin(); dataIter != output.end(); ++dataIter)
			{
			  TLOG(20) << "getNext: getNext_() returned fragment with sequenceID = " << (*dataIter)->sequenceID()
				   << ", timestamp = " << (*dataIter)->timestamp() << ", and sizeBytes = " << (*dataIter)->sizeBytes();
			}
		}
	}
	catch (const cet::exception& e)
	{
		latest_exception_report_ = "cet::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG(TLVL_ERROR) << "getNext: cet::exception caught: " << e ;
		set_exception(true);
		return false;
	}
	catch (const boost::exception& e)
	{
		latest_exception_report_ = "boost::exception caught in getNext(): ";
		latest_exception_report_.append(boost::diagnostic_information(e));
		TLOG(TLVL_ERROR) << "getNext: boost::exception caught: " << boost::diagnostic_information(e) ;
		set_exception(true);
		return false;
	}
	catch (const std::exception& e)
	{
		latest_exception_report_ = "std::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG(TLVL_ERROR) << "getNext: std::exception caught: " << e.what() ;
		set_exception(true);
		return false;
	}
	catch (...)
	{
		latest_exception_report_ = "Unknown exception caught in getNext().";
		TLOG(TLVL_ERROR) << "getNext: unknown exception caught" ;
		set_exception(true);
		return false;
	}

	if (!result)
	{
		TLOG(TLVL_DEBUG) << "stopped " ;
	}

	if (metricMan && !output.empty()) {

          auto timestamp = output.front()->timestamp();

          if (output.size() > 1) { // Only bother sorting if >1 entry                                            
            for (auto& outputfrag : output ) {
              if (outputfrag->timestamp() > timestamp) {
                timestamp = outputfrag->timestamp();
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
	TLOG(14) << "CFG::check_stop: should_stop=" << should_stop() << ", useDataThread_=" << useDataThread_ << ", exception status =" << int(exception()) ;

	if (!should_stop()) return false;
	if (!useDataThread_ || mode_ == RequestMode::Ignored) return true;
	if (force_stop_) return true;

	// check_stop returns true if the CFG should stop. We should wait for the RequestReceiver to stop before stopping.
	return !requestReceiver_->isRunning();
}

int artdaq::CommandableFragmentGenerator::fragment_id() const
{
	if (fragment_ids_.size() != 1)
	{
		throw cet::exception("Error in CommandableFragmentGenerator: can't call fragment_id() unless member fragment_ids_ vector is length 1");
	}
	else
	{
		return fragment_ids_[0];
	}
}

size_t artdaq::CommandableFragmentGenerator::ev_counter_inc(size_t step, bool force)
{
	if (force || mode_ == RequestMode::Ignored)
	{
		return ev_counter_.fetch_add(step);
	}
	return ev_counter_.load();
} // returns the prev value

void artdaq::CommandableFragmentGenerator::StartCmd(int run, uint64_t timeout, uint64_t timestamp)
{
	if (run < 0) throw cet::exception("CommandableFragmentGenerator") << "negative run number";

	timeout_ = timeout;
	timestamp_ = timestamp;
	ev_counter_.store(1);
	missing_request_ = false;
	should_stop_.store(false);
	exception_.store(false);
	run_number_ = run;
	subrun_number_ = 1;
	latest_exception_report_ = "none";
	dataBuffer_.clear();
	last_window_send_time_set_ = false; 
	windows_sent_ooo_.clear();

	start();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (mode_ != RequestMode::Ignored && !requestReceiver_->isRunning()) requestReceiver_->startRequestReceiverThread();
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_DEBUG) << "Stop Command received." ;

	timeout_ = timeout;
	timestamp_ = timestamp;
	if (requestReceiver_ && requestReceiver_->isRunning()) requestReceiver_->stopRequestReceiverThread();

	stopNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);
	stop();
	TLOG(TLVL_DEBUG) << "Stop command complete.";
}

void artdaq::CommandableFragmentGenerator::PauseCmd(uint64_t timeout, uint64_t timestamp)
{
	timeout_ = timeout;
	timestamp_ = timestamp;
	if (requestReceiver_->isRunning()) requestReceiver_->stopRequestReceiverThread();

	pauseNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);

	pause();
}

void artdaq::CommandableFragmentGenerator::ResumeCmd(uint64_t timeout, uint64_t timestamp)
{
	timeout_ = timeout;
	timestamp_ = timestamp;

	subrun_number_ += 1;
	should_stop_ = false;

	dataBuffer_.clear();

	// no lock required: thread not started yet
	resume();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (mode_ != RequestMode::Ignored && !requestReceiver_->isRunning()) requestReceiver_->startRequestReceiverThread();
}

std::string artdaq::CommandableFragmentGenerator::ReportCmd(std::string const& which)
{
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

void artdaq::CommandableFragmentGenerator::resume()
{
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
	TLOG(TLVL_INFO) << "Starting Data Receiver Thread" ;
	dataThread_ = boost::thread(&CommandableFragmentGenerator::getDataLoop, this);
}

void artdaq::CommandableFragmentGenerator::startMonitoringThread()
{
	if (monitoringThread_.joinable()) monitoringThread_.join();
	TLOG(TLVL_INFO) << "Starting Hardware Monitoring Thread" ;
	monitoringThread_ = boost::thread(&CommandableFragmentGenerator::getMonitoringDataLoop, this);
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

void artdaq::CommandableFragmentGenerator::getDataLoop()
{
	data_thread_running_ = true;
	while (!force_stop_)
	{
		if (!isHardwareOK_)
		{
			TLOG(TLVL_DEBUG) << "getDataLoop: isHardwareOK is " << isHardwareOK_ << ", aborting data thread" ;
			data_thread_running_ = false;
			return;
		}

		TLOG(13) << "getDataLoop: calling getNext_" ;

		bool data = false;
		auto startdata = std::chrono::steady_clock::now();

		try
		{
			data = getNext_(newDataBuffer_);
		}
		catch (...)
		{
			ExceptionHandler(ExceptionHandlerRethrow::no,
				"Exception thrown by fragment generator in CommandableFragmentGenerator::getDataLoop; setting exception state to \"true\"");
			set_exception(true);

			data_thread_running_ = false;
			return;
		}
		for (auto dataIter = newDataBuffer_.begin(); dataIter != newDataBuffer_.end(); ++dataIter)
		{
		  TLOG(20) << "getDataLoop: getNext_() returned fragment with sequenceID = " << (*dataIter)->sequenceID()
			   << ", timestamp = " << (*dataIter)->timestamp() << ", and sizeBytes = " << (*dataIter)->sizeBytes();
		}

		if (metricMan)
		{
			metricMan->sendMetric("Avg Data Acquisition Time", TimeUtils::GetElapsedTime(startdata), "s", 3, artdaq::MetricMode::Average);
		}

		if (newDataBuffer_.size() == 0 && sleep_on_no_data_us_ > 0)
		{
			usleep(sleep_on_no_data_us_);
		}

		TLOG(15) << "Waiting for data buffer ready" ;
		if (!waitForDataBufferReady()) return;
		TLOG(15) << "Done waiting for data buffer ready" ;

		TLOG(13) << "getDataLoop: processing data" ;
		if (data && !force_stop_)
		{
			std::unique_lock<std::mutex> lock(dataBufferMutex_);
			switch (mode_)
			{
			case RequestMode::Single:
				// While here, if for some strange reason more than one event's worth of data is returned from getNext_...
				while (newDataBuffer_.size() >= fragment_ids_.size())
				{
					dataBuffer_.clear();
					auto it = newDataBuffer_.begin();
					std::advance(it, fragment_ids_.size());
					dataBuffer_.splice(dataBuffer_.end(), newDataBuffer_, newDataBuffer_.begin(), it);
				}
				break;
			case RequestMode::Buffer:
			case RequestMode::Ignored:
			case RequestMode::Window:
			default:
				//dataBuffer_.reserve(dataBuffer_.size() + newDataBuffer_.size());
				dataBuffer_.splice(dataBuffer_.end(), newDataBuffer_);
				break;
			}
			getDataBufferStats();
		}

		{
			std::unique_lock<std::mutex> lock(dataBufferMutex_);
			if (dataBuffer_.size() > 0)
			{
				dataCondition_.notify_all();
			}
		}
		if (!data || force_stop_)
		{
			TLOG(TLVL_INFO) << "Data flow has stopped. Ending data collection thread" ;
			data_thread_running_ = false;
			if (requestReceiver_) requestReceiver_->ClearRequests();
			dataBuffer_.clear();
			newDataBuffer_.clear();
			return;
		}
	}
}

bool artdaq::CommandableFragmentGenerator::waitForDataBufferReady()
{
	auto startwait = std::chrono::steady_clock::now();
	auto first = true;
	auto lastwaittime = 0ULL;
	while (dataBufferIsTooLarge())
	{
		if (should_stop())
		{
			TLOG(TLVL_DEBUG) << "Run ended while waiting for buffer to shrink!" ;
			std::unique_lock<std::mutex> lock(dataBufferMutex_);
			getDataBufferStats();
			dataCondition_.notify_all();
			data_thread_running_ = false;
			return false;
		}
		auto waittime = TimeUtils::GetElapsedTimeMilliseconds(startwait);

		if (first || (waittime != lastwaittime && waittime % 1000 == 0))
		{
			TLOG(TLVL_WARNING) << "Bad Omen: Data Buffer has exceeded its size limits. "
				<< "(seq_id=" << ev_counter()
				<< ", frags=" << dataBufferDepthFragments_ << "/" << maxDataBufferDepthFragments_
				<< ", szB=" << dataBufferDepthBytes_ << "/" << maxDataBufferDepthBytes_ << ")" ;
			TLOG(TLVL_TRACE) << "Bad Omen: Possible causes include requests not getting through or Ignored-mode BR issues" ;
			first = false;
		}
		if (waittime % 5 && waittime != lastwaittime)
		{
			TLOG(13) << "getDataLoop: Data Retreival paused for " << std::to_string(waittime) << " ms waiting for data buffer to drain" ;
		}
		lastwaittime = waittime;
		usleep(1000);
	}
	return true;
}

bool artdaq::CommandableFragmentGenerator::dataBufferIsTooLarge()
{
	return (maxDataBufferDepthFragments_ > 0 && dataBufferDepthFragments_ > maxDataBufferDepthFragments_) || (maxDataBufferDepthBytes_ > 0 && dataBufferDepthBytes_ > maxDataBufferDepthBytes_);
}

void artdaq::CommandableFragmentGenerator::getDataBufferStats()
{
	/// dataBufferMutex must be owned by the calling thread!
	dataBufferDepthFragments_ = dataBuffer_.size();
	size_t acc = 0;
	TLOG(15) << "getDataBufferStats: Calculating buffer size" ;
	for (auto i = dataBuffer_.begin(); i != dataBuffer_.end(); ++i)
	{
		if (i->get() != nullptr)
		{
			acc += (*i)->sizeBytes();
		}
	}
	dataBufferDepthBytes_ = acc;

	if (metricMan)
	{
		TLOG(15) << "getDataBufferStats: Sending Metrics" ;
		metricMan->sendMetric("Buffer Depth Fragments", dataBufferDepthFragments_.load(), "fragments", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Buffer Depth Bytes", dataBufferDepthBytes_.load(), "bytes", 1, MetricMode::LastPoint);
	}
	TLOG(15) << "getDataBufferStats: frags=" << dataBufferDepthFragments_.load() << "/" << maxDataBufferDepthFragments_
		<< ", sz=" << std::to_string(dataBufferDepthBytes_.load()) << "/" << std::to_string(maxDataBufferDepthBytes_) ;
}

void artdaq::CommandableFragmentGenerator::checkDataBuffer()
{
	std::unique_lock<std::mutex> lock(dataBufferMutex_);
	dataCondition_.wait_for(lock, std::chrono::milliseconds(10));
	if (dataBufferDepthFragments_ > 0)
	{
		if ((mode_ == RequestMode::Buffer || mode_ == RequestMode::Window))
		{
			// Eliminate extra fragments
			while (dataBufferIsTooLarge())
			{
				dataBuffer_.erase(dataBuffer_.begin());
				getDataBufferStats();
			}
			if (dataBuffer_.size() > 0)
			{
				TLOG(17) << "Determining if Fragments can be dropped from data buffer" ;
				Fragment::timestamp_t last = dataBuffer_.back()->timestamp();
				Fragment::timestamp_t min = last > staleTimeout_ ? last - staleTimeout_ : 0;
				for (auto it = dataBuffer_.begin(); it != dataBuffer_.end();)
				{
					if ((*it)->timestamp() < min)
					{
						it = dataBuffer_.erase(it);
					}
					else
					{
						++it;
					}
				}
				getDataBufferStats();
			}
		}
		else if (mode_ == RequestMode::Single && dataBuffer_.size() > fragment_ids_.size())
		{
			// Eliminate extra fragments
			while (dataBuffer_.size() > fragment_ids_.size())
			{
				dataBuffer_.erase(dataBuffer_.begin());
			}
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
				<< " and monitoringInterval is " << monitoringInterval_ << ", returning" ;
			return;
		}
		TLOG(12) << "getMonitoringDataLoop: Determining whether to call checkHWStatus_" ;

		auto now = std::chrono::steady_clock::now();
		if (TimeUtils::GetElapsedTimeMicroseconds(lastMonitoringCall_, now) >= static_cast<size_t>(monitoringInterval_))
		{
			isHardwareOK_ = checkHWStatus_();
			TLOG(12) << "getMonitoringDataLoop: isHardwareOK_ is now " << std::boolalpha << isHardwareOK_ ;
			lastMonitoringCall_ = now;
		}
		usleep(monitoringInterval_ / 10);
	}
}

void artdaq::CommandableFragmentGenerator::applyRequestsIgnoredMode(artdaq::FragmentPtrs& frags)
{
	// We just copy everything that's here into the output.
	TLOG(9) << "Mode is Ignored; Copying data to output" ;
	std::move(dataBuffer_.begin(), dataBuffer_.end(), std::inserter(frags, frags.end()));
	dataBuffer_.clear();
}

void artdaq::CommandableFragmentGenerator::applyRequestsSingleMode(artdaq::FragmentPtrs& frags)
{
	// We only care about the latest request received. Send empties for all others.
	auto requests = requestReceiver_->GetRequests();
	while (requests.size() > 1) {
		// std::map is ordered by key => Last sequence ID in the map is the one we care about
		requestReceiver_->RemoveRequest(requests.begin()->first);
		requests.erase(requests.begin());
	}
	sendEmptyFragments(frags, requests);

	// If no requests remain after sendEmptyFragments, return
	if (requests.size() == 0 || !requests.count(ev_counter())) return;

	if (dataBuffer_.size() > 0)
	{
		TLOG(9) << "Mode is Single; Sending copy of last event" ;
		for (auto& fragptr : dataBuffer_)
		{
			// Return the latest data point
			auto frag = fragptr.get();
			auto newfrag = std::unique_ptr<artdaq::Fragment>(new Fragment(ev_counter(), frag->fragmentID()));
			newfrag->resize(frag->size() - detail::RawFragmentHeader::num_words());
			memcpy(newfrag->headerAddress(), frag->headerAddress(), frag->sizeBytes());
			newfrag->setTimestamp(requests[ev_counter()]);
			newfrag->setSequenceID(ev_counter());
			frags.push_back(std::move(newfrag));
		}
	}
	else
	{
		sendEmptyFragment(frags, ev_counter(), "No data for");
	}
	requestReceiver_->RemoveRequest(ev_counter());
	ev_counter_inc(1, true);
}

void artdaq::CommandableFragmentGenerator::applyRequestsBufferMode(artdaq::FragmentPtrs& frags)
{
	// We only care about the latest request received. Send empties for all others.
	auto requests = requestReceiver_->GetRequests();
	while (requests.size() > 1) {
		// std::map is ordered by key => Last sequence ID in the map is the one we care about
		requestReceiver_->RemoveRequest(requests.begin()->first);
		requests.erase(requests.begin());
	}
	sendEmptyFragments(frags, requests);

	// If no requests remain after sendEmptyFragments, return
	if (requests.size() == 0 || !requests.count(ev_counter())) return;

	TLOG(TLVL_DEBUG) << "Creating ContainerFragment for Buffered Fragments" ;
	frags.emplace_back(new artdaq::Fragment(ev_counter(), fragment_id()));
	frags.back()->setTimestamp(requests[ev_counter()]);
	ContainerFragmentLoader cfl(*frags.back());
	cfl.set_missing_data(false); // Buffer mode is never missing data, even if there IS no data.

	// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
	// Window mode TFGs must do a little bit more work to decide which fragments to send for a given request
	for (auto it = dataBuffer_.begin(); it != dataBuffer_.end();)
	{
		TLOG(9) << "ApplyRequests: Adding Fragment with timestamp " << std::to_string((*it)->timestamp()) << " to Container" ;
		cfl.addFragment(*it);
		it = dataBuffer_.erase(it);
	}
	requestReceiver_->RemoveRequest(ev_counter());
	ev_counter_inc(1, true);
}

void artdaq::CommandableFragmentGenerator::applyRequestsWindowMode(artdaq::FragmentPtrs& frags)
{
	TLOG(10) << "applyRequestsWindowMode BEGIN";
	if (!last_window_send_time_set_)
	{
		last_window_send_time_ = std::chrono::steady_clock::now();
		last_window_send_time_set_ = true;
	}

	auto requests = requestReceiver_->GetRequests();
	bool now_have_desired_request = std::any_of(requests.begin(), requests.end(),
		[this](decltype(requests)::value_type& request) {
		return request.first == ev_counter(); });

	if (missing_request_)
	{
		if (!now_have_desired_request &&  TimeUtils::GetElapsedTimeMicroseconds(missing_request_time_) > missing_request_window_timeout_us_)
		{
			TLOG(TLVL_ERROR) << "Data-taking has paused for " << TimeUtils::GetElapsedTimeMicroseconds(missing_request_time_) << " us "
				<< "(> " << std::to_string(missing_request_window_timeout_us_) << " us) while waiting for missing data request messages."
				<< " Sending Empty Fragments for missing requests!" ;
			sendEmptyFragments(frags, requests);

			missing_request_ = false;
			missing_request_time_ = decltype(missing_request_time_)::max();
		}
		else if (now_have_desired_request) {
			missing_request_ = false;
			missing_request_time_ = decltype(missing_request_time_)::max();
		}
	}

	TLOG(10) << "applyRequestsWindowMode: Starting request processing";
	for (auto req = requests.begin(); req != requests.end();)
	{
	  TLOG(10, "CommandableFragmentGenerator") << "applyRequestsWindowMode: processing request with sequence ID " << \
	               req->first << ", timestamp " << req->second;


		while (req->first < ev_counter() && requests.size() > 0)
		{
			TLOG(10) << "applyRequestsWindowMode: Clearing passed request for sequence ID " << req->first;
			requestReceiver_->RemoveRequest(req->first);
			req = requests.erase(req);
		}
		if (requests.size() == 0) break;
		if (req->first > ev_counter())
		{
			if (!missing_request_)
			{
				missing_request_ = true;
				missing_request_time_ = std::chrono::steady_clock::now();
			}
		}
		auto ts = req->second;
		TLOG(9) << "ApplyRequests: Checking that data exists for request window " << std::to_string(req->first) ;
		Fragment::timestamp_t min = ts > windowOffset_ ? ts - windowOffset_ : 0;
		Fragment::timestamp_t max = min + windowWidth_;
		TLOG(9) << "ApplyRequests: min is " << std::to_string(min) << ", max is " << std::to_string(max)
			<< " and last point in buffer is " << std::to_string((dataBuffer_.size() > 0 ? dataBuffer_.back()->timestamp() : 0)) << " (sz=" << std::to_string(dataBuffer_.size()) << ")" ;
		bool windowClosed = dataBuffer_.size() > 0 && dataBuffer_.back()->timestamp() >= max;
		bool windowTimeout = TimeUtils::GetElapsedTimeMicroseconds(last_window_send_time_) > window_close_timeout_us_;
		if (windowTimeout)
		{
			TLOG(TLVL_WARNING) << "A timeout occurred waiting for data to close the request window (max=" << std::to_string(max)
				<< ", buffer=" << std::to_string((dataBuffer_.size() > 0 ? dataBuffer_.back()->timestamp() : 0))
				<< " (if no buffer in memory, this is shown as a 0)). Time waiting: "
				<< TimeUtils::GetElapsedTimeMicroseconds(last_window_send_time_) << " us "
				<< "(> " << std::to_string(window_close_timeout_us_) << " us)." ;

			if (missing_request_) {
				TLOG(TLVL_ERROR) << "A Window timeout has occurred while there are pending requests. Sending empties." ;
				sendEmptyFragments(frags, requests);
			}
		}
		if (windowClosed || !data_thread_running_ || windowTimeout)
		{
			TLOG(TLVL_DEBUG) << "Creating ContainerFragment for Buffered or Window-requested Fragments" ;
			frags.emplace_back(new artdaq::Fragment(req->first, fragment_id()));
			frags.back()->setTimestamp(ts);
			ContainerFragmentLoader cfl(*frags.back());

			if (!windowClosed) cfl.set_missing_data(true);
			if (dataBuffer_.size() > 0 && dataBuffer_.front()->timestamp() > min)
			{
				TLOG(TLVL_DEBUG) << "Request Window covers data that is either before data collection began or has fallen off the end of the buffer" ;
				cfl.set_missing_data(true);
			}

			// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
			// Window mode TFGs must do a little bit more work to decide which fragments to send for a given request
			for (auto it = dataBuffer_.begin(); it != dataBuffer_.end();)
			{
				Fragment::timestamp_t fragT = (*it)->timestamp();
				if (fragT < min || fragT > max || (fragT == max && windowWidth_ > 0))
				{
					++it;
					continue;
				}

				TLOG(9) << "ApplyRequests: Adding Fragment with timestamp " << std::to_string((*it)->timestamp()) << " to Container" ;
				cfl.addFragment(*it);

				if (uniqueWindows_)
				{
					it = dataBuffer_.erase(it);
				}
				else
				{
					++it;
				}
			}
			if (req->first == ev_counter())
			{
				ev_counter_inc(1, true);
				while (windows_sent_ooo_.count(ev_counter()))
				{
					TLOG(9) << "Data-taking has caught up to out-of-order window request " << ev_counter() << ", removing from list" ;
					windows_sent_ooo_.erase(windows_sent_ooo_.begin(), windows_sent_ooo_.find(ev_counter()));
					ev_counter_inc(1, true);
				}
			}
			else
			{
				windows_sent_ooo_.insert(req->first);
			}
			requestReceiver_->RemoveRequest(req->first);
			req = requests.erase(req);
			last_window_send_time_ = std::chrono::steady_clock::now();
		}
		else
		{
			++req;
		}
	}
}

bool artdaq::CommandableFragmentGenerator::applyRequests(artdaq::FragmentPtrs& frags)
{
	if (check_stop() || exception())
	{
		return false;
	}

	// Wait for data, if in ignored mode, or a request otherwise
	if (mode_ == RequestMode::Ignored)
	{
		while (dataBufferDepthFragments_ <= 0)
		{
			if (check_stop() || exception() || !isHardwareOK_) return false;
			std::unique_lock<std::mutex> lock(dataBufferMutex_);
			dataCondition_.wait_for(lock, std::chrono::milliseconds(10), [this]() { return dataBufferDepthFragments_ > 0; });
		}
	}
	else
	{
		if ((check_stop() && requestReceiver_->size() == 0) || exception()) return false;
		checkDataBuffer();

		// Wait up to 1000 ms for a request...
		auto counter = 0;

		while (requestReceiver_->size() == 0 && counter < 100)
		{
			if (check_stop() || exception()) return false;

			checkDataBuffer();

			requestReceiver_->WaitForRequests(10); // milliseconds
			counter++;
		}
	}

	{
		std::unique_lock<std::mutex> dlk(dataBufferMutex_);

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

		getDataBufferStats();
	}

	if (frags.size() > 0)
		TLOG(9) << "Finished Processing Event " << std::to_string(ev_counter() + 1) << " for fragment_id " << fragment_id() << "." ;
	return true;
}

bool artdaq::CommandableFragmentGenerator::sendEmptyFragment(artdaq::FragmentPtrs& frags, size_t seqId, std::string desc)
{
	TLOG(TLVL_WARNING) << desc << " sequence ID " << seqId << ", sending empty fragment" ;
	for (auto fid : fragment_ids_)
	{
		auto frag = new Fragment();
		frag->setSequenceID(seqId);
		frag->setFragmentID(fid);
		frag->setSystemType(Fragment::EmptyFragmentType);
		frags.emplace_back(FragmentPtr(frag));
	}
	return true;
}

void artdaq::CommandableFragmentGenerator::sendEmptyFragments(artdaq::FragmentPtrs& frags, std::map<Fragment::sequence_id_t, Fragment::timestamp_t>& requests)
{
	if (requests.size() == 0 && windows_sent_ooo_.size() == 0) return;

	if (requests.size() > 0) {
		TLOG(19) << "Sending Empty Fragments for Sequence IDs from " << ev_counter() << " up to but not including " << requests.begin()->first ;
		while (requests.begin()->first > ev_counter())
		{
			sendEmptyFragment(frags, ev_counter(), "Missed request for");
			ev_counter_inc(1, true);
		}
	}
	else if (windows_sent_ooo_.size() > 0)
	{
		TLOG(19) << "Sending Empty Fragments for Sequence IDs from " << ev_counter() << " up to but not including " << *windows_sent_ooo_.begin() ;
		while (*windows_sent_ooo_.begin() > ev_counter())
		{
			sendEmptyFragment(frags, ev_counter(), "Missed request for");
			ev_counter_inc(1, true);
		}
	}
	while (windows_sent_ooo_.count(ev_counter()))
	{
		TLOG(19) << "Data-taking has caught up to out-of-order window request " << ev_counter() << ", removing from list" ;
		windows_sent_ooo_.erase(windows_sent_ooo_.begin(), windows_sent_ooo_.find(ev_counter()));
		ev_counter_inc(1, true);
	}
}
