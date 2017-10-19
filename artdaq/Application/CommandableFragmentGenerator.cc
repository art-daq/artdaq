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
#include <sys/poll.h>
#include "artdaq/DAQdata/TCPConnect.hh"

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator()
	: mutex_()
	, request_port_(3001)
	, request_addr_("227.128.12.26")
	, requests_()
	, request_stop_requested_(false)
	, request_received_(false)
	, end_of_run_timeout_ms_(1000)
	, windowOffset_(0)
	, windowWidth_(0)
	, staleTimeout_(Fragment::InvalidTimestamp)
	, expectedType_(Fragment::EmptyFragmentType)
	, maxFragmentCount_(std::numeric_limits<size_t>::max())
	, uniqueWindows_(true)
	, last_window_send_time_()
	, last_window_send_time_set_(false)
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
	, request_port_(ps.get<int>("request_port", 3001))
	, request_addr_(ps.get<std::string>("request_address", "227.128.12.26"))
	, requests_()
	, request_stop_requested_(false)
	, request_received_(false)
	, end_of_run_timeout_ms_(ps.get<size_t>("end_of_run_quiet_timeout_ms", 1000))
	, windowOffset_(ps.get<Fragment::timestamp_t>("request_window_offset", 0))
	, windowWidth_(ps.get<Fragment::timestamp_t>("request_window_width", 0))
	, staleTimeout_(ps.get<Fragment::timestamp_t>("stale_request_timeout", 0xFFFFFFFF))
	, expectedType_(ps.get<Fragment::type_t>("expected_fragment_type", Fragment::type_t(Fragment::EmptyFragmentType)))
	, uniqueWindows_(ps.get<bool>("request_windows_are_unique", true))
        , last_window_send_time_(decltype(last_window_send_time_)::max())
        , last_window_send_time_set_(false)
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

	TLOG_TRACE("CommandableFragmentGenerator") << "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)" << TLOG_ENDL;
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
	TLOG_DEBUG("CommandableFragmentGenerator") << "Request mode is " << printMode_() << TLOG_ENDL;

	if (mode_ != RequestMode::Ignored)
	{
		if (!useDataThread_)
		{
			latest_exception_report_ = "Error in CommandableFragmentGenerator: use_data_thread must be true when request_mode is not \"Ignored\"!";
			throw cet::exception(latest_exception_report_);
		}
		setupRequestListener();
	}
}

void artdaq::CommandableFragmentGenerator::setupRequestListener()
{
	request_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (!request_socket_)
	{
		throw art::Exception(art::errors::Configuration) << "CommandableFragmentGenerator: Error creating socket for receiving data requests! err=" << strerror(errno) << std::endl;
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(request_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		throw art::Exception(art::errors::Configuration) <<
			"RequestedFragmentGenrator: Unable to enable port reuse on request socket, err=" << strerror(errno) << std::endl;
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(request_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(request_socket_, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
	{
		throw art::Exception(art::errors::Configuration) <<
			"CommandableFragmentGenerator: Cannot bind request socket to port " << request_port_ << ", err=" << strerror(errno) << std::endl;
		exit(1);
	}

	if (request_addr_ != "localhost")
	{
		struct ip_mreq mreq;
		int sts = ResolveHost(request_addr_.c_str(), mreq.imr_multiaddr);
		if (sts == -1)
		{
			throw art::Exception(art::errors::Configuration) << "Unable to resolve multicast request address, err=" << strerror(errno) << std::endl;
			exit(1);
		}
		mreq.imr_interface.s_addr = htonl(INADDR_ANY);
		if (setsockopt(request_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
		{
			throw art::Exception(art::errors::Configuration) <<
				"CommandableFragmentGenerator: Unable to join multicast group, err=" << strerror(errno) << std::endl;
			exit(1);
		}
	}
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	force_stop_ = true;
	should_stop_ = true;
	TLOG_DEBUG("CommandableFragmentGenerator") << "Joining dataThread" << TLOG_ENDL;
	if (dataThread_.joinable()) dataThread_.join();
	TLOG_DEBUG("CommandableFragmentGenerator") << "Joining monitoringThread" << TLOG_ENDL;
	if (monitoringThread_.joinable()) monitoringThread_.join();
	TLOG_DEBUG("CommandableFragmentGenerator") << "Joining requestThread" << TLOG_ENDL;
	if (requestThread_.joinable()) requestThread_.join();
	if (request_socket_ != -1) close(request_socket_);
}

bool artdaq::CommandableFragmentGenerator::getNext(FragmentPtrs& output)
{
	bool result = true;

	if (check_stop()) usleep(sleep_on_stop_us_);
	if (exception() || force_stop_) return false;

	if (!useMonitoringThread_ && monitoringInterval_ > 0)
	{
		TLOG_ARB(10, "CommandableFragmentGenerator") << "getNext: Checking whether to collect Monitoring Data" << TLOG_ENDL;
		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_)
		{
			TLOG_ARB(10, "CommandableFragmentGenerator") << "getNext: Collecting Monitoring Data" << TLOG_ENDL;
			isHardwareOK_ = checkHWStatus_();
			TLOG_ARB(10, "CommandableFragmentGenerator") << "getNext: isHardwareOK_ is now " << std::boolalpha << isHardwareOK_ << TLOG_ENDL;
			lastMonitoringCall_ = now;
		}
	}

	try
	{
		std::lock_guard<std::mutex> lk(mutex_);
		if (useDataThread_)
		{
			TLOG_TRACE("CommandableFragmentGenerator") << "getNext: Calling applyRequests" << TLOG_ENDL;
			result = applyRequests(output);
			TLOG_TRACE("CommandableFragmentGenerator") << "getNext: Done with applyRequests" << TLOG_ENDL;

			if (exception())
			{
				throw cet::exception("CommandableFragmentGenerator") << "Exception found in BoardReader with board ID " << board_id() << "; BoardReader will now return error status when queried";
			}
		}
		else
		{
			if (!isHardwareOK_)
			{
				TLOG_ERROR("CommandableFragmentGenerator") << "Stopping CFG because the hardware reports bad status!" << TLOG_ENDL;
				return false;
			}
			TLOG_TRACE("CommandableFragmentGenerator") << "getNext: Calling getNext_ " << std::to_string(ev_counter()) << TLOG_ENDL;
			try
			{
				result = getNext_(output);
			}
			catch (...)
			{
				throw;
			}
			TLOG_TRACE("CommandableFragmentGenerator") << "getNext: Done with getNext_ " << std::to_string(ev_counter()) << TLOG_ENDL;
		}
	}
	catch (const cet::exception& e)
	{
		latest_exception_report_ = "cet::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG_ERROR("CommandableFragmentGenerator") << "getNext: cet::exception caught: " << e << TLOG_ENDL;
		set_exception(true);
		return false;
	}
	catch (const boost::exception& e)
	{
		latest_exception_report_ = "boost::exception caught in getNext(): ";
		latest_exception_report_.append(boost::diagnostic_information(e));
		TLOG_ERROR("CommandableFragmentGenerator") << "getNext: boost::exception caught: " << boost::diagnostic_information(e) << TLOG_ENDL;
		set_exception(true);
		return false;
	}
	catch (const std::exception& e)
	{
		latest_exception_report_ = "std::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG_ERROR("CommandableFragmentGenerator") << "getNext: std::exception caught: " << e.what() << TLOG_ENDL;
		set_exception(true);
		return false;
	}
	catch (...)
	{
		latest_exception_report_ = "Unknown exception caught in getNext().";
		TLOG_ERROR("CommandableFragmentGenerator") << "getNext: unknown exception caught" << TLOG_ENDL;
		set_exception(true);
		return false;
	}

	if (!result)
	{
		TLOG_DEBUG("getNext") << "stopped " << TLOG_ENDL;
	}

	return result;
}

bool artdaq::CommandableFragmentGenerator::check_stop()
{
	TLOG_ARB(14, "CommandableFragmentGeneraotr") << "CFG::check_stop: should_stop=" << should_stop() << ", useDataThread_=" << useDataThread_ << ", requests_.size()=" << std::to_string(requests_.size()) << ", exception status =" << int(exception()) << TLOG_ENDL;

	if (!should_stop()) return false;
	if (!useDataThread_ || mode_ == RequestMode::Ignored) return true;
	if (force_stop_) return true;

	if (!request_received_)
	{
		TLOG_ERROR("CommandableFragmentGenerator") << "Stop request received by request-based CommandableFragmentGenerator, but no requests have been received." << std::endl
			<< "Check that UDP port " << request_port_ << " is open in the firewall config." << TLOG_ENDL;
		return true;
	}

	if (!request_stop_requested_) return false;

	auto dur = std::chrono::steady_clock::now() - request_stop_timeout_;
	return  std::chrono::duration_cast<std::chrono::milliseconds>(dur).count() > static_cast<int>(end_of_run_timeout_ms_);// && requests_.size() == 0;
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
	should_stop_.store(false);
	exception_.store(false);
	run_number_ = run;
	subrun_number_ = 1;
	latest_exception_report_ = "none";
	dataBuffer_.clear();
	requests_.clear();
	last_window_send_time_set_ = false;

	start();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (mode_ != RequestMode::Ignored) startRequestReceiverThread();
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG_DEBUG("CommandableFragmentGenerator") << "Stop Command received." << TLOG_ENDL;

	timeout_ = timeout;
	timestamp_ = timestamp;

	stopNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);
	stop();
}

void artdaq::CommandableFragmentGenerator::PauseCmd(uint64_t timeout, uint64_t timestamp)
{
	timeout_ = timeout;
	timestamp_ = timestamp;

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
	requests_.clear();

	// no lock required: thread not started yet
	resume();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (mode_ != RequestMode::Ignored) startRequestReceiverThread();
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

void artdaq::CommandableFragmentGenerator::startDataThread()
{
	if (dataThread_.joinable()) dataThread_.join();
	TLOG_INFO("CommandableFragmentGenerator") << "Starting Data Receiver Thread" << TLOG_ENDL;
	dataThread_ = std::thread(&CommandableFragmentGenerator::getDataLoop, this);
}

void artdaq::CommandableFragmentGenerator::startMonitoringThread()
{
	if (monitoringThread_.joinable()) monitoringThread_.join();
	TLOG_INFO("CommandableFragmentGenerator") << "Starting Hardware Monitoring Thread" << TLOG_ENDL;
	monitoringThread_ = std::thread(&CommandableFragmentGenerator::getMonitoringDataLoop, this);
}

void artdaq::CommandableFragmentGenerator::startRequestReceiverThread()
{
	if (requestThread_.joinable()) requestThread_.join();
	TLOG_INFO("CommandableFragmentGenerator") << "Starting Request Reception Thread" << TLOG_ENDL;
	requestThread_ = std::thread(&CommandableFragmentGenerator::receiveRequestsLoop, this);
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
			TLOG_DEBUG("CommandableFragmentGenerator") << "getDataLoop: isHardwareOK is " << isHardwareOK_ << ", aborting data thread" << TLOG_ENDL;
			data_thread_running_ = false;
			return;
		}

		TLOG_ARB(13, "CommandableFragmentGenerator") << "getDataLoop: calling getNext_" << TLOG_ENDL;

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

		TLOG_ARB(13, "CommandableFragmentGenerator") << "getDataLoop: checking buffer size" << TLOG_ENDL;
		auto startwait = std::chrono::steady_clock::now();

		if (newDataBuffer_.size() == 0 && sleep_on_no_data_us_ > 0)
		{
			usleep(sleep_on_no_data_us_);
		}
		if (metricMan)
		{
			metricMan->sendMetric("Avg Data Acquisition Time", std::chrono::duration_cast<artdaq::TimeUtils::seconds>(startwait - startdata).count(), "s", 3, artdaq::MetricMode::Average);
		}

		auto first = true;
		auto lastwaittime = 0;
		while (dataBufferIsTooLarge())
		{
			if (should_stop())
			{
				TLOG_DEBUG("CommandableFragmentGenerator") << "Run ended while waiting for buffer to shrink!" << TLOG_ENDL;
				std::unique_lock<std::mutex> lock(dataBufferMutex_);
				getDataBufferStats();
				dataCondition_.notify_all();
				data_thread_running_ = false;
				return;
			}
			auto waittime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startwait).count();

			if (first || (waittime != lastwaittime && waittime % 1000 == 0))
			{
				TLOG_WARNING("CommandableFragmentGenerator") << "Bad Omen: Data Buffer has exceeded its size limits. Check the connection between the BoardReader and the EventBuilders! (seq_id=" << ev_counter() << ")" << TLOG_ENDL;
				first = false;
			}
			if (waittime % 5 && waittime != lastwaittime)
			{
				TLOG_ARB(13, "CommandableFragmentGenerator") << "getDataLoop: Data Retreival paused for " << std::to_string(waittime) << " ms waiting for data buffer to drain" << TLOG_ENDL;
			}
			lastwaittime = waittime;
			usleep(1000);
		}

		TLOG_ARB(13, "CommandableFragmentGenerator") << "getDataLoop: processing data" << TLOG_ENDL;
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
			TLOG_INFO("CommandableFragmentGenerator") << "Data flow has stopped. Ending data collection thread" << TLOG_ENDL;
			data_thread_running_ = false;
			return;
		}
	}
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
	TLOG_ARB(15, "CommandableFragmentGenerator") << "getDataBufferStats: Calculating buffer size" << TLOG_ENDL;
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
		TLOG_ARB(15, "CommandableFragmentGenerator") << "getDataBufferStats: Sending Metrics" << TLOG_ENDL;
		metricMan->sendMetric("Buffer Depth Fragments", dataBufferDepthFragments_.load(), "fragments", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Buffer Depth Bytes", dataBufferDepthBytes_.load(), "bytes", 1, MetricMode::LastPoint);
	}
	TLOG_ARB(15, "CommandableFragmentGenerator") << "getDataBufferStats: frags=" << dataBufferDepthFragments_.load() << "/" << maxDataBufferDepthFragments_
		<< ", sz=" << std::to_string(dataBufferDepthBytes_.load()) << "/" << std::to_string(maxDataBufferDepthBytes_) << TLOG_ENDL;
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
				TLOG_ARB(17, "CommandableFragmentGenerator") << "Determining if Fragments can be dropped from data buffer" << TLOG_ENDL;
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
			TLOG_DEBUG("CommandableFragmentGenerator") << "getMonitoringDataLoop: should_stop() is " << std::boolalpha << should_stop()
				<< " and monitoringInterval is " << monitoringInterval_ << ", returning" << TLOG_ENDL;
			return;
		}
		TLOG_ARB(12, "CommandableFragmentGenerator") << "getMonitoringDataLoop: Determining whether to call checkHWStatus_" << TLOG_ENDL;

		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_)
		{
			isHardwareOK_ = checkHWStatus_();
			TLOG_ARB(12, "CommandableFragmentGenerator") << "getMonitoringDataLoop: isHardwareOK_ is now " << std::boolalpha << isHardwareOK_ << TLOG_ENDL;
			lastMonitoringCall_ = now;
		}
		usleep(monitoringInterval_ / 10);
	}
}

void artdaq::CommandableFragmentGenerator::receiveRequestsLoop()
{
	while (!force_stop_)
	{
		if (check_stop() || !isHardwareOK_ || exception())
		{
			TLOG_DEBUG("CommandableFragmentGenerator") << "receiveRequestsLoop: check_stop is " << std::boolalpha << check_stop()
				<< ", isHardwareOK_ is " << isHardwareOK_ << ", and exception state is " << exception() << ", aborting request reception thread." << TLOG_ENDL;
			return;
		}

		// Don't listen for requests when we're going to ignore them anyway
		if (mode_ == RequestMode::Ignored) return;
		TLOG_ARB(16, "CommandableFragmentGenerator") << "receiveRequestsLoop: Polling Request socket for new requests" << TLOG_ENDL;

		int ms_to_wait = 1000;
		struct pollfd ufds[1];
		ufds[0].fd = request_socket_;
		ufds[0].events = POLLIN | POLLPRI;
		int rv = poll(ufds, 1, ms_to_wait);
		if (rv > 0)
		{
			if (ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
			{
				TLOG_ARB(11, "CommandableFragmentGenerator") << "Recieved packet on Request channel" << TLOG_ENDL;
				detail::RequestHeader hdr_buffer;
				recv(request_socket_, &hdr_buffer, sizeof(hdr_buffer), 0);
				TLOG_ARB(11, "CommandableFragmentGenerator") << "Request header word: 0x" << std::hex << hdr_buffer.header << TLOG_ENDL;
				if (hdr_buffer.isValid())
				{
					request_received_ = true;
					if (hdr_buffer.mode == detail::RequestMessageMode::EndOfRun)
					{
						TLOG_INFO("CommandableFragmentGenerator") << "Received Request Message with the EndOfRun marker. (Re)Starting 1-second timeout for receiving all outstanding requests..." << TLOG_ENDL;
						request_stop_timeout_ = std::chrono::steady_clock::now();
						request_stop_requested_ = true;
					}
					std::vector<detail::RequestPacket> pkt_buffer(hdr_buffer.packet_count);
					recv(request_socket_, &pkt_buffer[0], sizeof(detail::RequestPacket) * hdr_buffer.packet_count, 0);
					bool anyNew = false;
					for (auto& buffer : pkt_buffer)
					{
						if (!buffer.isValid()) continue;
						if (requests_.count(buffer.sequence_id) && requests_[buffer.sequence_id] != buffer.timestamp)
						{
							TLOG_ERROR("CommandableFragmentGenerator") << "Received conflicting request for SeqID "
								<< std::to_string(buffer.sequence_id) << "!"
								<< " Old ts=" << std::to_string(requests_[buffer.sequence_id])
								<< ", new ts=" << std::to_string(buffer.timestamp) << ". Keeping OLD!" << TLOG_ENDL;
						}
						else if (!requests_.count(buffer.sequence_id))
						{
							int delta = buffer.sequence_id - ev_counter();
							TLOG_ARB(11, "CommandableFragmentGenerator") << "Recieved request for sequence ID " << std::to_string(buffer.sequence_id)
								<< " and timestamp " << std::to_string(buffer.timestamp) << " (delta: " << delta << ")" << TLOG_ENDL;
							if (delta < 0)
							{
								TLOG_ARB(11, "CommandableFragmentGenerator") << "Already serviced this request! Ignoring..." << TLOG_ENDL;
							}
							else
							{
								std::unique_lock<std::mutex> tlk(request_mutex_);
								requests_[buffer.sequence_id] = buffer.timestamp;
								anyNew = true;
							}
						}
					}
					if (anyNew)
					{
						std::unique_lock<std::mutex> lock(request_mutex_);
						requestCondition_.notify_all();
					}
				}
			}
		}
	}
}

bool artdaq::CommandableFragmentGenerator::applyRequests(artdaq::FragmentPtrs& frags)
{
	if (check_stop() || exception())
	{
		return false;
	}

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
		if ((check_stop() && requests_.size() <= 0) || exception()) return false;
		checkDataBuffer();

		while (requests_.size() <= 0)
		{
			if (check_stop() || exception()) return false;

			checkDataBuffer();

			std::unique_lock<std::mutex> lock(request_mutex_);
			requestCondition_.wait_for(lock, std::chrono::milliseconds(10), [this]() { return requests_.size() > 0; });
		}
	}

	{
		std::unique_lock<std::mutex> dlk(dataBufferMutex_);
		std::unique_lock<std::mutex> rlk(request_mutex_);


		if (mode_ == RequestMode::Ignored)
		{
			// We just copy everything that's here into the output.
			TLOG_ARB(9, "CommandableFragmentGenerator") << "Mode is Ignored; Copying data to output" << TLOG_ENDL;
			std::move(dataBuffer_.begin(), dataBuffer_.end(), std::inserter(frags, frags.end()));
			dataBuffer_.clear();
		}
		else if (mode_ == RequestMode::Single)
		{
			// We only care about the latest request received. Send empties for all others.
			sendEmptyFragments(frags);

			if (dataBuffer_.size() > 0)
			{
				TLOG_ARB(9, "CommandableFragmentGenerator") << "Mode is Single; Sending copy of last event" << TLOG_ENDL;
				for (auto& fragptr : dataBuffer_)
				{
					// Return the latest data point
					auto frag = fragptr.get();
					auto newfrag = std::unique_ptr<artdaq::Fragment>(new Fragment(ev_counter(), frag->fragmentID()));
					newfrag->resize(frag->size() - detail::RawFragmentHeader::num_words());
					memcpy(newfrag->headerAddress(), frag->headerAddress(), frag->sizeBytes());
					newfrag->setTimestamp(requests_[ev_counter()]);
					newfrag->setSequenceID(ev_counter());
					frags.push_back(std::move(newfrag));
				}
			}
			else
			{
				sendEmptyFragment(frags, ev_counter(), "No data for");
			}
			requests_.clear();
			ev_counter_inc(1, true);
		}
		else if (mode_ == RequestMode::Buffer || mode_ == RequestMode::Window)
		{
		        if (!last_window_send_time_set_) {
              		    last_window_send_time_ = std::chrono::steady_clock::now();
			    last_window_send_time_set_ = true;
	                }

			if (mode_ == RequestMode::Buffer || static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - last_window_send_time_).count()) > missing_request_window_timeout_us_)
			{
				if (mode_ == RequestMode::Window)
				{
					TLOG_ERROR("CommandableFragmentGenerator") << "Data-taking has paused for " << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - last_window_send_time_).count() << " us "
						<< "(> " << std::to_string(missing_request_window_timeout_us_) << " us) while waiting for missing data request messages." << " Sending Empty Fragments for missing requests!" << TLOG_ENDL;
				} // else, Buffer mode, where it only makes sense to send for the last request
				sendEmptyFragments(frags);
			}
			for (auto req = requests_.begin(); req != requests_.end();)
			{
				auto ts = req->second;
				if (req->first < ev_counter())
				{
					req = requests_.erase(req);
					continue;
				}
				while (req->first > ev_counter() && request_stop_requested_ && std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - request_stop_timeout_).count() > 1)
				{
					sendEmptyFragment(frags, ev_counter(), "Missing request for");
					ev_counter_inc(1, true);
				}
				if (req->first > ev_counter())
				{
					++req;
					continue; // Will loop through all requests, means we're in Window mode and missing the correct one
				}
				TLOG_ARB(9, "CommandableFragmentGenerator") << "ApplyRequests: Checking that data exists for request window " << std::to_string(req->first) << " (Buffered mode will always succeed)" << TLOG_ENDL;
				Fragment::timestamp_t min = ts > windowOffset_ ? ts - windowOffset_ : 0;
				Fragment::timestamp_t max = min + windowWidth_;
				TLOG_ARB(9, "CommandableFragmentGenerator") << "ApplyRequests: min is " << std::to_string(min) << ", max is " << std::to_string(max)
					<< " and last point in buffer is " << std::to_string((dataBuffer_.size() > 0 ? dataBuffer_.back()->timestamp() : 0)) << " (sz=" << std::to_string(dataBuffer_.size()) << ")" << TLOG_ENDL;
				bool windowClosed = mode_ != RequestMode::Window || (dataBuffer_.size() > 0 && dataBuffer_.back()->timestamp() >= max);
				bool windowTimeout = static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - last_window_send_time_).count()) > window_close_timeout_us_;
				if (windowTimeout)
				{
					TLOG_WARNING("CommandableFragmentGenerator") << "A timeout occurred waiting for data to close the request window (max=" << std::to_string(max) << ", buffer=" << std::to_string(dataBuffer_.back()->timestamp()) << "). Time waiting: "
						<< std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - last_window_send_time_).count() << " us "
						<< "(> " << std::to_string(window_close_timeout_us_) << " us)." << TLOG_ENDL;
				}
				if (windowClosed || !data_thread_running_ || windowTimeout)
				{
					TLOG_DEBUG("CommandableFragmentGenerator") << "Creating ContainerFragment for Buffered or Window-requested Fragments" << TLOG_ENDL;
					frags.emplace_back(new artdaq::Fragment(ev_counter(), fragment_id()));
					frags.back()->setTimestamp(ts);
					ContainerFragmentLoader cfl(*frags.back());

					if (mode_ == RequestMode::Window && !windowClosed) cfl.set_missing_data(true);
					if (mode_ == RequestMode::Window && dataBuffer_.size() > 0 && dataBuffer_.front()->timestamp() > min)
					{
						TLOG_DEBUG("CommandableFragmentGenerator") << "Request Window covers data that is either before data collection began or has fallen off the end of the buffer" << TLOG_ENDL;
						cfl.set_missing_data(true);
					}

					// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
					// Window mode TFGs must do a little bit more work to decide which fragments to send for a given request
					for (auto it = dataBuffer_.begin(); it != dataBuffer_.end();)
					{
						if (mode_ == RequestMode::Window)
						{
							Fragment::timestamp_t fragT = (*it)->timestamp();
							if (fragT < min || fragT > max || (fragT == max && windowWidth_ > 0))
							{
								++it;
								continue;
							}
						}

						TLOG_ARB(9, "CommandableFragmentGenerator") << "ApplyRequests: Adding Fragment with timestamp " << std::to_string((*it)->timestamp()) << " to Container" << TLOG_ENDL;
						cfl.addFragment(*it);

						if (mode_ == RequestMode::Buffer || (mode_ == RequestMode::Window && uniqueWindows_))
						{
							it = dataBuffer_.erase(it);
						}
						else
						{
							++it;
						}
					}
					req = requests_.erase(req);
					ev_counter_inc(1, true);
					last_window_send_time_ = std::chrono::steady_clock::now();
				}
				else
				{
					// Wait for the window to be closed for the current event
					break;
				}
			}
		}
		getDataBufferStats();
	}

	if (frags.size() > 0)
		TLOG_ARB(9, "CommandableFragmentGenerator") << "Finished Processing Event " << std::to_string(ev_counter() + 1) << " for fragment_id " << fragment_id() << "." << TLOG_ENDL;
	return true;
}

bool artdaq::CommandableFragmentGenerator::sendEmptyFragment(artdaq::FragmentPtrs& frags, size_t seqId, std::string desc)
{
	TLOG_WARNING("CommandableFragmentGenerator") << desc << " request " << seqId << ", sending empty fragment" << TLOG_ENDL;
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

void artdaq::CommandableFragmentGenerator::sendEmptyFragments(artdaq::FragmentPtrs& frags)
{
	auto sequence_id = Fragment::InvalidSequenceID;
	auto timestamp = Fragment::InvalidTimestamp;
	// Map is ordered by sequence ID!
	TLOG_ARB(19, "CommandableFragmentGenerator") << "Sending Empty Fragments" << TLOG_ENDL;
	for (auto it = requests_.begin(); it != requests_.end();)
	{
		auto seq = it->first;
		auto ts = it->second;

		while (seq > ev_counter())
		{
			// Otherwise, this is just one we missed, send an empty
			sendEmptyFragment(frags, ev_counter(), "Missed request for");
			ev_counter_inc(1, true);
		}

		// Check if this is the one "true" request
		if (++it == requests_.end())
		{
			sequence_id = seq;
			timestamp = ts;
			break;
		}
		if (seq < ev_counter()) continue;

	}
	requests_.clear();

	if (sequence_id < ev_counter()) return; // No new requests received.
	requests_[sequence_id] = timestamp;
}
