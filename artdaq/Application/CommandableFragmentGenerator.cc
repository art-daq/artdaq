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
, end_of_run_timeout_ms_(1000)
	, windowOffset_(0)
	, windowWidth_(0)
	, staleTimeout_(Fragment::InvalidTimestamp)
	, maxFragmentCount_(std::numeric_limits<size_t>::max())
	, uniqueWindows_(true)
	, useDataThread_(false)
	, data_thread_running_(false)
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(1000)
	, maxDataBufferDepthBytes_(1000)
	, useMonitoringThread_(false)
	, monitoringInterval_(0)
	, lastMonitoringCall_(std::chrono::steady_clock::now())
	, isHardwareOK_(true)
	, dataBuffer_()
	, newDataBuffer_()
	, run_number_(-1)
	, subrun_number_(-1)
	, timeout_(std::numeric_limits<uint64_t>::max())
	, timestamp_(std::numeric_limits<uint64_t>::max())
	, should_stop_(false)
	, exception_(false)
	, latest_exception_report_("none")
	, ev_counter_(1)
	, board_id_(-1)
	, instance_name_for_metrics_("FragmentGenerator")
	, sleep_on_stop_us_(0) {}

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(const fhicl::ParameterSet& ps)
	: mutex_()
	, request_port_(ps.get<int>("request_port", 3001))
	, request_addr_(ps.get<std::string>("request_address", "227.128.12.26"))
	, requests_()
	, request_stop_requested_(false)
	, end_of_run_timeout_ms_(ps.get<size_t>("end_of_run_quiet_timeout_ms", 1000))
	, windowOffset_(ps.get<Fragment::timestamp_t>("request_window_offset", 0))
	, windowWidth_(ps.get<Fragment::timestamp_t>("request_window_width", 0))
	, staleTimeout_(ps.get<Fragment::timestamp_t>("stale_request_timeout", 0xFFFFFFFF))
	, uniqueWindows_(ps.get<bool>("request_windows_are_unique", true))
	, useDataThread_(ps.get<bool>("separate_data_thread", false))
	, data_thread_running_(false)
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(ps.get<int>("data_buffer_depth_fragments", 1000))
	, maxDataBufferDepthBytes_(ps.get<size_t>("data_buffer_depth_mb", 1000) * 1024 * 1024)
	, useMonitoringThread_(ps.get<bool>("separate_monitoring_thread", false))
	, monitoringInterval_(ps.get<int64_t>("hardware_poll_interval_us", 0))
	, lastMonitoringCall_(std::chrono::steady_clock::now())
	, isHardwareOK_(true)
	, dataBuffer_()
	, newDataBuffer_()
	, run_number_(-1)
	, subrun_number_(-1)
	, timeout_(std::numeric_limits<uint64_t>::max())
	, timestamp_(std::numeric_limits<uint64_t>::max())
	, should_stop_(false)
	, exception_(false)
	, latest_exception_report_("none")
	, ev_counter_(1)
	, board_id_(-1)
	, sleep_on_stop_us_(0)
{
	board_id_ = ps.get<int>("board_id");
	instance_name_for_metrics_ = "BoardReader." + boost::lexical_cast<std::string>(board_id_);

	fragment_ids_ = ps.get<std::vector<artdaq::Fragment::fragment_id_t>>("fragment_ids", std::vector<artdaq::Fragment::fragment_id_t>());

	TRACE(24, "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)");
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
		TRACE(3, "CommandableFragmentGenerator: RequestMode set to SINGLE");
		mode_ = RequestMode::Single;
	}
	else if (modeString.find("buffer") != std::string::npos || modeString.find("Buffer") != std::string::npos)
	{
		TRACE(3, "CommandableFragmentGenerator: RequestMode set to BUFFER");
		mode_ = RequestMode::Buffer;
	}
	else if (modeString == "window" || modeString == "Window")
	{
		TRACE(3, "CommandableFragmentGenerator: RequestMode set to WINDOW");
		mode_ = RequestMode::Window;
	}
	else if (modeString.find("ignore") != std::string::npos || modeString.find("Ignore") != std::string::npos)
	{
		TRACE(3, "CommandableFragmentGenerator: RequestMode set to IGNORE");
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
		throw art::Exception(art::errors::Configuration) << "CommandableFragmentGenerator: Error creating socket for receiving data requests!" << std::endl;
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(request_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		throw art::Exception(art::errors::Configuration) <<
			"RequestedFragmentGenrator: Unable to enable port reuse on request socket" << std::endl;
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(request_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(request_socket_, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
	{
		throw art::Exception(art::errors::Configuration) <<
			"CommandableFragmentGenerator: Cannot bind request socket to port " << request_port_ << std::endl;
		exit(1);
	}

	struct ip_mreq mreq;
	int sts = ResolveHost(request_addr_.c_str(), mreq.imr_multiaddr);
	if (sts == -1)
	{
		throw art::Exception(art::errors::Configuration) << "Unable to resolve multicast request address" << std::endl;
		exit(1);
	}
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (setsockopt(request_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
	{
		throw art::Exception(art::errors::Configuration) <<
			"CommandableFragmentGenerator: Unable to join multicast group" << std::endl;
		exit(1);
	}
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	TLOG_DEBUG("CommandableFragmentGenerator") << "Joining dataThread" << TLOG_ENDL;
	if (dataThread_.joinable()) dataThread_.join();
	TLOG_DEBUG("CommandableFragmentGenerator") << "Joining monitoringThread" << TLOG_ENDL;
	if (monitoringThread_.joinable()) monitoringThread_.join();
	TLOG_DEBUG("CommandableFragmentGenerator") << "Joining requestThread" << TLOG_ENDL;
	if (requestThread_.joinable()) requestThread_.join();
}

bool artdaq::CommandableFragmentGenerator::getNext(FragmentPtrs& output)
{
	bool result = true;

	if (check_stop()) usleep(sleep_on_stop_us_);
	if (exception()) return false;

	if (!useMonitoringThread_ && monitoringInterval_ > 0)
	{
		TRACE(4, "CFG: Collecting Monitoring Data");
		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_)
		{
			isHardwareOK_ = checkHWStatus_();
			lastMonitoringCall_ = now;
		}
	}

	try
	{
		std::lock_guard<std::mutex> lk(mutex_);
		if (useDataThread_)
		{
			TRACE(4, "CFG: Calling applyRequests");
			result = applyRequests(output);
			TRACE(4, "CFG: Done with applyRequests");

			if (exception()) {
			  throw cet::exception("CommandableFragmentGenerator") << "Exception found in BoardReader with board ID " << board_id() << "; BoardReader will now return error status when queried";
			}
		}
		else
		{
			TRACE(4, "CFG: Calling getNext_ %zu", ev_counter());
			try {
			  result = getNext_(output);
			} catch (...) {
			  throw;
			}
			TRACE(4, "CFG: Done with getNext_ %zu", ev_counter());
		}
	}
	catch (const cet::exception& e)
	{
		latest_exception_report_ = "cet::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG_ERROR("getNext") << "cet::exception caught: " << e << TLOG_ENDL;
		set_exception(true);
		return false;
	}
	catch (const boost::exception& e)
	{
		latest_exception_report_ = "boost::exception caught in getNext(): ";
		latest_exception_report_.append(boost::diagnostic_information(e));
		TLOG_ERROR("getNext") << "boost::exception caught: " << boost::diagnostic_information(e) << TLOG_ENDL;
		set_exception(true);
		return false;
	}
	catch (const std::exception& e)
	{
		latest_exception_report_ = "std::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		TLOG_ERROR("getNext") << "std::exception caught: " << e.what() << TLOG_ENDL;
		set_exception(true);
		return false;
	}
	catch (...)
	{
		latest_exception_report_ = "Unknown exception caught in getNext().";
		TLOG_ERROR("getNext") << "unknown exception caught" << TLOG_ENDL;
		set_exception(true);
		return false;
	}

	if (!result)
	{
		TRACE(4, "CFG:getNext Stopped");
		TLOG_DEBUG("getNext") << "stopped " << TLOG_ENDL;
	}

	return result;
}

bool artdaq::CommandableFragmentGenerator::check_stop()
{
  TRACE(4, "CFG::check_stop: should_stop=%i, useDataThread_=%i, requests_.size()=%zu, exception status =%d", should_stop(), useDataThread_, requests_.size(), int(exception()));
	if (!should_stop()) return false;
	if (!useDataThread_ || mode_ == RequestMode::Ignored) return true;
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

	start();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (mode_ != RequestMode::Ignored) startRequestReceiverThread();
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp)
{
	TLOG_DEBUG("CommandableFragmentGenerator") << "Stop Command received." << TLOG_ENDL;
	TRACE(4, "CFG: Stop Command Received");

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
	while (true)
	{
		if (!isHardwareOK_)
		{
			TLOG_DEBUG("CommandableFragmentGenerator") << "getDataLoop: isHardwareOK is " << isHardwareOK_ << ", aborting data thread" << TLOG_ENDL;
			data_thread_running_ = false;
			return;
		}

		TRACE(4, "CommandableFragmentGenerator::getDataLoop: calling getNext_");

		bool data = false;

		try {
		  data = getNext_(newDataBuffer_);
		} catch (...) {
		  ExceptionHandler(ExceptionHandlerRethrow::no,
				   "Exception thrown by fragment generator in CommandableFragmentGenerator::getDataLoop; setting exception state to \"true\"");
		  set_exception(true);

		  data_thread_running_ = false;
		  return;
		}

		auto startwait = std::chrono::steady_clock::now();
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
				TRACE(4, "CFG::getDataLoop: Data Retreival paused for %lu ms waiting for data buffer to drain", waittime);
			}
			lastwaittime = waittime;
			usleep(1000);
		}

		if (data)
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
		if (!data)
		{
			TLOG_INFO("CommandableFragmentGenerator") << "Data flow has stopped. Ending data collection thread" << TLOG_ENDL;
			data_thread_running_ = false;
			return;
		}
	}
}

bool artdaq::CommandableFragmentGenerator::dataBufferIsTooLarge()
{
	return (maxDataBufferDepthFragments_ > 0 && dataBufferDepthFragments_ >= maxDataBufferDepthFragments_) || (maxDataBufferDepthBytes_ > 0 && dataBufferDepthBytes_ >= maxDataBufferDepthBytes_);
}

void artdaq::CommandableFragmentGenerator::getDataBufferStats()
{
	/// dataBufferMutex must be owned by the calling thread!
	dataBufferDepthFragments_ = dataBuffer_.size();
	size_t acc = 0;
	for (auto i = dataBuffer_.begin(); i != dataBuffer_.end(); ++i)
	{
		acc += (*i)->sizeBytes();
	}
	dataBufferDepthBytes_ = acc;

	if (metricMan)
	{
		metricMan->sendMetric("Buffer Depth Fragments", dataBufferDepthFragments_.load(), "fragments", 1);
		metricMan->sendMetric("Buffer Depth Bytes", dataBufferDepthBytes_.load(), "bytes", 1);
	}
	TRACE(4, "CFG::getDataBufferStats: frags=%i/%i, sz=%zd/%zd", dataBufferDepthFragments_.load(), maxDataBufferDepthFragments_, dataBufferDepthBytes_.load(), maxDataBufferDepthBytes_);
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
	while (true)
	{
		if (should_stop() || monitoringInterval_ <= 0)
		{
			TLOG_DEBUG("CommandableFragmentGenerator") << "getMonitoringDataLoop: should_stop() is " << std::boolalpha << should_stop()
				<< " and monitoringInterval is " << monitoringInterval_ << ", returning" << TLOG_ENDL;
			return;
		}
		TRACE(4, "CFG::getMonitoringDataLoop Determining whether to call checkHWStatus_");

		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_)
		{
			isHardwareOK_ = checkHWStatus_();
			lastMonitoringCall_ = now;
		}
		usleep(monitoringInterval_ / 10);
	}
}

void artdaq::CommandableFragmentGenerator::receiveRequestsLoop()
{
	while (true)
	{
	  if (check_stop() || !isHardwareOK_ || exception())
		{
			TLOG_DEBUG("CommandableFragmentGenerator") << "receiveRequestsLoop: check_stop is " << std::boolalpha << check_stop()
								   << ", isHardwareOK_ is " << isHardwareOK_ << ", and exception state is " << exception() << ", aborting request reception thread." << TLOG_ENDL;
			return;
		}

		// Don't listen for requests when we're going to ignore them anyway
		if (mode_ == RequestMode::Ignored) return;
		TRACE(4, "CFG::receiveRequestsLoop: Polling Request socket for new requests");

		int ms_to_wait = 1000;
		struct pollfd ufds[1];
		ufds[0].fd = request_socket_;
		ufds[0].events = POLLIN | POLLPRI;
		int rv = poll(ufds, 1, ms_to_wait);
		if (rv > 0)
		{
			if (ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
			{
				TRACE(4, "CFG: Recieved packet on Request channel");
				detail::RequestHeader hdr_buffer;
				recv(request_socket_, &hdr_buffer, sizeof(hdr_buffer), 0);
				TRACE(4, "CFG: Request header word: 0x%x", (int)hdr_buffer.header);
				if (hdr_buffer.isValid())
				{
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
							TRACE(4, "CFG: Recieved request for sequence ID %llu and timestamp %lu (delta: %d)", (unsigned long long)buffer.sequence_id, (unsigned long)buffer.timestamp, delta);
							if (delta < 0)
							{
								TRACE(4, "CFG: Already serviced this request! Ignoring...");
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
		  if (check_stop() || exception()) return false;
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
			TRACE(4, "CFG: Mode is Ignored; Copying data to output");
			std::move(dataBuffer_.begin(), dataBuffer_.end(), std::inserter(frags, frags.end()));
			dataBuffer_.clear();
		}
		else if (mode_ == RequestMode::Single)
		{
			// We only care about the latest request received. Send empties for all others.
			sendEmptyFragments(frags);

			if (dataBuffer_.size() > 0)
			{
				TRACE(4, "CFG: Mode is Single; Sending copy of last event");
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
			if (mode_ == RequestMode::Buffer)
			{
				// We only care about the latest request received. Send empties for all others.
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
					sendEmptyFragment(frags,ev_counter(), "Missing request for");
					ev_counter_inc(1, true);
				}
				if (req->first > ev_counter())
				{
					++req;
					continue; // Will loop through all requests, means we're in Window mode and missing the correct one
				}
				TRACE(5, "CFG: ApplyRequestS: Checking that data exists for request window %llu (Buffered mode will always succeed)", (unsigned long long)req->first);
				Fragment::timestamp_t min = ts > windowOffset_ ? ts - windowOffset_ : 0;
				Fragment::timestamp_t max = min + windowWidth_;
				TRACE(5, "CFG::ApplyRequests: min is %lu and max is %lu and last point in buffer is %lu (sz=%zu)",
					(unsigned long)min, (unsigned long)max, (unsigned long)(dataBuffer_.size() > 0 ? dataBuffer_.back()->timestamp() : 0), dataBuffer_.size());
				bool windowClosed = mode_ != RequestMode::Window || (dataBuffer_.size() > 0 && dataBuffer_.back()->timestamp() >= max);
				if (windowClosed || !data_thread_running_)
				{
					TLOG_DEBUG("CommandableFragmentGenerator") << "Creating ContainerFragment for Buffered or Window-requested Fragments" << TLOG_ENDL;
					frags.emplace_back(new artdaq::Fragment(ev_counter(), fragment_id()));
					frags.back()->setTimestamp(ts);
					ContainerFragmentLoader cfl(*frags.back());

					if (mode_ == RequestMode::Window && !data_thread_running_ && !windowClosed) cfl.set_missing_data(true);
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
							if (fragT < min || fragT > max)
							{
								++it;
								continue;
							}
						}

						TRACE(5, "CFG::ApplyRequests: Adding Fragment with timestamp %llu to Container", (unsigned long long)(*it)->timestamp());
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
		TRACE(4, "CFG: Finished Processing Event %lu for fragment_id %i.", ev_counter() + 1, fragment_id());
	return true;
}

bool artdaq::CommandableFragmentGenerator::sendEmptyFragment(artdaq::FragmentPtrs& frags, size_t seqId, std::string desc)
{
	TLOG_WARNING("CommandableFragmentGenerator") << desc << " request " << seqId << ", sending empty fragment" << TLOG_ENDL;
	auto frag = new Fragment();
	frag->setSequenceID(seqId);
	frag->setSystemType(Fragment::EmptyFragmentType);
	frags.emplace_back(FragmentPtr(frag));
	return true;
}

void artdaq::CommandableFragmentGenerator::sendEmptyFragments(artdaq::FragmentPtrs& frags)
{
	auto sequence_id = Fragment::InvalidSequenceID;
	auto timestamp = Fragment::InvalidTimestamp;
	// Map is ordered by sequence ID!
	for (auto it = requests_.begin(); it != requests_.end();)
	{
		auto seq = it->first;
		auto ts = it->second;

		// Check if this is the one "true" request
		if (++it == requests_.end())
		{
			sequence_id = seq;
			timestamp = ts;
			break;
		}
		if (seq < ev_counter()) continue;

		// Otherwise, this is just one we missed, send an empty
		sendEmptyFragment(frags, ev_counter(), "Missed request for");
		ev_counter_inc(1, true);
	}
	requests_.clear();

	if (sequence_id < ev_counter()) return; // No new requests received.
	requests_[sequence_id] = timestamp;
}
