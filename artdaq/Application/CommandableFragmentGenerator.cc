#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "tracelib.h"		// TRACE

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <limits>
#include <iterator>

#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Utilities/SimpleLookupPolicy.h"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/ContainerFragmentLoader.hh"

#include <fstream>
#include <iomanip>
#include <iterator>
#include <iostream>
#include <iomanip>
#include <sys/poll.h>

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator()
	: mutex_()
	, metricMan_(nullptr)
	, listenForRequests_(false)
	, request_port_(3001)
	, request_addr_("227.128.12.26")
	, requests_()
	, windowOffset_(0)
	, windowWidth_(0)
	, staleTimeout_(Fragment::InvalidTimestamp)
	, maxFragmentCount_(std::numeric_limits<size_t>::max())
	, uniqueWindows_(true)
	, useDataThread_(false)
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(1000)
	, maxDataBufferDepthBytes_(1000)
	, useMonitoringThread_(false)
	, collectMonitoringData_(false)
	, monitoringInterval_(1000000)
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
	, sleep_on_stop_us_(0)
{
}

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(const fhicl::ParameterSet &ps)
	: mutex_()
	, metricMan_(nullptr)
	, listenForRequests_(ps.get<bool>("requests_enabled", false))
	, request_port_(ps.get<int>("request_port", 3001))
	, request_addr_(ps.get<std::string>("request_address", "227.128.12.26"))
	, requests_()
	, windowOffset_(ps.get<Fragment::timestamp_t>("request_window_offset", 0))
	, windowWidth_(ps.get<Fragment::timestamp_t>("request_window_width", 0))
	, staleTimeout_(ps.get<Fragment::timestamp_t>("stale_request_timeout", 0xFFFFFFFF))
	, uniqueWindows_(ps.get<bool>("request_windows_are_unique", true))
	, useDataThread_(ps.get<bool>("separate_data_thread", false))
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(ps.get<int>("data_buffer_depth_fragments", 1000))
	, maxDataBufferDepthBytes_(ps.get<size_t>("data_buffer_depth_mb", 1000) * 1024 * 1024)
	, useMonitoringThread_(ps.get<bool>("separate_monitoring_thread", false))
	, collectMonitoringData_(ps.get<bool>("poll_hardware_status", false))
	, monitoringInterval_(ps.get<int64_t>("hardware_poll_interval_us", 1000000))
	, lastMonitoringCall_(std::chrono::steady_clock::now())
	, isHardwareOK_(true)
	, dataBuffer_()
	, newDataBuffer_()
	, run_number_(-1)
	, subrun_number_(-1)
	, timeout_(std::numeric_limits<uint64_t>::max())
	, timestamp_(std::numeric_limits<uint64_t>::max())
	, should_stop_(false), exception_(false)
	, latest_exception_report_("none")
	, ev_counter_(1)
	, board_id_(-1)
	, sleep_on_stop_us_(0)
{
	board_id_ = ps.get<int>("board_id");
	instance_name_for_metrics_ = "BoardReader." + boost::lexical_cast<std::string>(board_id_);

	fragment_ids_ = ps.get< std::vector< artdaq::Fragment::fragment_id_t > >("fragment_ids", std::vector< artdaq::Fragment::fragment_id_t >());

	TRACE(24, "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)");
	int fragment_id = ps.get< int >("fragment_id", -99);

	if (fragment_id != -99) {
		if (fragment_ids_.size() != 0) {
			latest_exception_report_ = "Error in CommandableFragmentGenerator: can't both define \"fragment_id\" and \"fragment_ids\" in FHiCL document";
			throw cet::exception(latest_exception_report_);
		}
		else {
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
	mf::LogDebug("CommandableFragmentGenerator") << "Request mode is " << printMode_();

	if (listenForRequests_) setupRequestListener();
}

void artdaq::CommandableFragmentGenerator::setupRequestListener()
{
	listenForRequests_ = true;
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
	mreq.imr_multiaddr.s_addr = inet_addr(request_addr_.c_str());
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (setsockopt(request_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
		throw art::Exception(art::errors::Configuration) <<
			"CommandableFragmentGenerator: Unable to join multicast group" << std::endl;
		exit(1);
	}
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	mf::LogDebug("CommandableFragmentGenerator") << "Joining dataThread";
	if (dataThread_.joinable()) dataThread_.join();
	mf::LogDebug("CommandableFragmentGenerator") << "Joining monitoringThread";
	if (monitoringThread_.joinable()) monitoringThread_.join();
	mf::LogDebug("CommandableFragmentGenerator") << "Joining requestThread";
	if (requestThread_.joinable()) requestThread_.join();
}

bool artdaq::CommandableFragmentGenerator::getNext(FragmentPtrs & output) {

	bool result = true;

	if (check_stop()) usleep(sleep_on_stop_us_);
	if (exception()) return false;

	if (!useMonitoringThread_ && collectMonitoringData_) {
		TRACE(4, "CFG: Collecting Monitoring Data");
		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_) {
			isHardwareOK_ = checkHWStatus_();
			lastMonitoringCall_ = now;
		}
	}

	try {
		std::lock_guard<std::mutex> lk(mutex_);
		if (useDataThread_)
		{
			TRACE(4, "CFG: Calling applyRequests");
			result = applyRequests(output);
			TRACE(4, "CFG: Done with applyRequests");
		}
		else
		{
			TRACE(4, "CFG: Calling getNext_");
			result = getNext_(output);
			TRACE(4, "CFG: Done with getNext_");
		}
	}
	catch (const cet::exception &e) {
		latest_exception_report_ = "cet::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		mf::LogError("getNext") << "cet::exception caught: " << e;
		set_exception(true);
		return false;
	}
	catch (const boost::exception& e) {
		latest_exception_report_ = "boost::exception caught in getNext(): ";
		latest_exception_report_.append(boost::diagnostic_information(e));
		mf::LogError("getNext") << "boost::exception caught: " << boost::diagnostic_information(e);
		set_exception(true);
		return false;
	}
	catch (const std::exception& e) {
		latest_exception_report_ = "std::exception caught in getNext(): ";
		latest_exception_report_.append(e.what());
		mf::LogError("getNext") << "std::exception caught: " << e.what();
		set_exception(true);
		return false;
	}
	catch (...) {
		latest_exception_report_ = "Unknown exception caught in getNext().";
		mf::LogError("getNext") << "unknown exception caught";
		set_exception(true);
		return false;
	}

	if (!result) {
		TRACE(4, "CFG:getNext Stopped");
		mf::LogDebug("getNext") << "stopped ";
	}

	return result;

}

bool artdaq::CommandableFragmentGenerator::check_stop()
{
	TRACE(4, "CFG::check_stop: should_stop=%i, useDataThread_=%i, requests_.size()=%zu", should_stop(), useDataThread_, requests_.size());
	if (!should_stop()) return false;
	if (!useDataThread_ || mode_ == RequestMode::Ignored) return true;

	return requests_.size() == 0;
}

int artdaq::CommandableFragmentGenerator::fragment_id() const {

	if (fragment_ids_.size() != 1) {
		throw cet::exception("Error in CommandableFragmentGenerator: can't call fragment_id() unless member fragment_ids_ vector is length 1");
	}
	else {
		return fragment_ids_[0];
	}
}

size_t artdaq::CommandableFragmentGenerator::ev_counter_inc(size_t step, bool force) {
	if (force || mode_ == RequestMode::Ignored)
	{
		return ev_counter_.fetch_add(step);
	}
	return ev_counter_.load();
} // returns the prev value

void artdaq::CommandableFragmentGenerator::StartCmd(int run, uint64_t timeout, uint64_t timestamp) {

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
	if (listenForRequests_) startRequestReceiverThread();
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp) {
	mf::LogDebug("CommandableFragmentGenerator") << "Stop Command received.";
	TRACE(4, "CFG: Stop Command Received");

	timeout_ = timeout;
	timestamp_ = timestamp;

	stopNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);

	stop();
}

void artdaq::CommandableFragmentGenerator::PauseCmd(uint64_t timeout, uint64_t timestamp) {

	timeout_ = timeout;
	timestamp_ = timestamp;

	pauseNoMutex();
	should_stop_.store(true);
	std::unique_lock<std::mutex> lk(mutex_);

	pause();
}

void artdaq::CommandableFragmentGenerator::ResumeCmd(uint64_t timeout, uint64_t timestamp) {

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
	if (listenForRequests_) startRequestReceiverThread();
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
	if (which == "latest_exception") {
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
void artdaq::CommandableFragmentGenerator::pauseNoMutex() {
#pragma message "Using default implementation of CommandableFragmentGenerator::pauseNoMutex()"
}

void artdaq::CommandableFragmentGenerator::pause() {
#pragma message "Using default implementation of CommandableFragmentGenerator::pause()"
}

void artdaq::CommandableFragmentGenerator::resume() {
#pragma message "Using default implementation of CommandableFragmentGenerator::resume()"
}

std::string artdaq::CommandableFragmentGenerator::report() {
#pragma message "Using default implementation of CommandableFragmentGenerator::report()"
	return "";
}
std::string artdaq::CommandableFragmentGenerator::reportSpecific(std::string const&) {
#pragma message "Using default implementation of CommandableFragmentGenerator::reportSpecific(std::string)"
	return "";
}

bool artdaq::CommandableFragmentGenerator::checkHWStatus_() {
#pragma message "Using default implementation of CommandableFragmentGenerator::checkHWStatus_()"
	return true;
}

void artdaq::CommandableFragmentGenerator::startDataThread()
{
	if (dataThread_.joinable()) dataThread_.join();
	mf::LogInfo("CommandableFragmentGenerator") << "Starting Data Receiver Thread" << std::endl;
	dataThread_ = std::thread(&CommandableFragmentGenerator::getDataLoop, this);
}

void artdaq::CommandableFragmentGenerator::startMonitoringThread()
{
	if (monitoringThread_.joinable())  monitoringThread_.join();
	mf::LogInfo("CommandableFragmentGenerator") << "Starting Hardware Monitoring Thread" << std::endl;
	monitoringThread_ = std::thread(&CommandableFragmentGenerator::getMonitoringDataLoop, this);
}

void artdaq::CommandableFragmentGenerator::startRequestReceiverThread()
{
	if (requestThread_.joinable()) requestThread_.join();
	mf::LogInfo("CommandableFragmentGenerator") << "Starting Request Reception Thread" << std::endl;
	requestThread_ = std::thread(&CommandableFragmentGenerator::receiveRequestsLoop, this);
}

std::string artdaq::CommandableFragmentGenerator::printMode_()
{
	switch (mode_) {
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
	while (true) {
		if (should_stop() || !isHardwareOK_) {
			mf::LogDebug("CommandableFragmentGenerator") << "getDataLoop: should_stop is " << std::boolalpha << should_stop() << ", and isHardwareOK is " << isHardwareOK_;
			return;
		}

		TRACE(4, "CommandableFragmentGenerator::getDataLoop: calling getNext_");
		bool data = getNext_(newDataBuffer_);

		auto startwait = std::chrono::steady_clock::now();
		bool first = true;
		auto lastwaittime = 0;
		while (dataBufferIsTooLarge()) {

			if (should_stop()) {
				mf::LogDebug("CommandaleFragmentGenerator") << "Run ended while waiting for buffer to shrink!";
				std::unique_lock<std::mutex> lock(dataBufferMutex_);
				getDataBufferStats();
				dataCondition_.notify_all();
				return;
			}
			auto waittime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startwait).count();

			if (first || (waittime != lastwaittime && waittime % 1000 == 0))
			{
				mf::LogWarning("CommandableFragmentGenerator") << "Bad Omen: Data Buffer has exceeded its size limits. Check the connection between the BoardReader and the EventBuilders!";
				first = false;
			}
			if (waittime % 5 && waittime != lastwaittime) {
				TRACE(4, "CFG::getDataLoop: Data Retreival paused for %lu ms waiting for data buffer to drain", waittime);
			}
			lastwaittime = waittime;
			usleep(1000);
		}

		if (data) {
			std::unique_lock<std::mutex> lock(dataBufferMutex_);
			switch (mode_) {
			case RequestMode::Single:
				// While here, if for some strange reason more than one event's worth of data is returned from getNext_...
				while (newDataBuffer_.size() >= fragment_ids_.size()) {
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
			if (dataBuffer_.size() > 0) {
				dataCondition_.notify_all();
			}
		}
	}
}

bool artdaq::CommandableFragmentGenerator::dataBufferIsTooLarge()
{
	return (maxDataBufferDepthFragments_ > 0 && dataBufferDepthFragments_ >= maxDataBufferDepthFragments_) || (maxDataBufferDepthBytes_ > 0 && dataBufferDepthBytes_ >= maxDataBufferDepthBytes_);
}

/// <summary>
/// Calculate the size of the dataBuffer and report appropriate metrics
/// dataBufferMutex must be owned by the calling thread!
/// </summary>
void artdaq::CommandableFragmentGenerator::getDataBufferStats()
{
	dataBufferDepthFragments_ = dataBuffer_.size();
	size_t acc = 0;
	for (auto i = dataBuffer_.begin(); i != dataBuffer_.end(); ++i) {
		acc += (*i)->sizeBytes();
	}
	dataBufferDepthBytes_ = acc;

	if (metricMan_) {
		metricMan_->sendMetric("Buffer Depth Fragments", dataBufferDepthFragments_.load(), "fragments", 1);
		metricMan_->sendMetric("Buffer Depth Bytes", dataBufferDepthBytes_.load(), "bytes", 1);
	}
	TRACE(4, "CFG::getDataBufferStats: frags=%i/%i, sz=%zd/%zd", dataBufferDepthFragments_.load(), maxDataBufferDepthFragments_, dataBufferDepthBytes_.load(), maxDataBufferDepthBytes_);
}

void artdaq::CommandableFragmentGenerator::checkDataBuffer()
{
	std::unique_lock<std::mutex> lock(dataBufferMutex_);
	dataCondition_.wait_for(lock, std::chrono::milliseconds(10));
	if (dataBufferDepthFragments_ > 0) {
		if ((mode_ == RequestMode::Buffer || mode_ == RequestMode::Window))
		{
			// Eliminate extra fragments
			while (dataBufferIsTooLarge())
			{
				dataBuffer_.erase(dataBuffer_.begin());
				getDataBufferStats();
			}
			if (dataBuffer_.size() > 0) {
				Fragment::timestamp_t last = dataBuffer_.back()->timestamp();
				Fragment::timestamp_t min = last > staleTimeout_ ? last - staleTimeout_ : 0;
				for (auto it = dataBuffer_.begin(); it != dataBuffer_.end();)
				{
					if ((*it)->timestamp() < min) {
						it = dataBuffer_.erase(it);
					}
					else {
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
	while (true) {
		if (should_stop() || !collectMonitoringData_) {
			mf::LogDebug("CommandableFragmentGenerator") << "getMonitoringDataLoop: should_stop() is " << std::boolalpha << should_stop()
				<< " and collectMonitoringData is " << collectMonitoringData_;
			return;
		}
		TRACE(4, "CFG::getMonitoringDataLoop Determining whether to call checkHWStatus_");

		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_) {
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
		if (should_stop() || !isHardwareOK_)
		{
			mf::LogDebug("CommandableFragmentGenerator") << "receiveRequestsLoop: should_stop is " << std::boolalpha << should_stop() << ", and isHardwareOK is " << isHardwareOK_;
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
					std::vector<detail::RequestPacket> pkt_buffer(hdr_buffer.packet_count);
					recv(request_socket_, &pkt_buffer[0], sizeof(detail::RequestPacket) * hdr_buffer.packet_count, 0);
					bool anyNew = false;
					for (auto& buffer : pkt_buffer) {
						if (!buffer.isValid()) continue;
						if (requests_.count(buffer.sequence_id) && requests_[buffer.sequence_id] != buffer.timestamp) {
							mf::LogError("CommandableFragmentGenerator") << "Received conflicting request for SeqID "
								<< std::to_string(buffer.sequence_id) << "!"
								<< " Old ts=" << std::to_string(requests_[buffer.sequence_id])
								<< ", new ts=" << std::to_string(buffer.timestamp) << ". Keeping OLD!";
						}
						else if (!requests_.count(buffer.sequence_id)) {
							int delta = buffer.sequence_id - ev_counter();
							mf::LogDebug("CommandableFragmentGenerator") << "Recieved request for sequence ID " << buffer.sequence_id << " and timestamp " << buffer.timestamp << " (delta: " << delta << ")";
							if (delta < 0) {
								mf::LogDebug("CommandableFragmentGenerator") << "Already serviced this request! Ignoring...";
							}
							else {
								std::unique_lock<std::mutex> tlk(request_mutex_);
								requests_[buffer.sequence_id] = buffer.timestamp;
								anyNew = true;
							}
						}
					}
					if (anyNew) {
						std::unique_lock<std::mutex> lock(request_mutex_);
						requestCondition_.notify_all();
					}
				}
			}
		}
	}
}

bool artdaq::CommandableFragmentGenerator::applyRequests(artdaq::FragmentPtrs & frags) {
	if (check_stop()) {
		return false;
	}

	if (mode_ == RequestMode::Ignored) {
		while (dataBufferDepthFragments_ <= 0) {
			if (check_stop()) return false;
			std::unique_lock<std::mutex> lock(dataBufferMutex_);
			dataCondition_.wait_for(lock, std::chrono::milliseconds(10), [this]() { return dataBufferDepthFragments_ > 0; });
		}
	}
	else {
		while (requests_.size() <= 0) {
			if (check_stop()) return false;

			checkDataBuffer();

			std::unique_lock<std::mutex> lock(request_mutex_);
			requestCondition_.wait_for(lock, std::chrono::milliseconds(10), [this]() { return requests_.size() > 0; });
		}
	}

	{
		std::unique_lock<std::mutex> dlk(dataBufferMutex_);

		if (mode_ == RequestMode::Ignored) {
			// We just copy everything that's here into the output.
			TRACE(4, "CFG: Mode is Ignored; Copying data to output");
			std::move(dataBuffer_.begin(), dataBuffer_.end(), std::inserter(frags, frags.end()));
			dataBuffer_.clear();
		}
		else if (mode_ == RequestMode::Single) {
			// We only care about the latest request received. Send empties for all others.
			sendEmptyFragments(frags);

			if (dataBuffer_.size() > 0) {
				TRACE(4, "CFG: Mode is Single; Sending copy of last event");
				for (auto& fragptr : dataBuffer_) {
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
			else {
				sendEmptyFragment(frags, ev_counter(), "No data for");
			}
			requests_.clear();
			ev_counter_inc(1, true);
		}
		else if (mode_ == RequestMode::Buffer || mode_ == RequestMode::Window) {
			if (mode_ == RequestMode::Buffer) {
				// We only care about the latest request received. Send empties for all others.
				sendEmptyFragments(frags);
			}
			for (auto req = requests_.begin(); req != requests_.end();) {
				auto ts = req->second;
				if (req->first < ev_counter()) { 
					req = requests_.erase(req);
					continue; 
				}
				if (req->first > ev_counter()) { 
					++req; 
					continue; // Will loop through all requests, means we're in Window mode and missing the correct one
				}
				mf::LogDebug("CommandableFragmentGenerator") << "Checking that data exists for request window " << req->first << " (Buffered mode will always succeed)";
				Fragment::timestamp_t min = ts > windowOffset_ ? ts - windowOffset_ : 0;
				Fragment::timestamp_t max = min + windowWidth_;
				mf::LogDebug("CommandableFragmentGenerator") << "min is " << min << " and max is " << max << " and last point in buffer is " << (dataBuffer_.size() > 0 ? dataBuffer_.back()->timestamp() : 0) << " (sz=" << dataBuffer_.size() << ")";
				bool windowClosed = mode_ != RequestMode::Window || (dataBuffer_.size() > 0 && dataBuffer_.back()->timestamp() >= max);
				if (windowClosed || should_stop()) {
					mf::LogDebug("CommandableFragmentGenerator") << "Creating ContainerFragment for Buffered or Window-requested Fragments";
					frags.emplace_back(new artdaq::Fragment(ev_counter(), fragment_id()));
					frags.back()->setTimestamp(ts);
					ContainerFragmentLoader cfl(*frags.back());

					if (mode_ == RequestMode::Window && should_stop() && !windowClosed) cfl.set_missing_data(true);
					if (mode_ == RequestMode::Window && dataBuffer_.front()->timestamp() < min) {
						mf::LogDebug("CommandableFragmentGenerator") << "Request Window covers data that is either before data collection began or has fallen off the end of the buffer";
						cfl.set_missing_data(true);
					}

					// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
					// Window mode TFGs must do a little bit more work to decide which fragments to send for a given request
					for (auto it = dataBuffer_.begin(); it != dataBuffer_.end();) {

						if (mode_ == RequestMode::Window) {
							Fragment::timestamp_t fragT = (*it)->timestamp();
							if (fragT < min || fragT > max) {
								++it;
								continue;
							}
						}

						mf::LogDebug("CommandableFragmentGenerator") << "Adding Fragment with timestamp " << (*it)->timestamp() << " to Container";
						cfl.addFragment(*it);

						if (mode_ == RequestMode::Buffer || (mode_ == RequestMode::Window && uniqueWindows_)) {
							it = dataBuffer_.erase(it);
						}
						else {
							++it;
						}
					}
					req = requests_.erase(req);
					ev_counter_inc(1, true);
				}
				else {
					// Wait for the window to be closed for the current event
					break;
				}
			}
		}
		getDataBufferStats();
	}

	if (frags.size() > 0) TRACE(4, "CFG: Finished Processing Event %lu for fragment_id %i.", ev_counter() + 1, fragment_id());
	return true;
}

bool artdaq::CommandableFragmentGenerator::sendEmptyFragment(artdaq::FragmentPtrs& frags, size_t seqId, std::string desc)
{
	mf::LogWarning("CommandableFragmentGenerator") << desc << " request " << seqId << ", sending empty fragment";
	auto frag = new Fragment();
	frag->setSequenceID(seqId);
	frag->setSystemType(Fragment::EmptyFragmentType);
	frags.emplace_back(FragmentPtr(frag));
	return true;
}

// This function is for Buffered and Single trigger modes, as they can only respond to one data request at a time
// If the request message seqID > ev_counter, simply send empties until they're equal
void artdaq::CommandableFragmentGenerator::sendEmptyFragments(artdaq::FragmentPtrs& frags)
{
	auto sequence_id = Fragment::InvalidSequenceID;
	auto timestamp = Fragment::InvalidTimestamp;
	// Map is ordered by sequence ID!
	for (auto it = requests_.begin(); it != requests_.end();) {
		auto seq = it->first;
		auto ts = it->second;

		// Check if this is the one "true" request
		if (++it == requests_.end()) {
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
