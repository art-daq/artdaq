#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "tracelib.h"		// TRACE

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <limits>


#ifdef CANVAS
#include "canvas/Utilities/Exception.h"
#else
#include "art/Utilities/Exception.h"
#endif
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
	, listenForTriggers_(false)
	, triggerport_(3001)
	, trigger_addr_("227.128.12.26")
	, triggerBuffer_()
	, windowOffset_(0)
	, windowWidth_(0)
	, staleTimeout_(Fragment::InvalidTimestamp)
	, maxFragmentCount_(std::numeric_limits<size_t>::max())
	, uniqueWindows_(true)
	, useDataThread_(false)
	, triggerBufferDepthTriggers_(0)
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
	, listenForTriggers_(ps.get<bool>("triggers_enabled", false))
	, triggerport_(ps.get<int>("trigger_port", 3001))
	, trigger_addr_(ps.get<std::string>("trigger_address", "227.128.12.26"))
	, triggerBuffer_()
	, windowOffset_(ps.get<Fragment::timestamp_t>("trigger_window_offset", 0))
	, windowWidth_(ps.get<Fragment::timestamp_t>("trigger_window_width", 0))
	, staleTimeout_(ps.get<Fragment::timestamp_t>("stale_trigger_timeout", 0xFFFFFFFF))
	, uniqueWindows_(ps.get<bool>("trigger_windows_are_unique", true))
	, useDataThread_(ps.get<bool>("separate_data_thread", false))
	, triggerBufferDepthTriggers_(0)
	, dataBufferDepthFragments_(0)
	, dataBufferDepthBytes_(0)
	, maxDataBufferDepthFragments_(ps.get<int>("data_buffer_depth_fragments",1000))
    , maxDataBufferDepthBytes_(ps.get<size_t>("data_buffer_depth_mb",1000) * 1024 * 1024)
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

	std::string modeString = ps.get<std::string>("trigger_mode", "ignored");
	if (modeString == "single" || modeString == "Single")
	{
	    TRACE(3, "CommandableFragmentGenerator: TriggerMode set to SINGLE");
		mode_ = TriggerMode::Single;
	}
	else if (modeString.find("buffer") != std::string::npos || modeString.find("Buffer") != std::string::npos)
	{
	    TRACE(3, "CommandableFragmentGenerator: TriggerMode set to BUFFER");
		mode_ = TriggerMode::Buffer;
	}
	else if (modeString == "window" || modeString == "Window")
	{
	    TRACE(3, "CommandableFragmentGenerator: TriggerMode set to WINDOW");
		mode_ = TriggerMode::Window;
	}
	else if (modeString.find("ignore") != std::string::npos || modeString.find("Ignore") != std::string::npos)
	{
	    TRACE(3, "CommandableFragmentGenerator: TriggerMode set to IGNORE");
		mode_ = TriggerMode::Ignored;
	}
	mf::LogDebug("CommandableFragmentGenerator") << "Trigger mode is " << printMode_();

	if (listenForTriggers_) setupTriggerListener();
}

void artdaq::CommandableFragmentGenerator::setupTriggerListener()
{
	listenForTriggers_ = true;
	triggersocket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (!triggersocket_)
	{
		throw art::Exception(art::errors::Configuration) << "CommandableFragmentGenerator: Error creating socket!" << std::endl;
		exit(1);
	}

	struct sockaddr_in si_me_trigger;

	int yes = 1;
	if (setsockopt(triggersocket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		throw art::Exception(art::errors::Configuration) <<
			"TriggeredFragmentGenrator: Unable to enable port reuse on trigger socket" << std::endl;
		exit(1);
	}
	memset(&si_me_trigger, 0, sizeof(si_me_trigger));
	si_me_trigger.sin_family = AF_INET;
	si_me_trigger.sin_port = htons(triggerport_);
	si_me_trigger.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(triggersocket_, (struct sockaddr *)&si_me_trigger, sizeof(si_me_trigger)) == -1)
	{
		throw art::Exception(art::errors::Configuration) <<
			"CommandableFragmentGenerator: Cannot bind trigger socket to port " << triggerport_ << std::endl;
		exit(1);
	}

	struct ip_mreq mreq;
	mreq.imr_multiaddr.s_addr = inet_addr(trigger_addr_.c_str());
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (setsockopt(triggersocket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
		throw art::Exception(art::errors::Configuration) <<
			"CommandableFragmentGenerator: Unable to join multicast group" << std::endl;
		exit(1);
	}
}

artdaq::CommandableFragmentGenerator::~CommandableFragmentGenerator()
{
	if (dataThread_.joinable()) dataThread_.join();
	if (monitoringThread_.joinable()) monitoringThread_.join();
	if (triggerThread_.joinable()) triggerThread_.join();
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
		  TRACE(4, "CFG: Calling applyTriggers");
		  result = applyTriggers(output);
		}
	  else
		{
		  TRACE(4, "CFG: Calling getNext_");
		  result = getNext_(output);
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
		mf::LogDebug("getNext") << "stopped ";
	}

	return result;

}

bool artdaq::CommandableFragmentGenerator::check_stop()
{
	if (!should_stop()) return false;
	if(!useDataThread_) return true;

	return triggerBufferDepthTriggers_ == 0;
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
	if (force || mode_ == TriggerMode::Ignored)
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
	triggerBuffer_.clear();

	start();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (listenForTriggers_) startTriggerReceiverThread();
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp) {

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
	triggerBuffer_.clear();

	// no lock required: thread not started yet
	resume();

	std::unique_lock<std::mutex> lk(mutex_);
	if (useDataThread_) startDataThread();
	if (useMonitoringThread_) startMonitoringThread();
	if (listenForTriggers_) startTriggerReceiverThread();
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

void artdaq::CommandableFragmentGenerator::startTriggerReceiverThread()
{
	if (triggerThread_.joinable()) triggerThread_.join();
	mf::LogInfo("CommandableFragmentGenerator") << "Starting Trigger Reception Thread" << std::endl;
	triggerThread_ = std::thread(&CommandableFragmentGenerator::receiveTriggersLoop, this);
}

std::string artdaq::CommandableFragmentGenerator::printMode_()
{
	switch (mode_) {
	case TriggerMode::Single:
		return "Single";
	case TriggerMode::Buffer:
		return "Buffer";
	case TriggerMode::Window:
		return "Window";
	case TriggerMode::Ignored:
		return "Ignored";
	}

	return "ERROR";
}

void artdaq::CommandableFragmentGenerator::getDataLoop()
{
	while (true) {
		if (should_stop() || !isHardwareOK_) {
			mf::LogDebug("CommandableFragmentGenerator") << "should_stop is " << std::boolalpha << should_stop() << ", and isHardwareOK is " << isHardwareOK_;
			return;
		}

		TRACE(4,"CommandableFragmentGenerator::getDataLoop: calling getNext_");
		bool data = getNext_(newDataBuffer_);
		
		auto startwait = std::chrono::steady_clock::now();
		bool first = true;
		auto lastwaittime = 0;
		while(dataBufferIsTooLarge()) {

          if(should_stop()) {
              mf::LogDebug("CommandaleFragmentGenerator") << "Run ended while waiting for buffer to shrink!";
          }
		  auto waittime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startwait).count();
		  
		  if(first || (waittime != lastwaittime && waittime % 1000 == 0))
			{
				//mf::LogWarning("CommandableFragmentGenerator") << "Bad Omen: Data Buffer has exceeded its size limits. Check the connection between the BoardReader and the EventBuilders!";
			  first = false;
			}
		  if(waittime % 5 && waittime != lastwaittime) {
			  TRACE(4, "CFG::getDataLoop: Data Retreival paused for %lu ms waiting for data buffer to drain",waittime);
		  }
		  lastwaittime = waittime;
		  usleep(1000);
		}

		if (data) {
		  std::unique_lock<std::mutex> lock(dataBufferMutex_);
			switch (mode_) {
			case TriggerMode::Single:
				newDataBuffer_.swap(dataBuffer_);
				break;
			case TriggerMode::Buffer:
			case TriggerMode::Ignored:
			case TriggerMode::Window:
			default:
				//dataBuffer_.reserve(dataBuffer_.size() + newDataBuffer_.size());
				std::move(newDataBuffer_.begin(), newDataBuffer_.end(), std::inserter(dataBuffer_, dataBuffer_.end()));
				break;
			}
			getDataBufferStats();
			dataCondition_.notify_all();
			newDataBuffer_.clear();
		}
	}
}

bool artdaq::CommandableFragmentGenerator::dataBufferIsTooLarge()
{
  return (maxDataBufferDepthFragments_ > 0 && dataBufferDepthFragments_ >= maxDataBufferDepthFragments_) || (maxDataBufferDepthBytes_ > 0 && dataBufferDepthBytes_ >= maxDataBufferDepthBytes_);
}

void artdaq::CommandableFragmentGenerator::getDataBufferStats()
{
  dataBufferDepthFragments_ = dataBuffer_.size();
  size_t acc =0;
  for(auto i = dataBuffer_.begin(); i != dataBuffer_.end(); ++i) {
	acc += (*i)->sizeBytes();
  }
  dataBufferDepthBytes_ = acc;
  TRACE(4, "CFG::getDataBufferStats: frags=%i/%i, sz=%zd/%zd",dataBufferDepthFragments_.load(), maxDataBufferDepthFragments_,dataBufferDepthBytes_.load(),maxDataBufferDepthBytes_);
}

void artdaq::CommandableFragmentGenerator::checkDataBuffer()
{
  std::unique_lock<std::mutex> lock(dataBufferMutex_);
  dataCondition_.wait_for(lock,std::chrono::milliseconds(10));
  if(dataBufferDepthFragments_ > 0) {
	if ((mode_ == TriggerMode::Buffer || mode_ == TriggerMode::Window))
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
		  for (auto it = dataBuffer_.begin(); it != dataBuffer_.end(); ++it)
			{
			  if ((*it)->timestamp() < min) {
				it = dataBuffer_.erase(it);
				if (it == dataBuffer_.end()) break;
				--it;
			  }
			}
		  getDataBufferStats();
		}
	  }
	else if (mode_ == TriggerMode::Single && dataBuffer_.size() > 1)
	  {
		// Eliminate extra fragments
		while (dataBuffer_.size() > 1)
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
			return;
		}

		auto now = std::chrono::steady_clock::now();
		if (std::chrono::duration_cast<std::chrono::microseconds>(now - lastMonitoringCall_).count() >= monitoringInterval_) {
			isHardwareOK_ = checkHWStatus_();
			lastMonitoringCall_ = now;
		}
		usleep(monitoringInterval_ / 10);
	}
}

void artdaq::CommandableFragmentGenerator::receiveTriggersLoop()
{
	while (true)
	{
		if (should_stop() || !isHardwareOK_)
		{
			mf::LogDebug("CommandableFragmentGenerator") << "should_stop is " << std::boolalpha << should_stop() << ", and isHardwareOK is " << isHardwareOK_;
			return;
		}

		int ms_to_wait = 1000;
		struct pollfd ufds[1];
		ufds[0].fd = triggersocket_;
		ufds[0].events = POLLIN | POLLPRI;
		int rv = poll(ufds, 1, ms_to_wait);
		if (rv > 0)
		{
			if (ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
			{
			  TRACE(4, "CFG: Recieved packet on Trigger channel");
				detail::TriggerPacket buffer;
				recv(triggersocket_, &buffer, sizeof(buffer), 0);
				TRACE(4, "CFG: Trigger header word: 0x%x",(int)buffer.header);
				if (buffer.header == 0x54524947 && buffer.sequence_id >= ev_counter() && buffer.sequence_id < ev_counter() + 100)
				{
					int delta = buffer.sequence_id - ev_counter();
					mf::LogDebug("CommandableFragmentGenerator") << "Recieved trigger for sequence ID " << buffer.sequence_id << " and timestamp " << buffer.timestamp << " (delta: " << delta << ")";
					std::unique_lock<std::mutex> lock(triggerBufferMutex_);
					triggerBuffer_.push_back(detail::TriggerMessage(buffer));
					while (triggerBuffer_.size() > 0 && triggerBuffer_.front().sequence_id() < ev_counter()) { triggerBuffer_.pop_front(); }
					triggerBufferDepthTriggers_ = triggerBuffer_.size();
					triggerCondition_.notify_all();
					//triggerBuffer_.sort([](const detail::TriggerMessage& a, const detail::TriggerMessage& b) { return a.sequence_id() < b.sequence_id(); });
				}
			}
		}
	}
}

bool artdaq::CommandableFragmentGenerator::applyTriggers(artdaq::FragmentPtrs & frags) {
	if (check_stop()) {
		return false;
	}

	if(mode_ == TriggerMode::Ignored) {
	  std::unique_lock<std::mutex> lock(dataBufferMutex_);
	  dataCondition_.wait(lock, [this](){ return dataBufferDepthFragments_ > 0; });
	  std::unique_lock<std::mutex> tlock(triggerBufferMutex_);
	  triggerBuffer_.clear();
	  triggerBufferDepthTriggers_ = 0;
	}
	else {
	  while ( triggerBufferDepthTriggers_ <= 0)
		{
		  if (check_stop()) {
			return false;
		  }

		  checkDataBuffer();
		  
		  std::unique_lock<std::mutex> lock(triggerBufferMutex_);
		  triggerCondition_.wait_for(lock, std::chrono::milliseconds(10), 
									 [this](){ return triggerBufferDepthTriggers_ > 0;});
		}
	}

	triggerBufferMutex_.lock();
	detail::TriggerMessage trigger;
	while (triggerBuffer_.size() > 0 && triggerBuffer_.front().sequence_id() < ev_counter())
	  {
		triggerBuffer_.pop_front(); 
	  }
	triggerBufferDepthTriggers_ = triggerBuffer_.size();
	if (triggerBufferDepthTriggers_ > 0) {
	  if (triggerBuffer_.front().sequence_id() == ev_counter()) {
		trigger = triggerBuffer_.front();
		mf::LogDebug("CommandableFragmentGenerator") << "Received trigger #" << ev_counter() << ", sending data";
		triggerBuffer_.pop_front();
	  }
	  else
		{
		  mf::LogDebug("CommandableFragmentGenerator") << "First trigger in buffer is for sequence ID " << triggerBuffer_.front().sequence_id() << ", but I'm looking for " << ev_counter();
		  return true;
		}
	}
	triggerBufferMutex_.unlock();

	dataBufferMutex_.lock();

	bool fragSent = false;
	if (mode_ == TriggerMode::Ignored) {
		// We just copy everything that's here into the output.
	  TRACE(4, "CFG: Mode is Ignored; Copying data to output");
		std::move(dataBuffer_.begin(), dataBuffer_.end(), std::inserter(frags, frags.end()));
		dataBuffer_.clear();
	}
	// Check that the current trigger is actually a valid trigger. If not, send an empty fragment. (We missed a trigger)
	else if (trigger.isValid()) {
		if (mode_ == TriggerMode::Single) {
			if (dataBuffer_.size() > 0) {
			  TRACE(4, "CFG: Mode is Single; Sending copy of last data point");
				// Return the latest data point
				auto frag = dataBuffer_.front().get();
				auto newfrag = std::unique_ptr<artdaq::Fragment>(
					new Fragment(ev_counter(), frag->fragmentID()));
				newfrag->resize(frag->size() - detail::RawFragmentHeader::num_words());
				memcpy(newfrag->headerAddress(), frag->headerAddress(), frag->sizeBytes());
				newfrag->setTimestamp(trigger.timestamp());
				newfrag->setSequenceID(ev_counter());
				frags.push_back(std::move(newfrag));
			}
			else
			{
				sendEmptyFragment(frags, ev_counter(), "No data for");
			}
			fragSent = true;
		}
		else {
			mf::LogDebug("CommandableFragmentGenerator") << "Checking that data exists for trigger window (Buffered mode will always succeed)";
			Fragment::timestamp_t min = trigger.timestamp() > windowOffset_ ? trigger.timestamp() - windowOffset_ : 0;
			Fragment::timestamp_t max = min + windowWidth_;
			mf::LogDebug("CommandableFragmentGenerator") << "min is " << min << " and max is " << max << " and last point in buffer is " << (dataBuffer_.size() > 0 ? dataBuffer_.back()->timestamp() : 0);
			bool windowClosed = mode_ != TriggerMode::Window || (dataBuffer_.size() > 0 && dataBuffer_.back()->timestamp() >= max);
			if (windowClosed || should_stop()) {
				mf::LogDebug("CommandableFragmentGenerator") << "Creating ContainerFragment for Buffered or Window-triggered Fragments";
				frags.emplace_back(new artdaq::Fragment(ev_counter(), fragment_id()));
				frags.back()->setTimestamp(trigger.timestamp());
				ContainerFragmentLoader cfl(*frags.back());

                if(mode_ == TriggerMode::Window && should_stop() && !windowClosed) cfl.set_missing_data(true);

				// Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
				// Window mode TFGs must do a little bit more work to decide which fragments to send for a given trigger
				for (auto it = dataBuffer_.begin(); it != dataBuffer_.end(); ++it) {

					if (mode_ == TriggerMode::Window) {
						Fragment::timestamp_t fragT = (*it)->timestamp();
						if (fragT < min || fragT > max) {
							continue;
						}
					}

					mf::LogDebug("CommandableFragmentGenerator") << "Adding Fragment with timestamp " << (*it)->timestamp() << " to Container";
					cfl.addFragment(*it);

					if (mode_ == TriggerMode::Buffer || (mode_ == TriggerMode::Window && uniqueWindows_)) {
						it = dataBuffer_.erase(it);
						if (it == dataBuffer_.end()) break;
						--it;
					}
				}
				fragSent = true;
			}
			else
			{
				// Put the trigger back for next time
				triggerBufferMutex_.lock();
				triggerBuffer_.push_front(trigger);
				triggerBufferDepthTriggers_ = triggerBuffer_.size();
				triggerBufferMutex_.unlock();
			}
		}
	}
	else {
		sendEmptyFragment(frags, ev_counter(), "Missing trigger message for");
		fragSent = true;
	}
	getDataBufferStats();
	dataBufferMutex_.unlock();

	// Ignored mode TFGs rely on subclasses to handle the ev_counter for their fragments
	if (mode_ != TriggerMode::Ignored && fragSent) {
	  TRACE(4, "CommandableFragmentGenerator: Incrementing Event Counter");
		ev_counter_inc(1, true);
	}

	if(frags.size() > 0 ) TRACE(4,"CFG: Finished Processing Event %lu for fragment_id %i.",ev_counter() +1, fragment_id());
	return true;
}

bool artdaq::CommandableFragmentGenerator::sendEmptyFragment(artdaq::FragmentPtrs& frags, size_t seqId, std::string desc)
{
	mf::LogWarning("CommandableFragmentGenerator") << desc << " trigger " << seqId << ", sending empty fragment";
	auto frag = new Fragment();
	frag->setSequenceID(seqId);
	frag->setSystemType(Fragment::EmptyFragmentType);
	frags.emplace_back(FragmentPtr(frag));
	return true;
}
