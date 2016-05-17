#include "artdaq/Application/TriggeredFragmentGenerator.hh"

#include "art/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Utilities/SimpleLookupPolicy.h"
#include "artdaq-core/Data/ContainerFragmentLoader.hh"

#include <fstream>
#include <iomanip>
#include <iterator>
#include <iostream>
#include <sys/poll.h>

artdaq::TriggeredFragmentGenerator::TriggeredFragmentGenerator(fhicl::ParameterSet const & ps)
  : CommandableFragmentGenerator(ps)
  , triggerport_(ps.get<int>("trigger_port",5001))
  , trigger_addr_(ps.get<std::string>("trigger_address", "227.128.12.26"))
  , triggerBuffer_()
  , windowOffset_(static_cast<Fragment::timestamp_t>(ps.get<uint64_t>("trigger_window_offset",0)))
  , windowWidth_(static_cast<Fragment::timestamp_t>(ps.get<uint64_t>("trigger_window_width",0)))
  , staleTimeout_(static_cast<Fragment::timestamp_t>(ps.get<uint64_t>("stale_trigger_timeout", 0xFFFFFFFFFFFFFFFF)))
  , uniqueWindows_(ps.get<bool>("trigger_windows_are_unique", true))
  , haveData_(false)
  , dataBuffer_()
  , newDataBuffer_()
{
  dataBuffer_.emplace_back(FragmentPtr(new Fragment()));
  (*dataBuffer_.begin())->setSystemType(Fragment::EmptyFragmentType);
  
  std::string modeString = ps.get<std::string>("trigger_mode", "single");
  if(modeString == "single" || modeString == "Single") 
	{ 
	  mf::LogInfo("TriggeredFragmentGenerator") << "Mode is set to SINGLE";
	  mode_ = TriggeredFragmentGeneratorMode::Single; 
	}
  else if(modeString.find("buffer") != std::string::npos || modeString.find("Buffer") != std::string::npos)
    {
      mf::LogInfo("TriggeredFragmentGenerator") << "Mode is set to BUFFER";
      mode_ = TriggeredFragmentGeneratorMode::Buffer; 
    }
  else if(modeString == "window" || modeString == "Window")
    {
      mf::LogInfo("TriggeredFragmentGenerator") << "Mode is set to WINDOW";
      mode_ = TriggeredFragmentGeneratorMode::Window; 
    }
  else if(modeString.find("ignore") != std::string::npos || modeString.find("Ignore") != std::string::npos)
	{
	  mf::LogInfo("TriggeredFragmentGenerator") << "Mode is set to IGNORE";
      mode_ = TriggeredFragmentGeneratorMode::Ignored;
	}
  mf::LogDebug("TriggeredFragmentGenerator") << "Trigger mode is " << printMode_();

  triggersocket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if(!triggersocket_) 
	{
	  throw art::Exception(art::errors::Configuration) << "TriggeredFragmentGenerator: Error creating socket!" << std::endl;
	  exit(1);
	}

  struct sockaddr_in si_me_trigger;

  int yes = 1;
  if(setsockopt(triggersocket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
	  throw art::Exception(art::errors::Configuration) <<
		"TriggeredFragmentGenrator: Unable to enable port reuse on trigger socket" << std::endl;
	  exit(1);
	}
  memset(&si_me_trigger,0,sizeof(si_me_trigger));
  si_me_trigger.sin_family = AF_INET;
  si_me_trigger.sin_port = htons(triggerport_);
  si_me_trigger.sin_addr.s_addr = htonl(INADDR_ANY);
  if(bind(triggersocket_, (struct sockaddr *)&si_me_trigger, sizeof(si_me_trigger)) == -1)
	{
      throw art::Exception(art::errors::Configuration) << 
        "TriggeredFragmentGenerator: Cannot bind trigger socket to port " << triggerport_ << std::endl;
      exit(1);
	}
 
  struct ip_mreq mreq;
  mreq.imr_multiaddr.s_addr=inet_addr(trigger_addr_.c_str());
  mreq.imr_interface.s_addr=htonl(INADDR_ANY);
  if( setsockopt(triggersocket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0 ){
    throw art::Exception(art::errors::Configuration) <<
      "TriggeredFragmentGenerator: Unable to join multicast group" << std::endl;
    exit(1);
  }
}

artdaq::TriggeredFragmentGenerator::~TriggeredFragmentGenerator()
{
  if(dataThread_.joinable()) dataThread_.join();
}

void artdaq::TriggeredFragmentGenerator::getNextFragmentLoop_()
{
  while(true) {
    if (should_stop()) {
      return;
    }

    //std::cout << "TriggeredFragmentGenerator: calling getNextFragment_" << std::endl;
    haveData_ = getNextFragment_(newDataBuffer_);
    dataBufferMutex_.lock();
    switch(mode_) {
    case TriggeredFragmentGeneratorMode::Ignored:
    case TriggeredFragmentGeneratorMode::Single:
    default:
      newDataBuffer_.swap(dataBuffer_);
      break;
    case TriggeredFragmentGeneratorMode::Buffer:
	case TriggeredFragmentGeneratorMode::Window:
      //dataBuffer_.reserve(dataBuffer_.size() + newDataBuffer_.size());
      std::move(newDataBuffer_.begin(), newDataBuffer_.end(), std::inserter(dataBuffer_,dataBuffer_.end()));
      break;
    }
    dataBufferMutex_.unlock();
    newDataBuffer_.clear();
    //std::cout << "TriggeredFragmentGenerator: end of getNextFragment_ call, haveData_ is " << haveData_ << std::endl;
  }
}

bool artdaq::TriggeredFragmentGenerator::getNext_(artdaq::FragmentPtrs & frags) {
  if (should_stop()) {
    return false;
  }
 
  int ms_to_wait = mode_ == TriggeredFragmentGeneratorMode::Ignored ? 100 : 1000;
  while(!(haveData_ && mode_ == TriggeredFragmentGeneratorMode::Ignored) && triggerBuffer_.size() == 0) {
    //std::cout << "Start of wait loop: D:" << haveData_ << ", " << printMode_() << ", T:" << triggerBuffer_.size() << std::endl;
    if(should_stop()) {
      return false;
    }
    struct pollfd ufds[1];
    ufds[0].fd = triggersocket_;
    ufds[0].events = POLLIN | POLLPRI;
    int rv = poll(ufds, 1, ms_to_wait);
    if(rv > 0) 
	  {
		// Event counter is monotonically increasing. If we recieve a trigger for an event,
		// fill in the triggers for all the events leading up to it as well.
		if(ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
		  {
			//std::cout << "Recieved packet on Trigger channel" << std::endl;
			detail::TriggerPacket buffer;
			recv(triggersocket_, &buffer, sizeof(buffer), 0);
			//std::cout << "Trigger header word: 0x" << std::hex << (int)buffer.header << std::dec << std::endl;
			if(buffer.header == 0x54524947 && buffer.sequence_id >= ev_counter() && buffer.sequence_id < ev_counter() + 100)
			  {
				int delta = buffer.sequence_id - ev_counter();
				mf::LogDebug("TriggeredFragmentGenerator") << "Recieved trigger for sequence ID " << buffer.sequence_id << " and timestamp " << buffer.timestamp << " (delta: " << delta << ")";
				for(int i = 0; i < delta; ++i)
				  {
					detail::TriggerMessage trig = detail::TriggerMessage();
					trig.setSequenceID( ev_counter() + i );
					triggerBuffer_.push(trig);
				  }

				triggerBuffer_.push(detail::TriggerMessage(buffer));
                
			  }
		  }
	  }

	if((mode_ == TriggeredFragmentGeneratorMode::Buffer || mode_ == TriggeredFragmentGeneratorMode::Window) && triggerBuffer_.size() == 0)
	  {
		dataBufferMutex_.lock();
		// Eliminate extra fragments
		while(dataBuffer_.size() > maxFragmentCount_)
		  {
			dataBuffer_.erase(dataBuffer_.begin());
		  }
		Fragment::timestamp_t last = dataBuffer_.back()->timestamp();
		Fragment::timestamp_t min = last > staleTimeout_ ? last - staleTimeout_ : 0;
		for(auto it = dataBuffer_.begin(); it != dataBuffer_.end(); ++it)
		  { if((*it)->timestamp() < min) {
			  it = dataBuffer_.erase(it);
			  --it;
			}
		  }
		dataBufferMutex_.unlock();
	  }
  }

  while(triggerBuffer_.size() > 0 && triggerBuffer_.front().sequence_id() < ev_counter()) { triggerBuffer_.pop(); }
    

  detail::TriggerMessage trigger;
  if (triggerBuffer_.size() > 0) {
    if(triggerBuffer_.front().sequence_id() == ev_counter()) {
	  trigger = triggerBuffer_.front();
      mf::LogDebug("TriggeredFragmentGenerator") << "Received trigger, sending data";
      triggerBuffer_.pop();
    }
  }
  else { 
	trigger = detail::TriggerMessage();
	trigger.setSequenceID(ev_counter()); 
  }

  dataBufferMutex_.lock();

  if(mode_ == TriggeredFragmentGeneratorMode::Ignored) {
	// We just copy everything that's here into the output.
    mf::LogInfo("TriggeredFragmentGenerator") << "Copying data to output";
	std::move(dataBuffer_.begin(), dataBuffer_.end(), std::inserter(frags,frags.end()));
  }
  // Check that the current trigger is actually a valid trigger. If not, send an empty fragment. (We missed a trigger)
  else if(trigger.isValid())	{
	if(mode_ == TriggeredFragmentGeneratorMode::Single) {
	  // Return the latest data point
	  auto frag = std::unique_ptr<artdaq::Fragment>();
	  frag.swap(dataBuffer_.back());
	  frag->setSequenceID(ev_counter());
	  frags.push_back(std::move(frag));
	}
	else {
	  frags.emplace_back(new artdaq::Fragment(0, ev_counter()));
	  ContainerFragmentLoader cfl(*frags.back());
	  Fragment::timestamp_t min = trigger.timestamp() > windowOffset_ ? trigger.timestamp() - windowOffset_ : 0;
	  Fragment::timestamp_t max = min + windowWidth_;
	  // Buffer mode TFGs should simply copy out the whole dataBuffer_ into a ContainerFragment
	  // Window mode TFGs must do a little bit more work to decide which fragments to send for a given trigger
	  bool windowClosed = mode_ != TriggeredFragmentGeneratorMode::Window;
	  do {
		// Check for new data. windowClosed is true when the last data point has a timestamp after the trigger
		dataBufferMutex_.unlock();
		dataBufferMutex_.lock();
		windowClosed = dataBuffer_.back()->timestamp() >= max;

		for(auto it = dataBuffer_.begin(); it != dataBuffer_.end(); ++it)	  {

		  if(mode_ == TriggeredFragmentGeneratorMode::Window)		  {
			Fragment::timestamp_t fragT = (*it)->timestamp();
			if(fragT < min || fragT > max)			  {
			  //Check for timeout
			  if(fragT < (min > staleTimeout_ ? min - staleTimeout_ : 0) ) { 
				it = dataBuffer_.erase(it);
				--it;
			  }
			  continue;
			}
		  }

		  cfl.addFragment(*it);

		  if(mode_ == TriggeredFragmentGeneratorMode::Buffer || (mode_ == TriggeredFragmentGeneratorMode::Window && uniqueWindows_))		  {
			it = dataBuffer_.erase(it);
			--it;
		  }
		}
	  } while(!windowClosed);
	}
  }
  else	{
	mf::LogWarning("TriggeredFragmentGenerator") << "Missed trigger " << ev_counter() << ", sending empty fragment";
	auto frag = new Fragment();
	frag->setSequenceID(ev_counter());
	frag->setSystemType(Fragment::EmptyFragmentType);
	frags.emplace_back(FragmentPtr(frag));
  }
  haveData_ = false;
  dataBufferMutex_.unlock();

  // Ignored mode TFGs rely on subclasses to handle the ev_counter for their fragments
  if(mode_ != TriggeredFragmentGeneratorMode::Ignored) ev_counter_inc();

  mf::LogInfo("TriggeredFragmentGenerator") << "Returning true";
  return true;
}

std::string artdaq::TriggeredFragmentGenerator::printMode_()
{
  switch (mode_) {
  case TriggeredFragmentGeneratorMode::Single:
    return "Single";
  case TriggeredFragmentGeneratorMode::Buffer:
	return "Buffer";
  case TriggeredFragmentGeneratorMode::Window:
    return "Window";
  case TriggeredFragmentGeneratorMode::Ignored:
    return "Ignored";
  }

  return "ERROR";
}

void artdaq::TriggeredFragmentGenerator::start()
{
  startThread();
  start_();
}

void artdaq::TriggeredFragmentGenerator::resume()
{
  startThread();
  resume_();
}

void artdaq::TriggeredFragmentGenerator::startThread()
{
  if(dataThread_.joinable())  dataThread_.join();
  mf::LogWarning("TriggeredFragmentGenerator") << "Starting Data Receiver Thread" << std::endl;
  dataThread_ = std::thread(&TriggeredFragmentGenerator::getNextFragmentLoop_,this);
}

void artdaq::TriggeredFragmentGenerator::resume_()
{
#pragma message "Using default implementation of TriggeredFragmentGenerator::resume_()"
}

void artdaq::TriggeredFragmentGenerator::ev_counter_inc_()
{
  if(mode_ == TriggeredFragmentGeneratorMode::Ignored) ev_counter_inc();
}
