#include "artdaq/Application/TriggeredFragmentGenerator.hh"

#include "art/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Utilities/SimpleLookupPolicy.h"

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
	  mode_ = TriggeredFragmentGeneratorMode::Single; 
	}
  else if(modeString.find("buffer") != std::string::npos || modeString.find("Buffer") != std::string::npos)
    {
      mode_ = TriggeredFragmentGeneratorMode::Buffer; 
    }
  else if(modeString == "window" || modeString == "Window")
    {
      mode_ = TriggeredFragmentGeneratorMode::Window; 
    }
  else if(modeString == "ignored" || modeString == "Ignored")
	{
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
  dataThread_.join();
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
			TriggerPacket buffer;
			recv(triggersocket_, &buffer, sizeof(buffer), 0);
			//std::cout << "Trigger header word: 0x" << std::hex << (int)buffer.header << std::dec << std::endl;
			if(buffer.header == 0x54524947 && buffer.fragment_ID >= ev_counter() && buffer.fragment_ID < ev_counter() + 100)
			  {
				int delta = buffer.fragment_ID - ev_counter();
				mf::LogDebug("TriggeredFragmentGenerator") << "Recieved trigger for fragment_ID " << buffer.fragment_ID << " (delta: " << delta << ")";
				for(int i = 0; i < delta; ++i)
				  {
					TriggerPacket trig;
					trig.header = 0;
					trig.fragment_ID = ev_counter() + i;
					trig.timestamp = 0;
					triggerBuffer_.push(trig);
				  }

					triggerBuffer_.push(buffer);
                
			  }
		  }
	  }

	if((mode_ == TriggeredFragmentGeneratorMode::Buffer || mode_ == TriggeredFragmentGeneratorMode::Window)	   && triggerBuffer_.size() == 0)
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

  while(triggerBuffer_.size() > 0 && triggerBuffer_.front().fragment_ID < ev_counter()) { triggerBuffer_.pop(); }
    

  TriggerPacket trigger;
  if (triggerBuffer_.size() > 0) {
    if(triggerBuffer_.front().fragment_ID == ev_counter()) {
	  trigger = triggerBuffer_.front();
      mf::LogDebug("TriggeredFragmentGenerator") << "Received trigger, sending data";
      triggerBuffer_.pop();
    }
  }
  else { trigger.header = 0; trigger.fragment_ID = ev_counter(); }

  dataBufferMutex_.lock();
  // Check that the current trigger is actually a valid trigger. If not, send an empty fragment. (We missed a trigger)
  if(mode_ != TriggeredFragmentGeneratorMode::Ignored && trigger.header != 0)
	{	
	  Fragment::timestamp_t min = trigger.timestamp > windowOffset_ ? trigger.timestamp - windowOffset_ : 0;
	  Fragment::timestamp_t max = min + windowWidth_;
	  // For Single, Ignored, and Buffer modes, the dataBuffer is equal to the desired data.
	  // Ignored mode TFGs rely on subclasses to handle the ev_counter for their fragments
	  // Window mode TFGs must do a little bit more work to decide which fragments to send for a given trigger
	  for(auto it = dataBuffer_.begin(); it != dataBuffer_.end(); ++it)
		{
		  // If not in Ignored mode, set the sequence ID of the fragment to the current trigger's sequence ID.
		  if(mode_ != TriggeredFragmentGeneratorMode::Ignored) (*it)->setSequenceID(ev_counter());

		  if(mode_ == TriggeredFragmentGeneratorMode::Window)
			{
			  Fragment::timestamp_t fragT = (*it)->timestamp();
			  if(fragT < min || fragT > max)
				{
				  //Check for timeout
				  if(fragT < (min > staleTimeout_ ? min - staleTimeout_ : 0) ) { 
					it = dataBuffer_.erase(it);
					--it;
                  }
				  continue;
				}
			}

		  frags.emplace_back(FragmentPtr(new Fragment(*(*it))));

		  if(mode_ == TriggeredFragmentGeneratorMode::Buffer || (mode_ == TriggeredFragmentGeneratorMode::Window && uniqueWindows_))
			{
			  it = dataBuffer_.erase(it);
			  --it;
			}
		}
	}
  else
	{
	  mf::LogWarning("TriggeredFragmentGenerator") << "Missed trigger " << ev_counter() << ", sending empty fragment";
	  auto frag = new Fragment();
	  frag->setSequenceID(ev_counter());
	  frag->setSystemType(Fragment::EmptyFragmentType);
	  frags.emplace_back(FragmentPtr(frag));
	}

  if(frags.size() == 0) {
	  mf::LogWarning("TriggeredFragmentGenerator") << "No data available for trigger " << ev_counter() << ", sending empty fragment";
	  auto frag = new Fragment();
	  frag->setSequenceID(ev_counter());
	  frag->setSystemType(Fragment::EmptyFragmentType);
	  frags.emplace_back(FragmentPtr(frag));
  }
  haveData_ = false;
  dataBufferMutex_.unlock();

  if(mode_ != TriggeredFragmentGeneratorMode::Ignored) ev_counter_inc();
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
  //mf::LogDebug("TriggeredFragmentGenerator") << "Starting Data Receiver Thread" << std::endl;
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
