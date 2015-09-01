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
  , haveData_(false)
  , dataBuffer_()
  , newDataBuffer_()
{
  dataBuffer_.emplace_back(FragmentPtr(new Fragment()));
  (*dataBuffer_.begin())->setSystemType(Fragment::EmptyFragmentType);
  
  std::string modeString = ps.get<std::string>("trigger_mode", "triggered");
  if(modeString == "triggered" || modeString == "Triggered" 
     || modeString == "triggerOnly" || modeString == "TriggerOnly") 
  { 
    mode_ = TriggeredFragmentGeneratorMode::TriggerOnly; 
  }
  else if(modeString == "untriggered" || modeString == "Untriggered" 
	  || modeString == "triggerOrData" || modeString == "TriggerOrData")
    {
      mode_ = TriggeredFragmentGeneratorMode::TriggerOrData; 
    }
  else if(modeString.find("buffered") != std::string::npos || modeString.find("Buffered") != std::string::npos)
    {
      mode_ = TriggeredFragmentGeneratorMode::BufferedTriggered; 
    }

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
    case TriggeredFragmentGeneratorMode::TriggerOnly:
    case TriggeredFragmentGeneratorMode::TriggerOrData:
    default:
      newDataBuffer_.swap(dataBuffer_);
      break;
    case TriggeredFragmentGeneratorMode::BufferedTriggered:
      dataBuffer_.reserve(dataBuffer_.size() + newDataBuffer_.size());
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
 
  // And use it, along with the artdaq::Fragment header information
  // (fragment id, sequence id, and user type) to create a fragment

  // We'll use the static factory function 

  // artdaq::Fragment::FragmentBytes(std::size_t payload_size_in_bytes, sequence_id_t sequence_id,
  //  fragment_id_t fragment_id, type_t type, const T & metadata)

  // which will then return a unique_ptr to an artdaq::Fragment
  // object. The advantage of this approach over using the
  // artdaq::Fragment constructor is that, if we were to want to
  // initialize the artdaq::Fragment with a nonzero-size payload (data
  // after the artdaq::Fragment header and metadata), we could provide
  // the size of the payload in bytes, rather than in units of the
  // artdaq::Fragment's RawDataType (8 bytes, as of 3/26/14). The
  // artdaq::Fragment constructor itself was not altered so as to
  // maintain backward compatibility.

  int ms_to_wait = mode_ == TriggeredFragmentGeneratorMode::TriggerOrData ? 100 : 1000;
  while(!(haveData_ && mode_ == TriggeredFragmentGeneratorMode::TriggerOrData) && triggerBuffer_.size() == 0) {
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
            int delta = buffer.fragment_ID - ev_counter() + 1;
	    mf::LogDebug("TriggeredFragmentGenerator") << "Recieved trigger for fragment_ID " << buffer.fragment_ID << " (delta: " << delta << ")";
            for(int i = 0; i < delta; ++i)
	    {
                TriggerPacket trig;
                trig.header = 0x54524947;
                trig.fragment_ID = ev_counter() + i;
		triggerBuffer_.push(trig);
	    }
	}
      }
    }
  }

  while(triggerBuffer_.size() > 0 && triggerBuffer_.front().fragment_ID < ev_counter()) { triggerBuffer_.pop(); }
    
  if (triggerBuffer_.size() > 0) {
    if(triggerBuffer_.front().fragment_ID == ev_counter()) {
      mf::LogDebug("TriggeredFragmentGenerator") << "Received trigger, sending data";
      triggerBuffer_.pop();
    }
  }

  dataBufferMutex_.lock();
  for(auto it = dataBuffer_.begin(); it != dataBuffer_.end(); ++it)
  {
    (*it)->setSequenceID(ev_counter());
    frags.emplace_back(FragmentPtr(new Fragment(*(*it))));
  }
  haveData_ = false;
  dataBufferMutex_.unlock();

  ev_counter_inc();
  return true;
}

std::string artdaq::TriggeredFragmentGenerator::printMode_()
{
  switch (mode_) {
  case TriggeredFragmentGeneratorMode::TriggerOrData:
    return "TriggerOrData";
  case TriggeredFragmentGeneratorMode::BufferedTriggered:
    return "Buffered";
  case TriggeredFragmentGeneratorMode::TriggerOnly:
    return "Triggered";
  }

  return "Triggered";
}

void artdaq::TriggeredFragmentGenerator::StartCmd(int run, uint64_t timeout, uint64_t timestamp) {

  if (run < 0) throw cet::exception("CommandableFragmentGenerator") << "negative run number";

  //mf::LogDebug("TriggeredFragmentGenerator") << "TFG StartCmd Called" << std::endl;
  
  timeout_ = timeout;
  timestamp_ = timestamp;
  ev_counter_.store (1);
  should_stop_.store (false);
  exception_.store(false);
  run_number_ = run;
  subrun_number_ = 1;
  latest_exception_report_ = "none";

  // Start data-collection thread
  startThread();

  // no lock required: thread not started yet
  start();
}

void artdaq::TriggeredFragmentGenerator::ResumeCmd(uint64_t timeout, uint64_t timestamp) {

  timeout_ = timeout;
  timestamp_ = timestamp;

  subrun_number_ += 1;
  should_stop_ = false; 

  // Start data-collection thread

  // no lock required: thread not started yet
  resume();
}

void artdaq::TriggeredFragmentGenerator::startThread()
{
  //mf::LogDebug("TriggeredFragmentGenerator") << "Starting Data Receiver Thread" << std::endl;
  dataThread_ = std::thread(&TriggeredFragmentGenerator::getNextFragmentLoop_,this);
}
