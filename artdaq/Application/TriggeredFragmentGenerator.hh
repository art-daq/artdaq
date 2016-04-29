#ifndef artdaq_Application_TriggeredFragmentGenerator_hh
#define artdaq_Application_TriggeredFragmentGenerator_hh

// A CommandableFragmentGenerator that accepts trigger messages
// and forwards data as appropriate (based on configuration).
//
// 3 Trigger modes are supported: TriggerOnly, TriggerOrData, and BufferedTriggered
// 1. TriggerOnly: The TriggeredFragmentGenerator sends its 
//    latest data point when it receives a trigger message.
// 2. TriggerOrData: The TFG sends data when it gets it from 
//    the hardware or when it receives a trigger message.
// 3. BufferedTriggered: The TFG sends all data accumulated
//    since the last trigger message when it receives a trigger
//    message. If no data was received, it will act like a 
//    TriggerOnly TFG, and resend its last data point.

// Some C++ conventions used:

// -Append a "_" to every private member function and variable

#include "fhiclcpp/fwd.h"
#include "artdaq-core/Data/Fragments.hh" 
#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "artdaq/DAQrate/detail/TriggerMessage.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <list>
#include <queue>
#include <atomic>
#include <thread>
#include <mutex>

namespace artdaq {    
  enum class TriggeredFragmentGeneratorMode {
	  Single,
	  Buffer,
	  Window,
	  Ignored
  };

  class TriggeredFragmentGenerator : public CommandableFragmentGenerator {
  public:
    TriggeredFragmentGenerator(fhicl::ParameterSet const & ps);
    virtual ~TriggeredFragmentGenerator();

  protected:
    virtual bool getNextFragment_(FragmentPtrs & output) = 0;
    virtual void start_() = 0;
    virtual void resume_();

    void ev_counter_inc_();
  private:
    
    // Hide this function from subclasses
    using CommandableFragmentGenerator::ev_counter_inc;
  
    // These functions are implemented in TriggeredFragmentGenerator to provide
    // core functionality. Subclasses can (and should, for getNextFragment_) implement
    // getNextFragment_, start_, and resume_, instead.
    bool getNext_(FragmentPtrs & output) final;
    void start() final;
    void resume() final;

    void getNextFragmentLoop_();
    void startThread();

    std::string printMode_();

    // FHiCL-configurable variables. Note that the C++ variable names
    // are the FHiCL variable names with a "_" appended

    int triggerport_;
    std::string trigger_addr_;

    //Socket parameters
    struct sockaddr_in si_data_;
    int triggersocket_;
    std::queue< detail::TriggerMessage > triggerBuffer_;

    TriggeredFragmentGeneratorMode mode_;
	Fragment::timestamp_t windowOffset_;
	Fragment::timestamp_t windowWidth_;
	Fragment::timestamp_t staleTimeout_;
	size_t maxFragmentCount_;
	bool uniqueWindows_;

    std::thread dataThread_;
    std::atomic<bool> haveData_;
    FragmentPtrs dataBuffer_;
    FragmentPtrs newDataBuffer_;
    std::mutex dataBufferMutex_;
  };
}

#endif /* artdaq_Application_TriggeredFragmentGenerator_hh */
