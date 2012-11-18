#ifndef ds50daq_DAQ_Commandable_hh
#define ds50daq_DAQ_Commandable_hh

#include <string>
#include <vector>

#include "fhiclcpp/ParameterSet.h"
#include "art/Persistency/Provenance/RunID.h"
#include "ds50daq/DAQ/Commandable_sm.h"  // must be included after others

namespace ds50
{
  class Commandable;
}

class ds50::Commandable
{
public:
  Commandable();
  Commandable(Commandable const&) = delete;
  virtual ~Commandable() = default;
  Commandable& operator=(Commandable const&) = delete;

  // these methods define the externally available commands
  bool initialize(fhicl::ParameterSet const&);
  bool start(art::RunID, std::string const& runtype);
  bool stop();
  bool pause();
  bool resume();
  /* Report_ptr */ std::string report(std::string const&) const {return reportString_;}
  std::string status() const;
  bool perfreset(std::string const& which) {
    if (which=="fail") {
      return false;
    }
    else {
      return true;
    }
  }
  bool shutdown() {return true;}
  bool soft_initialize(fhicl::ParameterSet const&) {return true;}
  bool reinitialize(fhicl::ParameterSet const&) {return true;}
  std::vector<std::string> legalCommands() const;

  // these methods provide the operations that are used by the state machine
  virtual bool do_initialize(fhicl::ParameterSet const&);
  virtual bool do_start(art::RunID, std::string const&);
  virtual bool do_stop();
  virtual bool do_pause();
  virtual bool do_resume();
  virtual bool do_reinitialize(fhicl::ParameterSet const&);
  virtual bool do_softInitialize(fhicl::ParameterSet const&);
  virtual void badTransition(const std::string& );

private:
  CommandableContext fsm_;
  bool externalRequestStatus_;
  std::string reportString_;
};

#endif
