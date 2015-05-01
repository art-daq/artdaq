#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "tracelib.h"		// TRACE

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <limits>

artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator() :
  mutex_(),
  metricMan_(nullptr),
  run_number_(-1), subrun_number_(-1),
  timeout_( std::numeric_limits<uint64_t>::max() ), 
  timestamp_( std::numeric_limits<uint64_t>::max() ), 
  should_stop_(false), exception_(false),
  ev_counter_(1),
  board_id_(-1),
  instance_name_for_metrics_("FragmentGenerator"),
  sleep_on_stop_us_(0)
{
}


artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(const fhicl::ParameterSet &ps) :
  mutex_(),
  metricMan_(nullptr),
  run_number_(-1), subrun_number_(-1),
  timeout_( std::numeric_limits<uint64_t>::max() ), 
  timestamp_( std::numeric_limits<uint64_t>::max() ), 
  should_stop_(false), exception_(false),
  ev_counter_(1),
  board_id_(-1), 
  sleep_on_stop_us_(0)
{
  board_id_ = ps.get<int> ("board_id");
  instance_name_for_metrics_ = "Board " + boost::lexical_cast<std::string>(board_id_);

  fragment_ids_ = ps.get< std::vector< artdaq::Fragment::fragment_id_t > >( "fragment_ids", std::vector< artdaq::Fragment::fragment_id_t >() );

  TRACE( 24, "artdaq::CommandableFragmentGenerator::CommandableFragmentGenerator(ps)" );
  int fragment_id = ps.get< int > ("fragment_id", -99);

  if (fragment_id != -99) {
    if (fragment_ids_.size() != 0) {
      throw cet::exception("Error in CommandableFragmentGenerator: can't both define \"fragment_id\" and \"fragment_ids\" in FHiCL document");
    } else {
      fragment_ids_.emplace_back( fragment_id );
    }
  }

  sleep_on_stop_us_ = ps.get<int> ("sleep_on_stop_us", 0);
}

bool artdaq::CommandableFragmentGenerator::getNext(FragmentPtrs & output) {

  bool result = true;  
  
  if (should_stop() ) usleep( sleep_on_stop_us_);
  if (exception() ) return false;
  
  try { 
    std::lock_guard<std::mutex> lk(mutex_);
    result = getNext_( output );
  } catch (const cet::exception &e) {
    mf::LogError ("getNext") << "cet::exception caught: " << e;
    set_exception (true);
    return false;
  } catch (const boost::exception& e) {
    mf::LogError ("getNext") << "boost::exception caught: " << boost::diagnostic_information(e);
    set_exception (true);
    return false;
  } catch (const std::exception& e  ) {
    mf::LogError ("getNext") << "std::exception caught: " << e.what();
    set_exception (true);
    return false;
  } catch (...) {
    mf::LogError ("getNext") << "unknown exception caught";
    set_exception (true);
    return false;
  }

  if ( ! result ) {
    mf::LogDebug("getNext") << "stopped ";
  }

  return result;

}

int artdaq::CommandableFragmentGenerator::fragment_id () const {

  if (fragment_ids_.size() != 1 ) {
    throw cet::exception("Error in CommandableFragmentGenerator: can't call fragment_id() unless member fragment_ids_ vector is length 1");
  } else {
    return fragment_ids_[0] ;
  }
}

void artdaq::CommandableFragmentGenerator::StartCmd(int run, uint64_t timeout, uint64_t timestamp) {

  if (run < 0) throw cet::exception("CommandableFragmentGenerator") << "negative run number";

  timeout_ = timeout;
  timestamp_ = timestamp;
  ev_counter_.store (1);
  should_stop_.store (false);
  exception_.store(false);
  run_number_ = run;
  subrun_number_ = 1;

  // no lock required: thread not started yet
  start();
}

void artdaq::CommandableFragmentGenerator::StopCmd(uint64_t timeout, uint64_t timestamp) {

  timeout_ = timeout;
  timestamp_ = timestamp;

  stopNoMutex();
  should_stop_.store (true);
  std::unique_lock<std::mutex> lk(mutex_);

  stop();
}

void artdaq::CommandableFragmentGenerator::PauseCmd(uint64_t timeout, uint64_t timestamp) {

  timeout_ = timeout;
  timestamp_ = timestamp;

  pauseNoMutex();
  should_stop_.store (true);
  std::unique_lock<std::mutex> lk(mutex_);

  pause();
}

void artdaq::CommandableFragmentGenerator::ResumeCmd(uint64_t timeout, uint64_t timestamp) {

  timeout_ = timeout;
  timestamp_ = timestamp;

  subrun_number_ += 1;
  should_stop_ = false; 

  // no lock required: thread not started yet
  resume();
}

std::string artdaq::CommandableFragmentGenerator::ReportCmd() 
{
  if (exception()) return "exception";
  std::lock_guard<std::mutex> lk(mutex_);

  return report();
}

