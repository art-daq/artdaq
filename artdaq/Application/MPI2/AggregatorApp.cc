#include "artdaq/Application/MPI2/AggregatorApp.hh"
#include "artdaq/Application/MPI2/AggregatorCore.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq/Application/TaskType.hh"

#include "art/Framework/Art/artapp.h"

#include <iostream>

artdaq::AggregatorApp::AggregatorApp(int mpi_rank, MPI_Comm local_group_comm, std::string name) :
  mpi_rank_(mpi_rank), local_group_comm_(local_group_comm), name_(name) { }

// *******************************************************************
// *** The following methods implement the state machine operations.
// *******************************************************************

bool artdaq::AggregatorApp::do_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
  report_string_ = "";

  //aggregator_ptr_.reset(nullptr);
  if (aggregator_ptr_.get() == 0) {
    aggregator_ptr_.reset(new AggregatorCore(mpi_rank_, local_group_comm_, name_));
  }
  external_request_status_ = aggregator_ptr_->initialize(pset);
  if (!external_request_status_) {
    report_string_ = "Error initializing ";
    report_string_.append(name_ + " ");
    report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
  }

  return external_request_status_;
}

bool artdaq::AggregatorApp::do_start(art::RunID id, uint64_t, uint64_t )
{
  report_string_ = "";
  external_request_status_ = aggregator_ptr_->start(id);
  if (!external_request_status_) {
    report_string_ = "Error starting ";
    report_string_.append(name_ + " ");
    report_string_.append("for run number ");
    report_string_.append(boost::lexical_cast<std::string>(id.run()));
    report_string_.append(".");
  }

  aggregator_future_ =
    std::async(std::launch::async, &AggregatorCore::process_fragments,
               aggregator_ptr_.get());

  return external_request_status_;

}

bool artdaq::AggregatorApp::do_stop(uint64_t, uint64_t)
{
  report_string_ = "";
  external_request_status_ = aggregator_ptr_->stop();
  if (!external_request_status_) {
    report_string_ = "Error stopping ";
    report_string_.append(name_ + ".");
  }

  if (aggregator_future_.valid()) {
    aggregator_future_.get();
  }
  return external_request_status_;
}

bool artdaq::AggregatorApp::do_pause(uint64_t, uint64_t)
{
  report_string_ = "";
  external_request_status_ = aggregator_ptr_->pause();
  if (!external_request_status_) {
    report_string_ = "Error pausing ";
    report_string_.append(name_ + ".");
  }

  if (aggregator_future_.valid()) {
    aggregator_future_.get();
  }
  return external_request_status_;
}

bool artdaq::AggregatorApp::do_resume(uint64_t, uint64_t)
{
  report_string_ = "";
  external_request_status_ = aggregator_ptr_->resume();
  if (!external_request_status_) {
    report_string_ = "Error resuming ";
    report_string_.append(name_ + ".");
  }

  aggregator_future_ =
    std::async(std::launch::async, &AggregatorCore::process_fragments,
               aggregator_ptr_.get());

  return external_request_status_;
}

bool artdaq::AggregatorApp::do_shutdown(uint64_t )
{
  report_string_ = "";
  external_request_status_ = aggregator_ptr_->shutdown();
  if (!external_request_status_) {
    report_string_ = "Error shutting down ";
    report_string_.append(name_ + ".");
  }

  return external_request_status_;
}

bool artdaq::AggregatorApp::do_soft_initialize(fhicl::ParameterSet const& , uint64_t, uint64_t )
{
  return true;
}

bool artdaq::AggregatorApp::do_reinitialize(fhicl::ParameterSet const& , uint64_t, uint64_t )
{
  return true;
}

std::string artdaq::AggregatorApp::report(std::string const& which) const
{
  std::string resultString;

  // if all that is requested is the latest state change result, return it
  if (which == "transition_status") {
    if (report_string_.length() > 0) {return report_string_;}
    else {return "Success";}
  }

  // if there is an outstanding report/message at the Commandable/Application
  // level, prepend that
  if (report_string_.length() > 0) {
    resultString.append("*** Overall status message:\r\n");
    resultString.append(report_string_ + "\r\n");
    resultString.append("*** Requested report response:\r\n");
  }

  // pass the request to the AggregatorCore instance, if it's available
  if (aggregator_ptr_.get() != 0) {
    resultString.append(aggregator_ptr_->report(which));
  }
  else {
    resultString.append("This Aggregator has not yet been initialized and ");
    resultString.append("therefore can not provide reporting.");
  }

  return resultString;
}
