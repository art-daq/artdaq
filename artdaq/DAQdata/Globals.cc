#include "artdaq/DAQdata/Globals.hh"

int artdaq::Globals::my_rank_ = -1;
artdaq::MetricManager* artdaq::Globals::metricMan_ = nullptr;
std::string artdaq::Globals::app_name_ = "";

std::string artdaq::Globals::mftrace_iteration_ = "Booted";
std::string artdaq::Globals::mftrace_module_ = "DAQ";