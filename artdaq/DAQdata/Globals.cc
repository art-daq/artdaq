#include "artdaq/DAQdata/Globals.hh"

int artdaq::Globals::my_rank_ = -1;
artdaq::MetricManager* artdaq::Globals::metricMan_ = nullptr;
artdaq::PortManager* artdaq::Globals::portMan_ = nullptr;
std::string artdaq::Globals::app_name_ = "";
int artdaq::Globals::partition_number_ = -1;

std::string artdaq::Globals::mftrace_iteration_ = "Booted";
std::string artdaq::Globals::mftrace_module_ = "DAQ";
