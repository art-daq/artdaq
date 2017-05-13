#include "artdaq/DAQdata/Globals.hh"

int artdaq::Globals::my_rank_ = -1;
artdaq::MetricManager* artdaq::Globals::metricMan_ = nullptr;

double artdaq::Globals::timevalAsDouble(struct timeval tv)
{
	return tv.tv_sec + tv.tv_usec / 1000000.0;
}