#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#include "artdaq/DAQdata/configureMessageFacility.hh"
#include "tracemf.h"
#include <sstream>
#include "artdaq-utilities/Plugins/MetricManager.hh"

#define my_rank artdaq::Globals::my_rank_
#define metricMan artdaq::Globals::metricMan_

// Trace Levels
#define DATA_RECV         5
#define DATA_SEND         6
#define TRANSFER_SEND1    7
#define TRANSFER_SEND2    8
#define TRANSFER_RECEIVE1 9
#define TRANSFER_RECEIVE2 10

namespace artdaq
{
	class Globals
	{
	public:
		static int my_rank_;
		static MetricManager* metricMan_;
	};
}

#endif // ARTDAQ_DAQDATA_GLOBALS_HH
