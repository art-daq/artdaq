#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#include "artdaq-utilities/Plugins/MetricManager.hh"

#define my_rank artdaq::Globals::my_rank_
#define metricMan artdaq::Globals::metricMan_

// Trace Levels
#define TLVL_ERROR        0
#define TLVL_WARNING      1
#define TLVL_INFO         2
#define TLVL_DEBUG        3
#define TLVL_VERBOSE      4
#define TLVL_TRACE        5
#define DATA_RECV         6
#define DATA_SEND         7
#define TRANSFER_SEND1    8
#define TRANSFER_SEND2    9
#define TRANSFER_RECEIVE1 10
#define TRANSFER_RECEIVE2 11

namespace artdaq {
	class Globals {
	public:
		static int my_rank_;
		static MetricManager* metricMan_;
	};
}


#endif // ARTDAQ_DAQDATA_GLOBALS_HH