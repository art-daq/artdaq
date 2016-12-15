#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#include "artdaq-utilities/Plugins/MetricManager.hh"

#define my_rank artdaq::Globals::my_rank_
#define metricMan artdaq::Globals::metricMan_

namespace artdaq {
	class Globals {
	public:
		static int my_rank_;
		static MetricManager* metricMan_;
	};
}


#endif // ARTDAQ_DAQDATA_GLOBALS_HH