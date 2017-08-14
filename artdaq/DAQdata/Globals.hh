#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#include "artdaq/DAQdata/configureMessageFacility.hh"
#include "tracemf.h"
#include <sstream>
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#define my_rank artdaq::Globals::my_rank_
#define metricMan artdaq::Globals::metricMan_
#define seedAndRandom() artdaq::Globals::seedAndRandom_()

// Trace Levels
#define DATA_RECV         5
#define DATA_SEND         6
#define TRANSFER_SEND1    7
#define TRANSFER_SEND2    8
#define TRANSFER_RECEIVE1 9
#define TRANSFER_RECEIVE2 10

/**
 * \brief The artdaq namespace
 */
namespace artdaq
{
	/**
	 * \brief The artdaq::Globals class contains several variables which are useful across the entire artdaq system
	 */
	class Globals
	{
	public:
		static int my_rank_; ///< The rank of the current application
		static MetricManager* metricMan_; ///< A handle to MetricManager

		/**
		 * \brief Convert a timeval value to a double
		 * \param tv Timeval to convert
		 * \return timeval represented as a double
		 */
		static double timevalAsDouble(struct timeval tv);

		/**
		 * \brief Seed the C random number generator with the current time (if that has not been done already) and generate a random value
		 * \return A random number.
		 */
		static uint32_t seedAndRandom_()
		{
			static bool initialized_ = false;
			if (!initialized_) {
				srand(TimeUtils::gettimeofday_us());
				initialized_ = true;
			}
			return rand();
		}
	};
}

#endif // ARTDAQ_DAQDATA_GLOBALS_HH
