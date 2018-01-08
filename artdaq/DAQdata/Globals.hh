#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "tracemf.h"
#include <sstream>
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#define my_rank artdaq::Globals::my_rank_
#define metricMan artdaq::Globals::metricMan_
#define seedAndRandom() artdaq::Globals::seedAndRandom_()

//https://stackoverflow.com/questions/21594140/c-how-to-ensure-different-random-number-generation-in-c-when-program-is-execut
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>


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
		 * \brief Seed the C random number generator with the current time (if that has not been done already) and generate a random value
		 * \return A random number.
		 */
		static uint32_t seedAndRandom_()
		{
			static bool initialized_ = false;
			if (!initialized_) {
				int fp = open("/dev/random", O_RDONLY);
				if (fp == -1) abort();
				unsigned seed;
				unsigned pos = 0;
				while (pos < sizeof(seed))
				{
					int amt = read(fp, (char *)&seed + pos, sizeof(seed) - pos);
					if (amt <= 0) abort();
					pos += amt;
				}
				srand(seed);
				close(fp);
				initialized_ = true;
			}
			return rand();
		}
	};
}

#endif // ARTDAQ_DAQDATA_GLOBALS_HH
