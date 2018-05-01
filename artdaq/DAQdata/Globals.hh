#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#include <sstream>
#include "artdaq-utilities/Plugins/MetricManager.hh"

#define my_rank artdaq::Globals::my_rank_
#define app_name artdaq::Globals::app_name_
#define metricMan artdaq::Globals::metricMan_
#define seedAndRandom() artdaq::Globals::seedAndRandom_()

#define mftrace_iteration artdaq::Globals::mftrace_iteration_
#define mftrace_module artdaq::Globals::mftrace_module_
#define SetMFModuleName(name) mftrace_module = name
#define SetMFIteration(name) mftrace_iteration = name

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
		static std::string app_name_; ///< The name of the current application, to be used in logging and metrics

		// MessageFacility's module and iteration are thread-local, but we want to use them to represent global state in artdaq.
		static std::string mftrace_module_;
		static std::string mftrace_iteration_;

		/**
		 * \brief Seed the C random number generator with the current time (if that has not been done already) and generate a random value
		 * \return A random number.
		 */
		static uint32_t seedAndRandom_()
		{
			static bool initialized_ = false;
			if (!initialized_)
			{
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

		/**
		* \brief Get the current partition number, as defined by the ARTDAQ_PARTITION_NUMBER environment variable
		* \return The current partition number (defaults to 0 if unset, will be between 0 and 127)
		*/
		static int GetPartitionNumber()
		{
			auto part = getenv("ARTDAQ_PARTITION_NUMBER"); // 0-127
			uint32_t part_u = 0;
			if (part != nullptr)
			{
				try
				{
					auto part_s = std::string(part);
					part_u = static_cast<uint32_t>(std::stoll(part_s, 0, 0));
				}
				catch (std::invalid_argument) {}
				catch (std::out_of_range) {}
			}

			return (part_u & 0x7F);
		}
	};
}

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "tracemf.h"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Table.h"
#include "fhiclcpp/types/Sequence.h"
#include "fhiclcpp/types/TableFragment.h"
#if MESSAGEFACILITY_HEX_VERSION >= 0x20103
# include "fhiclcpp/types/ConfigurationTable.h"
#endif
#endif // ARTDAQ_DAQDATA_GLOBALS_HH
