#ifndef ARTDAQ_DAQDATA_GLOBALS_HH
#define ARTDAQ_DAQDATA_GLOBALS_HH

#ifndef TRACE_DEFINE  // as set in Globals.cc
#define TRACE_DECLARE
#endif
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQdata/PortManager.hh"

#define my_rank artdaq::Globals::my_rank_
#define app_name artdaq::Globals::app_name_
#define metricMan artdaq::Globals::metricMan_
#define portMan artdaq::Globals::portMan_
#define seedAndRandom() artdaq::Globals::seedAndRandom_()
#define GetPartitionNumber() artdaq::Globals::getPartitionNumber_()

#define GetMFIteration() artdaq::Globals::GetMFIteration_()
#define GetMFModuleName() artdaq::Globals::GetMFModuleName_()
#define SetMFModuleName(name) artdaq::Globals::SetMFModuleName_(name)
#define SetMFIteration(name) artdaq::Globals::SetMFIteration_(name)

// https://stackoverflow.com/questions/21594140/c-how-to-ensure-different-random-number-generation-in-c-when-program-is-execut
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

/**
 * \brief The artdaq namespace
 */
namespace artdaq {
/**
 * \brief The artdaq::Globals class contains several variables which are useful across the entire artdaq system
 */
class Globals
{
public:
	static int my_rank_;                               ///< The rank of the current application
	static std::unique_ptr<MetricManager> metricMan_;  ///< A handle to MetricManager
	static std::unique_ptr<PortManager> portMan_;      ///< A handle to PortManager
	static std::string app_name_;                      ///< The name of the current application, to be used in logging and metrics
	static int partition_number_;                      ///< The partition number of the current application

	static std::mutex mftrace_mutex_;       ///< Mutex to protect mftrace_module_ and mftrace_iteration_
	static std::string mftrace_module_;     ///< MessageFacility's module and iteration are thread-local, but we want to use them to represent global state in artdaq.
	static std::string mftrace_iteration_;  ///< MessageFacility's module and iteration are thread-local, but we want to use them to represent global state in artdaq.

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
				int amt = read(fp, reinterpret_cast<char *>(&seed) + pos, sizeof(seed) - pos);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
				if (amt <= 0) abort();
				pos += amt;
			}
			srand(seed);
			close(fp);
			initialized_ = true;
		}
		return rand();  // NOLINT(cert-msc50-cpp)
	}

	/**
	 * \brief Get the current partition number, as defined by the ARTDAQ_PARTITION_NUMBER environment variable
	 * \return The current partition number (defaults to 0 if unset, will be between 0 and 127)
	 */
	static int getPartitionNumber_()
	{
		uint32_t part_u = 0;

		// 23-May-2018, KAB: added the option to return the partition number data member
		// and gave it precedence over the env var since it is typcally based on information
		// that the user specified on the command line.
		if (partition_number_ >= 0)
		{
			part_u = static_cast<uint32_t>(partition_number_);
		}
		else
		{
			auto part = getenv("ARTDAQ_PARTITION_NUMBER");  // Valid values 0-127
			if (part != nullptr)
			{
				try
				{
					auto part_s = std::string(part);
					part_u = static_cast<uint32_t>(std::stoll(part_s, nullptr, 0));
				}
				catch (const std::invalid_argument &)
				{}
				catch (const std::out_of_range &)
				{}
			}
			partition_number_ = part_u & 0x7F;
		}

		return (part_u & 0x7F);
	}

	/**
	 * \brief Get the current iteration for MessageFacility messages
	 * \return The current iteration
	 */
	static std::string GetMFIteration_()
	{
		std::unique_lock<std::mutex> lk(mftrace_mutex_);
		return mftrace_iteration_;
	}

	/**
	 * \brief Get the current module name for MessageFacility messages
	 * \return The current module name
	 */
	static std::string GetMFModuleName_()
	{
		std::unique_lock<std::mutex> lk(mftrace_mutex_);
		return mftrace_module_;
	}

	/**
	 * \brief Set the current iteration for MessageFacility messages
	 * \param name The current iteration
	 */
	static void SetMFIteration_(std::string const &name)
	{
		std::unique_lock<std::mutex> lk(mftrace_mutex_);
		mftrace_iteration_ = name;
	}

	/**
	 * \brief Set the current module name for MessageFacility messages
	 * \param name The current module name
	 */
	static void SetMFModuleName_(std::string const &name)
	{
		std::unique_lock<std::mutex> lk(mftrace_mutex_);
		mftrace_module_ = name;
	}

	/**
	 * \brief Clean up statically-allocated Manager class instances
	 */
	static void CleanUpGlobals()
	{
		metricMan_.reset(nullptr);
		portMan_.reset(nullptr);
	}
};
}  // namespace artdaq

#include "tracemf.h"

#endif  // ARTDAQ_DAQDATA_GLOBALS_HH
