#ifndef ARTDAQ_ARTDAQ_BUILDINFO_GETPACKAGEBUILDINFO_HH_
#define ARTDAQ_ARTDAQ_BUILDINFO_GETPACKAGEBUILDINFO_HH_

#include "artdaq-core/Data/PackageBuildInfo.hh"

#include <string>

/**
 * \brief Namespace used to differentiate the artdaq version of GetPackageBuildInfo
 * from other versions present in the system.
 */
namespace artdaq {
/**
 * \brief Wrapper around the artdaq::GetPackageBuildInfo::getPackageBuildInfo function
 */
struct GetPackageBuildInfo
{
	/**
	 * \brief Gets the version number and build timestmap for artdaq
	 * \return An artdaq::PackageBuildInfo object containing the version number and build timestamp for artdaq
	 */
	static artdaq::PackageBuildInfo getPackageBuildInfo();
};
}  // namespace artdaq

#endif  // ARTDAQ_ARTDAQ_BUILDINFO_GETPACKAGEBUILDINFO_HH_
