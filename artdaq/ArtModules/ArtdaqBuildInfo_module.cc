#include "artdaq/ArtModules/BuildInfo_module.hh"

#include "artdaq-core/BuildInfo/GetPackageBuildInfo.hh"
#include "artdaq-utilities/BuildInfo/GetPackageBuildInfo.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"

#include <string>

namespace artdaq {
static std::string instanceName = "ArtdaqBuildInfo";  ///< Name of this BuildInfo instance

/**
 * \brief Specialized artdaq::BuildInfo object for the Artdaq build info
 */
typedef artdaq::BuildInfo<&instanceName, artdaqcore::GetPackageBuildInfo, artdaqutilities::GetPackageBuildInfo, artdaq::GetPackageBuildInfo> ArtdaqBuildInfo;

DEFINE_ART_MODULE(ArtdaqBuildInfo)  // NOLINT(performance-unnecessary-value-param)
}  // namespace artdaq
