#include "artdaq/ArtModules/BuildInfo_module.hh"

#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "artdaq-core/BuildInfo/GetPackageBuildInfo.hh"

#include <string>

namespace artdaq {

  static std::string instanceName = "ArtdaqBuildInfo";
  typedef artdaq::BuildInfo< &instanceName, artdaqcore::GetPackageBuildInfo, artdaq::GetPackageBuildInfo> ArtdaqBuildInfo;

  DEFINE_ART_MODULE(ArtdaqBuildInfo)
}
