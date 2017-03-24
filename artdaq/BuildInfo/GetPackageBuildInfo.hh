#ifndef artdaq_BuildInfo_GetPackageBuildInfo_hh
#define artdaq_BuildInfo_GetPackageBuildInfo_hh

#include "artdaq-core/Data/PackageBuildInfo.hh"

#include <string>

namespace artdaq
{
	struct GetPackageBuildInfo
	{
		static artdaq::PackageBuildInfo getPackageBuildInfo();
	};
}

#endif /* artdaq_BuildInfo_GetPackageBuildInfo_hh */
