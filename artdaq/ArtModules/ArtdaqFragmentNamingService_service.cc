#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq/ArtModules/ArtdaqFragmentNamingService.h"

#include "TRACE/tracemf.h"
#define TRACE_NAME "ArtdaqFragmentNamingService"

ArtdaqFragmentNamingService::ArtdaqFragmentNamingService(fhicl::ParameterSet const& ps, art::ActivityRegistry& /*unused*/)
    : ArtdaqFragmentNamingServiceInterface(ps)
{
	TLOG(TLVL_DEBUG) << "ArtdaqFragmentNamingService CONSTRUCTOR START";
	TLOG(TLVL_DEBUG) << "ArtdaqFragmentNamingService CONSTRUCTOR END";
}

ArtdaqFragmentNamingService::~ArtdaqFragmentNamingService() = default;

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqFragmentNamingService, ArtdaqFragmentNamingServiceInterface)
