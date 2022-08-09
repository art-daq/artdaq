#define TRACE_NAME "ArtdaqGlobalsService"

#include <cstdint>
#include <memory>

#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/ArtModules/ArtdaqGlobalsService.h"

#include "artdaq/DAQdata/Globals.hh"

#define build_key(seed) ((seed) + ((GetPartitionNumber() + 1) << 16) + (getppid() & 0xFFFF))

static fhicl::ParameterSet empty_pset;

ArtdaqGlobalsService::ArtdaqGlobalsService(fhicl::ParameterSet const& pset, art::ActivityRegistry& /*unused*/)
{
	TLOG(TLVL_DEBUG + 33) << "ArtdaqGlobalsService CONSTRUCTOR";

	char const* artapp_env = getenv("ARTDAQ_APPLICATION_NAME");
	std::string artapp_str;
	if (artapp_env != nullptr)
	{
		artapp_str = std::string(artapp_env) + "_";
	}

	TLOG(TLVL_DEBUG + 33) << "Setting app_name";

	artapp_env = getenv("ARTDAQ_RANK");
	if (artapp_env != nullptr && my_rank < 0)
	{
		TLOG(TLVL_DEBUG + 33) << "Setting rank from envrionment";
		my_rank = strtol(artapp_env, nullptr, 10);
		app_name = artapp_str + "art" + std::string(artapp_env);
	}
	else
	{
		TLOG(TLVL_DEBUG + 33) << "Setting default rank and name";
		my_rank = -1;
		app_name = artapp_str + "art";
	}

	try
	{
		if (metricMan)
		{
			metricMan->initialize(pset.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), app_name);
			metricMan->do_start();
		}
	}
	catch (...)
	{
		artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no, "Error loading metrics in ArtdaqGlobalsService()");
	}

	TLOG(TLVL_INFO) << "app_name is " << app_name << ", rank " << my_rank;
}

ArtdaqGlobalsService::~ArtdaqGlobalsService()
{
	artdaq::Globals::CleanUpGlobals();
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqGlobalsService, ArtdaqSharedMemoryServiceInterface)