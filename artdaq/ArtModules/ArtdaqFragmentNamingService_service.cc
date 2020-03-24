#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq/ArtModules/ArtdaqFragmentNamingService.h"

#include "TRACE/tracemf.h"
#define TRACE_NAME "ArtdaqFragmentNamingService"

ArtdaqFragmentNamingService::ArtdaqFragmentNamingService(fhicl::ParameterSet const& ps, art::ActivityRegistry&)
    : ArtdaqFragmentNamingServiceInterface(ps)
{
	TLOG(TLVL_DEBUG) << "ArtdaqFragmentNamingService CONSTRUCTOR START";
	TLOG(TLVL_DEBUG) << "ArtdaqFragmentNamingService CONSTRUCTOR END";
}

ArtdaqFragmentNamingService::~ArtdaqFragmentNamingService()
{
}

std::string ArtdaqFragmentNamingService::GetInstanceNameForType(artdaq::Fragment::type_t type_id)
{
	if (type_map_.count(type_id) > 0) { return type_map_[type_id]; }
	return unidentified_instance_name_;
}

std::set<std::string> ArtdaqFragmentNamingService::GetAllProductInstanceNames()
{
	std::set<std::string> output;
	for (const auto& map_iter : type_map_)
	{
		std::string instance_name = map_iter.second;
		if (!output.count(instance_name))
		{
			output.insert(instance_name);
			TLOG(TLVL_TRACE) << "Adding product instance name \"" << map_iter.second
			                 << "\" to list of expected names";
		}
	}

	auto container_type = type_map_.find(artdaq::Fragment::type_t(artdaq::Fragment::ContainerFragmentType));
	if (container_type != type_map_.end())
	{
		std::string container_type_name = container_type->second;
		std::set<std::string> tmp_copy = output;
		for (const auto& set_iter : tmp_copy)
		{
			output.insert(container_type_name + set_iter);
		}
	}

	return output;
}

std::pair<bool, std::string>
ArtdaqFragmentNamingService::GetInstanceNameForFragment(artdaq::Fragment const& fragment)
{
	auto type_map_end = type_map_.end();
	bool success_code = true;
	std::string instance_name;

	auto primary_type = type_map_.find(fragment.type());
	if (primary_type != type_map_end)
	{
		TLOG(TLVL_TRACE) << "Found matching instance name " << primary_type->second << " for Fragment type " << fragment.type();
		instance_name = primary_type->second;
		if (fragment.type() == artdaq::Fragment::ContainerFragmentType)
		{
			artdaq::ContainerFragment cf(fragment);
			auto contained_type = type_map_.find(cf.fragment_type());
			if (contained_type != type_map_end)
			{
				instance_name += contained_type->second;
			}
		}
	}
	else
	{
		TLOG(TLVL_TRACE) << "Could not find match for Fragment type " << fragment.type() << ", returning " << unidentified_instance_name_;
		instance_name = unidentified_instance_name_;
		success_code = false;
	}

	return std::make_pair(success_code, instance_name);
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqFragmentNamingService, ArtdaqFragmentNamingServiceInterface)
