#ifndef artdaq_ArtModules_InFlightConfiguration_h
#define artdaq_ArtModules_InFlightConfiguration_h

#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "art/Framework/Services/System/PathSelection.h"
#include "art/Framework/Services/UserInteraction/UserInteraction.h"
#include "fhiclcpp/ParameterSet.h"

#include <vector>

namespace artdaq
{
	class InFlightConfiguration;
}

class artdaq::InFlightConfiguration : public ui::UserInteraction
{
public:
	InFlightConfiguration(fhicl::ParameterSet const& ps,
	                      art::ActivityRegistry& areg);

	void moduleList(std::vector<ModuleInfo> const& infos) override;

	void pickModule() override;

	UserInteraction::NextStep nextAction() override;

private:
	std::vector<ModuleInfo> moduleInfos_;
	art::ServiceHandle<art::PathSelection> pathSelectionService_;
};

DECLARE_ART_SERVICE(artdaq::InFlightConfiguration, LEGACY)
#endif /* artdaq_ArtModules_InFlightConfiguration_h */

// Local Variables:
// mode: c++
// End:
