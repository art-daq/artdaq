#include "artdaq-core/Plugins/FragmentNameHelper.hh"

#include "TRACE/tracemf.h"
#define TRACE_NAME "GenericFragmentNameHelper"

namespace artdaq {
/**
 * \brief GenericFragmentNameHelper extends ArtdaqFragmentNamingService.
 * This implementation assigns name "Generic" to the first User Fragment type
 */
class GenericFragmentNameHelper : public FragmentNameHelper
{
public:
	/**
	 * \brief DefaultArtdaqFragmentNamingService Destructor
	 */
	~GenericFragmentNameHelper() override = default;

	/**
	 * \brief GenericFragmentNameHelper Constructor
	 */
	GenericFragmentNameHelper(std::string unidentified_instance_name, std::vector<std::pair<artdaq::Fragment::type_t, std::string>> extraTypes);

private:
	GenericFragmentNameHelper(GenericFragmentNameHelper const&) = delete;
	GenericFragmentNameHelper(GenericFragmentNameHelper&&) = delete;
	GenericFragmentNameHelper& operator=(GenericFragmentNameHelper const&) = delete;
	GenericFragmentNameHelper& operator=(GenericFragmentNameHelper&&) = delete;
};

GenericFragmentNameHelper::GenericFragmentNameHelper(std::string unidentified_instance_name, std::vector<std::pair<artdaq::Fragment::type_t, std::string>> extraTypes)
    : FragmentNameHelper(unidentified_instance_name, extraTypes)
{
	TLOG(TLVL_DEBUG + 32) << "GenericFragmentNameHelper CONSTRUCTOR START";
	auto output = artdaq::Fragment::MakeSystemTypeMap();
	output[artdaq::Fragment::FirstUserFragmentType] = "Generic";
	SetBasicTypes(output);
	TLOG(TLVL_DEBUG + 32) << "GenericFragmentNameHelper CONSTRUCTOR END";
}
}  // namespace artdaq

DEFINE_ARTDAQ_FRAGMENT_NAME_HELPER(artdaq::GenericFragmentNameHelper)
