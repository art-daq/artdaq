#ifndef artdaq_ArtModules_ArtdaqFragmentNamingService_h
#define artdaq_ArtModules_ArtdaqFragmentNamingService_h

#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "artdaq-core/Plugins/FragmentNameHelper.hh"
#include "fhiclcpp/types/Atom.h"

/**
 * \brief Interface for ArtdaqFragmentNamingService. This interface is declared to art as part of the required registration of an art Service
 */
class ArtdaqFragmentNamingServiceInterface
{
public:
	/**
	 * \brief Default virtual destructor
	 */
	virtual ~ArtdaqFragmentNamingServiceInterface() = default;

	/**
	 * \brief ArtdaqFragmentNamingServiceInterface constructor
	 * \param ps ParameterSet used to configure ArtdaqFragmentNamingServiceInterface
	 *
	 * ArtdaqFragmentNamingServiceInterface accepts the following Parameters:
	 * "unidentified_instance_name" (Default: "unidentified"): Name to use for any Fragments which are not successfully translated by the ArtdaqFragmentNamingServiceInterface
	 * "fragment_type_map" (Default: []): A list of Fragment type_t to string pairs for additional types to register with the ArtdaqFragmentNamingServiceInterface
	 */
	ArtdaqFragmentNamingServiceInterface(fhicl::ParameterSet const& ps)
	    : nameHelper_(nullptr)
	{
		auto unidentified_instance_name = ps.get<std::string>("unidentified_instance_name", "unidentified");
		auto extraTypes = ps.get<std::vector<std::pair<artdaq::Fragment::type_t, std::string>>>("fragment_type_map", std::vector<std::pair<artdaq::Fragment::type_t, std::string>>());
		auto fragmentNameHelperPluginType = ps.get<std::string>("helper_plugin", "Artdaq");

		nameHelper_ = artdaq::makeNameHelper(fragmentNameHelperPluginType, unidentified_instance_name, extraTypes);
	}

	/**
			 * \brief Returns the basic translation for the specified type. Must be implemented by derived classes
			 */
	std::string GetInstanceNameForType(artdaq::Fragment::type_t type_id) const { return nameHelper_->GetInstanceNameForType(type_id); }

	/**
			 * \brief Returns the full set of product instance names which may be present in the data, based on
			 *        the types that have been specified in the SetBasicTypes() and AddExtraType() methods.  This
			 *        *does* include "container" types, if the container type mapping is part of the basic types.
			 *  Must be implemented by derived classes
			 */
	std::set<std::string> GetAllProductInstanceNames() const { return nameHelper_->GetAllProductInstanceNames(); }

	/**
			 * \brief Returns the product instance name for the specified fragment, based on the types that have
			 *        been specified in the SetBasicTypes() and AddExtraType() methods.  This *does* include the
			 *        use of "container" types, if the container type mapping is part of the basic types.  If no
			 *        mapping is found, the specified unidentified_instance_name is returned.
			 * Must be implemented by derived classes
			 */
	std::pair<bool, std::string>
	GetInstanceNameForFragment(artdaq::Fragment const& fragment) const { return nameHelper_->GetInstanceNameForFragment(fragment); }

	/**
	 * @brief Get the name used for unidentified Fragment types
	 * @return The name used for unidentified Fragment types
	*/
	std::string GetUnidentifiedInstanceName() const { return nameHelper_->GetUnidentifiedInstanceName(); }

protected:
	std::shared_ptr<artdaq::FragmentNameHelper> nameHelper_;  ///< FragmentNameHelper plugin used to resolve Fragment names

private:
	ArtdaqFragmentNamingServiceInterface(ArtdaqFragmentNamingServiceInterface const&) = delete;
	ArtdaqFragmentNamingServiceInterface(ArtdaqFragmentNamingServiceInterface&&) = delete;
	ArtdaqFragmentNamingServiceInterface& operator=(ArtdaqFragmentNamingServiceInterface const&) = delete;
	ArtdaqFragmentNamingServiceInterface& operator=(ArtdaqFragmentNamingServiceInterface&&) = delete;
};
DECLARE_ART_SERVICE_INTERFACE(ArtdaqFragmentNamingServiceInterface, LEGACY)

// ----------------------------------------------------------------------

/**
 * \brief ArtdaqFragmentNamingService extends ArtdaqFragmentNamingServiceInterface.
 * This implementation uses the default SystemTypeMap and directly assigns names based on it
 */
class ArtdaqFragmentNamingService : public ArtdaqFragmentNamingServiceInterface
{
public:
	/**
	 * \brief DefaultArtdaqFragmentNamingService Destructor
	 */
	virtual ~ArtdaqFragmentNamingService();

	/**
	 * \brief NetMonTransportService Constructor
	 * \param pset ParameterSet used to configure NetMonTransportService and DataSenderManager. See NetMonTransportService::Config
	 */
	ArtdaqFragmentNamingService(fhicl::ParameterSet const& pset, art::ActivityRegistry&);

private:
	ArtdaqFragmentNamingService(ArtdaqFragmentNamingService const&) = delete;
	ArtdaqFragmentNamingService(ArtdaqFragmentNamingService&&) = delete;
	ArtdaqFragmentNamingService& operator=(ArtdaqFragmentNamingService const&) = delete;
	ArtdaqFragmentNamingService& operator=(ArtdaqFragmentNamingService&&) = delete;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqFragmentNamingService, ArtdaqFragmentNamingServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqFragmentNamingService_h */

// Local Variables:
// mode: c++
// End:
