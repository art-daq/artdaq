#ifndef artdaq_ArtModules_ArtdaqFragmentNamingService_h
#define artdaq_ArtModules_ArtdaqFragmentNamingService_h

#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "artdaq-core/Data/Fragment.hh"
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
	    : type_map_(), unidentified_instance_name_(ps.get<std::string>("unidentified_instance_name", "unidentified"))
	{
		SetBasicTypes(artdaq::Fragment::MakeSystemTypeMap());
		auto extraTypes = ps.get<std::vector<std::pair<artdaq::Fragment::type_t, std::string>>>("fragment_type_map", std::vector<std::pair<artdaq::Fragment::type_t, std::string>>());
		for (auto it = extraTypes.begin(); it != extraTypes.end(); ++it)
		{
			AddExtraType(it->first, it->second);
		}
	}

	/**
	 * \brief Sets the basic types to be translated.  (Should not include "container" types.)
	 */
	void SetBasicTypes(std::map<artdaq::Fragment::type_t, std::string> const& type_map)
	{
		for (auto& type_pair : type_map)
		{
			type_map_[type_pair.first] = type_pair.second;
		}
	}

	/**
	 * \brief Adds an additional type to be translated.
	 */
	void AddExtraType(artdaq::Fragment::type_t type_id, std::string type_name)
	{
		type_map_[type_id] = type_name;
	}

	/**
	 * \brief Get the configured unidentified_instance_name
	 * \return The configured unidentified_instance_name
	 */
	std::string GetUnidentifiedInstanceName() { return unidentified_instance_name_; }

	/**
			 * \brief Returns the basic translation for the specified type. Must be implemented by derived classes
			 */
	virtual std::string GetInstanceNameForType(artdaq::Fragment::type_t type_id) = 0;

	/**
			 * \brief Returns the full set of product instance names which may be present in the data, based on
			 *        the types that have been specified in the SetBasicTypes() and AddExtraType() methods.  This
			 *        *does* include "container" types, if the container type mapping is part of the basic types.
			 *  Must be implemented by derived classes
			 */
	virtual std::set<std::string> GetAllProductInstanceNames() = 0;

	/**
			 * \brief Returns the product instance name for the specified fragment, based on the types that have
			 *        been specified in the SetBasicTypes() and AddExtraType() methods.  This *does* include the
			 *        use of "container" types, if the container type mapping is part of the basic types.  If no
			 *        mapping is found, the specified unidentified_instance_name is returned.
			 * Must be implemented by derived classes
			 */
	virtual std::pair<bool, std::string>
	GetInstanceNameForFragment(artdaq::Fragment const& fragment) = 0;

protected:
	std::map<artdaq::Fragment::type_t, std::string> type_map_;  ///< Map relating Fragment Type to strings
	std::string unidentified_instance_name_;                    ///< The name to use for unknown Fragment types
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

	/**
			 * \brief Returns the basic translation for the specified type.  Defaults to the specified
			 *        unidentified_instance_name if no translation can be found.
			 */
	virtual std::string GetInstanceNameForType(artdaq::Fragment::type_t type_id);

	/**
			 * \brief Returns the full set of product instance names which may be present in the data, based on
			 *        the types that have been specified in the SetBasicTypes() and AddExtraType() methods.  This
			 *        *does* include "container" types, if the container type mapping is part of the basic types.
			 */
	virtual std::set<std::string> GetAllProductInstanceNames();

	/**
			 * \brief Returns the product instance name for the specified fragment, based on the types that have
			 *        been specified in the SetBasicTypes() and AddExtraType() methods.  This *does* include the
			 *        use of "container" types, if the container type mapping is part of the basic types.  If no
			 *        mapping is found, the specified unidentified_instance_name is returned.
			 */
	virtual std::pair<bool, std::string>
	GetInstanceNameForFragment(artdaq::Fragment const& fragment);

private:
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqFragmentNamingService, ArtdaqFragmentNamingServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqFragmentNamingService_h */

// Local Variables:
// mode: c++
// End:
