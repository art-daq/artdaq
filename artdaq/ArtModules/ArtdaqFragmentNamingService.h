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

	ArtdaqFragmentNamingServiceInterface(fhicl::ParameterSet const& ps)
	    : type_map_()
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
			 * \brief Returns the basic translation for the specified type. Must be implemented by derived classes
			 */
	virtual std::string GetInstanceNameForType(artdaq::Fragment::type_t type_id, std::string unidentified_instance_name) = 0;

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
	GetInstanceNameForFragment(artdaq::Fragment const& fragment, std::string unidentified_instance_name) = 0;

protected:
	std::map<artdaq::Fragment::type_t, std::string> type_map_;  ///< Map relating Fragment Type to strings
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
	virtual std::string GetInstanceNameForType(artdaq::Fragment::type_t type_id, std::string unidentified_instance_name);

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
	GetInstanceNameForFragment(artdaq::Fragment const& fragment, std::string unidentified_instance_name);

private:
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqFragmentNamingService, ArtdaqFragmentNamingServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqFragmentNamingService_h */

// Local Variables:
// mode: c++
// End:
