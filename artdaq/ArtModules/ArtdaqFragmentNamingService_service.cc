#include "TRACE/tracemf.h"
#define TRACE_NAME "ArtdaqFragmentNamingService"

#include "artdaq/ArtModules/ArtdaqFragmentNamingService.h"
#include "art/Framework/Services/Registry/ServiceDefinitionMacros.h"

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

ArtdaqFragmentNamingService::ArtdaqFragmentNamingService(fhicl::ParameterSet const& ps, art::ActivityRegistry& /*unused*/)
    : ArtdaqFragmentNamingServiceInterface(ps)
{
	TLOG(TLVL_DEBUG + 32) << "ArtdaqFragmentNamingService CONSTRUCTOR START";
	TLOG(TLVL_DEBUG + 32) << "ArtdaqFragmentNamingService CONSTRUCTOR END";
}

ArtdaqFragmentNamingService::~ArtdaqFragmentNamingService() = default;

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqFragmentNamingService, ArtdaqFragmentNamingServiceInterface)
