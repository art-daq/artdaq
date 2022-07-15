#ifndef artdaq_ArtModules_ArtdaqGlobalsService_h
#define artdaq_ArtModules_ArtdaqGlobalsService_h

#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"
#include "artdaq-core/Data/RawEvent.hh"
#include "fhiclcpp/types/Atom.h"

// ----------------------------------------------------------------------

/**
 * \brief ArtdaqGlobalsService extends ArtdaqSharedMemoryServiceInterface.
 * It manages the artdaq Global varaibles my_rank and app_name, and initializes MetricManager.
 * Users should retrieve a ServiceHandle to this class before using artdaq Globals to ensure the correct values are used.
 */
class ArtdaqGlobalsService : public ArtdaqSharedMemoryServiceInterface
{
public:
	/// <summary>
	/// Allowed Configuration parameters of ArtdaqGlobalsService. May be used for configuration validation
	/// </summary>
	struct Config
	{
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
	 * \brief ArtdaqGlobalsService Destructor. Calls disconnect().
	 */
	virtual ~ArtdaqGlobalsService();

	/**
	 * \brief ArtdaqGlobalsService Constructor
	 * \param pset ParameterSet used to configure ArtdaqGlobalsService. See ArtdaqGlobalsService::Config
	 */
	ArtdaqGlobalsService(fhicl::ParameterSet const& pset, art::ActivityRegistry&);

	/**
	 * \brief Pretend to receive an event from the shared memory
	 * \return Map of Fragment types retrieved from shared memory
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> ReceiveEvent(bool) override {
		return std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>>();
	}

	/**
	 * \brief Get the number of events which are ready to be read (0)
	 * \return The number of events which can be read (0)
	 */
	size_t GetQueueSize() override { return 0; }
	/**
	 * \brief Get the maximum number of events which can be stored in the shared memory (0)
	 * \return The maximum number of events which can be stored in the shared memory (0)
	 */
	size_t GetQueueCapacity() override { return 0; }
	/**
	 * \brief Get a shared_ptr to the current event header, if any
	 * \return Nullptr
	 */
	std::shared_ptr<artdaq::detail::RawEventHeader> GetEventHeader() override { return nullptr; }

private:
	ArtdaqGlobalsService(ArtdaqGlobalsService const&) = delete;
	ArtdaqGlobalsService(ArtdaqGlobalsService&&) = delete;
	ArtdaqGlobalsService& operator=(ArtdaqGlobalsService const&) = delete;
	ArtdaqGlobalsService& operator=(ArtdaqGlobalsService&&) = delete;

};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqGlobalsService, ArtdaqSharedMemoryServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqGlobalsService_h */

// Local Variables:
// mode: c++
// End:
