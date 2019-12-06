#ifndef artdaq_ArtModules_ArtdaqSharedMemoryService_h
#define artdaq_ArtModules_ArtdaqSharedMemoryService_h

#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "fhiclcpp/types/Atom.h"

/**
 * \brief Interface for ArtdaqSharedMemoryService. This interface is declared to art as part of the required registration of an art Service
 */
class ArtdaqSharedMemoryServiceInterface
{
public:
	/**
	 * \brief Default virtual destructor
	 */
	virtual ~ArtdaqSharedMemoryServiceInterface() = default;

	/**
	 * \brief Receive an event from the shared memory
	 * \param broadcast Whether to only attempt to receive a broadcast (broadcasts are always preferentially received over data)
	 * \return Map of Fragment types retrieved from shared memory
	 */
	virtual std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> ReceiveEvent(bool broadcast) = 0;

	/**
	 * \brief Get the number of events which are ready to be read
	 * \return The number of events which can be read
	 */
	virtual size_t GetQueueSize() = 0;

	/**
	 * \brief Get the maximum number of events which can be stored in the shared memory
	 * \return The maximum number of events which can be stored in the shared memory
	 */
	virtual size_t GetQueueCapacity() = 0;
};

DECLARE_ART_SERVICE_INTERFACE(ArtdaqSharedMemoryServiceInterface, LEGACY)

// ----------------------------------------------------------------------

/**
 * \brief ArtdaqSharedMemoryService extends ArtdaqSharedMemoryServiceInterface.
 * It receives events from shared memory using SharedMemoryEventReceiver. It also manages the artdaq Global varaibles my_rank and app_name.
 * Users should retrieve a ServiceHandle to this class before using artdaq Globals to ensure the correct values are used.
 */
class ArtdaqSharedMemoryService : public ArtdaqSharedMemoryServiceInterface
{
public:
	/// <summary>
	/// Allowed Configuration parameters of NetMonTransportService. May be used for configuration validation
	/// </summary>
	struct Config
	{
		/// "shared_memory_key" (Default: 0xBEE70000 + pid): Key to use when connecting to shared memory. Will default to 0xBEE70000 + getppid().
		fhicl::Atom<uint32_t> shared_memory_key{fhicl::Name{"shared_memory_key"}, fhicl::Comment{"Key to use when connecting to shared memory. Will default to 0xBEE70000 + getppid()."}, 0xBEE70000};
		/// "shared_memory_key" (Default: 0xCEE70000 + pid): Key to use when connecting to broadcast shared memory. Will default to 0xCEE70000 + getppid().
		fhicl::Atom<uint32_t> broadcast_shared_memory_key{fhicl::Name{"broadcast_shared_memory_key"}, fhicl::Comment{"Key to use when connecting to broadcast shared memory. Will default to 0xCEE70000 + getppid()."}, 0xCEE70000};
		/// "rank" (OPTIONAL) : The rank of this applicaiton, for use by non - artdaq applications running NetMonTransportService
		fhicl::Atom<int> rank{fhicl::Name{"rank"}, fhicl::Comment{"Rank of this artdaq application. Used for data transfers"}};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
	 * \brief NetMonTransportService Destructor. Calls disconnect().
	 */
	virtual ~ArtdaqSharedMemoryService();

	/**
	 * \brief NetMonTransportService Constructor
	 * \param pset ParameterSet used to configure NetMonTransportService and DataSenderManager. See NetMonTransportService::Config
	 */
	ArtdaqSharedMemoryService(fhicl::ParameterSet const& pset, art::ActivityRegistry&);

	/**
	 * \brief Receive an event from the shared memory
	 * \param broadcast Whether to only attempt to receive a broadcast (broadcasts are always preferentially received over data)
	 * \return Map of Fragment types retrieved from shared memory
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> ReceiveEvent(bool broadcast) override;

	/**
	 * \brief Get the number of events which are ready to be read
	 * \return The number of events which can be read
	 */
	size_t GetQueueSize() override { return incoming_events_->ReadReadyCount(); }
	/** 
	 * \brief Get the maximum number of events which can be stored in the shared memory
	 * \return The maximum number of events which can be stored in the shared memory
	 */
	size_t GetQueueCapacity() override { return incoming_events_->size(); }
	/**
	 * \brief Get a shared_ptr to the current event header, if any
	 * \return std::shared_ptr to current event header. May be nullptr if no event is currently being read
	 */
	std::shared_ptr<artdaq::detail::RawEventHeader> GetEventHeader() { return evtHeader_; }

private:
	std::unique_ptr<artdaq::SharedMemoryEventReceiver> incoming_events_;
	std::shared_ptr<artdaq::detail::RawEventHeader> evtHeader_;
	size_t read_timeout_;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqSharedMemoryService, ArtdaqSharedMemoryServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqSharedMemoryService_h */

// Local Variables:
// mode: c++
// End:
