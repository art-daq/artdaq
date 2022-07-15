#ifndef artdaq_ArtModules_ArtdaqSharedMemoryServiceInterface_h
#define artdaq_ArtModules_ArtdaqSharedMemoryServiceInterface_h

#include "art/Framework/Services/Registry/ServiceMacros.h"
#include "artdaq-core/Data/RawEvent.hh"
#include "fhiclcpp/types/Atom.h"

/**
 * \brief Interface for ArtdaqSharedMemoryService. This interface is declared to art as part of the required registration of an art Service
 */
class ArtdaqSharedMemoryServiceInterface
{
public:
	ArtdaqSharedMemoryServiceInterface() = default;

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

	/**
	 * \brief Get a shared_ptr to the current event header, if any
	 * \return std::shared_ptr to current event header. May be nullptr if no event is currently being read
	 */
	virtual std::shared_ptr<artdaq::detail::RawEventHeader> GetEventHeader() = 0;

private:
	ArtdaqSharedMemoryServiceInterface(ArtdaqSharedMemoryServiceInterface const&) = delete;
	ArtdaqSharedMemoryServiceInterface(ArtdaqSharedMemoryServiceInterface&&) = delete;
	ArtdaqSharedMemoryServiceInterface& operator=(ArtdaqSharedMemoryServiceInterface const&) = delete;
	ArtdaqSharedMemoryServiceInterface& operator=(ArtdaqSharedMemoryServiceInterface&&) = delete;
};

DECLARE_ART_SERVICE_INTERFACE(ArtdaqSharedMemoryServiceInterface, LEGACY)

#endif /* artdaq_ArtModules_ArtdaqSharedMemoryServiceInterface_h */

// Local Variables:
// mode: c++
// End:
