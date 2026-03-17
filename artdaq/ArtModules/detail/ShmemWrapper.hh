#ifndef artdaq_ArtModules_ShmemWrapper_hh
#define artdaq_ArtModules_ShmemWrapper_hh

#include "TRACE/tracemf.h"  // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"

#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq/ArtModules/ArtdaqEvent.hh"

#include <memory>
#include <string>
#include <unordered_map>

namespace art {
/**
 * \brief This class wraps ArtdaqSharedMemoryService so that it can act as an ArtdaqInput
 * template class.
 *
 * JCF, May-27-2016
 *
 * This class is written with functionality such that it satisfies the
 * requirements needed to be a template in the ArtdaqInput class
 */
class ShmemWrapper
{
public:
	/**
	 * \brief ShmemWrapper Constructor
	 * \param ps ParameterSet for ShmemWrapper
	 */
	ShmemWrapper(fhicl::ParameterSet const& ps);

	/**
	 * \brief ShmemWrapper Destructor
	 */
	virtual ~ShmemWrapper() = default;

	/**
	 * \brief Receive all messsages for an event from ArtdaqSharedMemoryService
	 * \return A map of Fragment::type_t to a unique_ptr to Fragments containing all Fragments in an event
	 */
	std::shared_ptr<ArtdaqEvent> receiveMessages();

	/**
	 * \brief Receive an init message from the ArtdaqSharedMemoryService
	 * \return A list of unique_ptrs to InitFragments
	 */
	artdaq::FragmentPtrs receiveInitMessage();

private:
	ShmemWrapper(ShmemWrapper const&) = delete;
	ShmemWrapper(ShmemWrapper&&) = delete;
	ShmemWrapper& operator=(ShmemWrapper const&) = delete;
	ShmemWrapper& operator=(ShmemWrapper&&) = delete;

	fhicl::ParameterSet data_pset_;
	bool init_received_;
	bool eod_received_{false};
	double init_timeout_s_;
};
}  // namespace art

#endif /* artdaq_ArtModules_ShmemWrapper_hh */
