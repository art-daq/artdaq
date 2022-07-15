#ifndef artdaq_ArtModules_ShmemWrapper_hh
#define artdaq_ArtModules_ShmemWrapper_hh

#include "TRACE/tracemf.h" // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"

#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/RawEvent.hh"

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
		 * \brief Receive a message from the ArtdaqSharedMemoryService
		* \return A list of unique_ptrs to received Fragments
		 */
	artdaq::FragmentPtrs receiveMessage();
	/**
	 * \brief Receive all messsages for an event from ArtdaqSharedMemoryService
	 * \return A map of Fragment::type_t to a unique_ptr to Fragments containing all Fragments in an event
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> receiveMessages();

	/**
		* \brief Receive an init message from the ArtdaqSharedMemoryService
		* \return A list of unique_ptrs to InitFragments
		*/
	artdaq::FragmentPtrs receiveInitMessage();

	/**
	 * \brief Get a pointer to the last received RawEventHeader
	 * \return a shared_ptr to the last received RawEventHeader
	 */
	std::shared_ptr<artdaq::detail::RawEventHeader> getEventHeader() { return hdr_ptr_; }

private:
	ShmemWrapper(ShmemWrapper const&) = delete;
	ShmemWrapper(ShmemWrapper&&) = delete;
	ShmemWrapper& operator=(ShmemWrapper const&) = delete;
	ShmemWrapper& operator=(ShmemWrapper&&) = delete;

	fhicl::ParameterSet data_pset_;
	bool init_received_;
	double init_timeout_s_;
	std::shared_ptr<artdaq::detail::RawEventHeader> hdr_ptr_;
};
}  // namespace art

#endif /* artdaq_ArtModules_ShmemWrapper_hh */
