#ifndef artdaq_ArtModules_ShmemWrapper_hh
#define artdaq_ArtModules_ShmemWrapper_hh

#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include <memory>
#include <string>

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
		 * \param[out] msg A pointer to the received message
		 */
	artdaq::FragmentPtrs receiveMessage();
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> receiveMessages();

	/**
		* \brief Receive an init message from the ArtdaqSharedMemoryService
		* \param[out] msg A pointer to the received message
		*/
	artdaq::FragmentPtrs receiveInitMessage();

	std::shared_ptr<artdaq::detail::RawEventHeader> getEventHeader() { return hdr_ptr_; }

private:
	fhicl::ParameterSet data_pset_;
	bool init_received_;
	double init_timeout_s_;
	std::shared_ptr<artdaq::detail::RawEventHeader> hdr_ptr_;
};
}  // namespace art

#endif /* artdaq_ArtModules_ShmemWrapper_hh */
