#ifndef artdaq_TransferPlugins_ShmemTransfer_hh
#define artdaq_TransferPlugins_ShemmTransfer_hh

#include "fhiclcpp/fwd.h"

#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq-core/Core/SharedMemoryFragmentManager.hh"

namespace artdaq
{
	/**
	 * \brief A TransferInterface implementation plugin that transfers data using Shared Memory
	 */
	class ShmemTransfer : public TransferInterface
	{
	public:

		/**
		 * \brief ShmemTransfer Constructor
		 * \param pset ParameterSet used to configure ShmemTransfer
		 * \param role Role of this ShmemTransfer instance (kSend or kReceive)
		 * 
		 * \verbatim
		 * ShmemTransfer accepts the following Parameters:
		 * "shm_key" (Default: hash of uniqueLabel): Key to use for shared memory segment
		 * \endverbatim
		 * ShmemTransfer also requires all Parameters for configuring a TransferInterface
		 */
		ShmemTransfer(fhicl::ParameterSet const& pset, Role role);

		/**
		 * \brief ShmemTransfer Destructor
		 */
		virtual ~ShmemTransfer() noexcept;

		/**
		* \brief Receive a Fragment from Shared Memory
		* \param[out] fragment Received Fragment
		* \param receiveTimeout Timeout for receive, in microseconds
		* \return Rank of sender or RECV_TIMEOUT
		*/
		int receiveFragment(Fragment& fragment,
		                            size_t receiveTimeout) override;

		/**
		* \brief Receive a Fragment Header from the transport mechanism
		* \param[out] header Received Fragment Header
		* \param receiveTimeout Timeout for receive
		* \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		*/
		int receiveFragmentHeader(detail::RawFragmentHeader& header, size_t receiveTimeout) override;

		/**
		* \brief Receive the body of a Fragment to the given destination pointer
		* \param destination Pointer to memory region where Fragment data should be stored
		 * \param wordCount Number of words of Fragment data to receive
		* \param receiveTimeout Timeout for receive
		* \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		*/
		int receiveFragmentData(RawDataType* destination,size_t wordCount, size_t receiveTimeout) override;

		/**
		* \brief Copy a Fragment to the destination. May be unreliable
		* \param fragment Fragment to copy
		* \param send_timeout_usec Timeout for send, in microseconds (default size_t::MAX_VALUE)
		* \return CopyStatus detailing result of copy
		*/
		CopyStatus copyFragment(Fragment& fragment,
		                                size_t send_timeout_usec = std::numeric_limits<size_t>::max()) override;

		/**
		* \brief Move a Fragment to the destination.
		* \param fragment Fragment to move
		* \param send_timeout_usec Timeout for send, in microseconds (default size_t::MAX_VALUE)
		* \return CopyStatus detailing result of move
		*/
		CopyStatus moveFragment(Fragment&& fragment,
		                                size_t send_timeout_usec = std::numeric_limits<size_t>::max()) override;

	private:
		CopyStatus sendFragment(Fragment&& fragment,
		                        size_t send_timeout_usec, bool reliable = false);
		
		size_t send_timeout_usec_;
		std::unique_ptr<SharedMemoryFragmentManager> shm_manager_;

		Role role_;
	};
}

#endif // artdaq_TransferPlugins/ShmemTransfer_hh
