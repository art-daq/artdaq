#ifndef artdaq_TransferPlugins_ShmemTransfer_hh
#define artdaq_TransferPlugins_ShemmTransfer_hh

#include "fhiclcpp/fwd.h"
#include <atomic>

#include "artdaq/TransferPlugins/TransferInterface.hh"

#define BUFFER_EMPTY 0UL
#define WRITING_FRAGMENT 1UL
#define FRAGMENT_READY 2UL
#define READING_FRAGMENT 3UL

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

		bool readyForRead_();

		bool readyForWrite_();

		RawDataType* offsetToPtr(size_t offset);

		struct ShmBuffer
		{
			size_t offset;
			size_t fragmentSizeWords;
			std::atomic<unsigned int> sem;
			unsigned int writeCount;
		};

		struct ShmStruct
		{
			std::atomic<unsigned int> read_pos;
			std::atomic<unsigned int> write_pos;
			ShmBuffer buffers[100];
		};


		size_t send_timeout_usec_;
		int shm_segment_id_;
		ShmStruct* shm_ptr_;
		int shm_key_;

		Role role_;
	};
}

#endif // artdaq_TransferPlugins/ShmemTransfer_hh
