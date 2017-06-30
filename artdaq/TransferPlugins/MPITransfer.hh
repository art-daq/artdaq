#ifndef ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
#define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH

#include <vector>

#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
*/

namespace artdaq
{
	/**
	 * \brief MPITransfer is a TransferInterface implementation plugin that transfers data using MPI
	 */
	class MPITransfer : public TransferInterface
	{
	public:
		/**
		 * \brief MPITransfer Constructor
		 * \param pset ParameterSet used to configure MPITransfer
		 * \param role Role of this MPITransfer instance (kSend or kReceive)
		 *
		 * \verbatim
		 * MPITransfer accepts the following Parameters:
		 * "synchronous_sends" (Default: true): When false, use MPI_ISend, otherwise, use MPI_SSend
		 * \endverbatim
		 * MPITransfer also requires all Parameters for configuring a TransferInterface
		 */
		MPITransfer(fhicl::ParameterSet pset, Role role);

		/**
		 * \brief Copy Constructor is deleted
		 */
		MPITransfer(const MPITransfer&) = delete;

		/**
		 * \brief Copy Assignment operator is deleted
		 * \return MPITransfer reference
		 */
		MPITransfer& operator=(const MPITransfer&) = delete;

		/**
		 * \brief MPITransfer Destructor
		 */
		virtual ~MPITransfer();

		/**
		 * \brief Copy a Fragment to the destination. Forces asynchronous send
		 * \param frag Fragment to copy
		 * \param timeout_usec Timeout for send, in microseconds
		 * \return CopyStatus detailing result of copy
		 */
		CopyStatus copyFragment(Fragment& frag, size_t timeout_usec = std::numeric_limits<size_t>::max()) override;

		/**
		 * \brief Move a Fragment to the destination.
		 * \param frag Fragment to move
		 * \param timeout_usec Timeout for send, in microseconds
		 * \return CopyStatus detailing result of copy
		 */
		CopyStatus moveFragment(Fragment&& frag, size_t timeout_usec = std::numeric_limits<size_t>::max()) override;

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
		int receiveFragmentData(RawDataType* destination, size_t wordCount, size_t receiveTimeout) override;

	private:

		void cancelReq_(MPI_Request req) const;
		
		// Identify an available buffer.
		int findAvailable();
						
		static std::mutex mpi_mutex_;

		std::vector<MPI_Request> reqs_;
		Fragments payload_;
		int pos_;
	};
}

#endif //define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
