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
		 * \brief MPITransfer Destructor
		 */
		virtual ~MPITransfer();

		/**
		 * \brief Copy a Fragment to the destination. Forces asynchronous send
		 * \param frag Fragment to copy
		 * \param timeout_usec Timeout for send, in microseconds
		 * \return CopyStatus detailing result of copy
		 */
		CopyStatus copyFragment(Fragment& frag, size_t timeout_usec) override;

		/**
		 * \brief Move a Fragment to the destination.
		 * \param frag Fragment to move
		 * \param timeout_usec Timeout for send, in microseconds
		 * \return CopyStatus detailing result of copy
		 */
		CopyStatus moveFragment(Fragment&& frag, size_t timeout_usec) override;

		/**
		 * \brief Receive a Fragment using MPI
		 * \param[out] frag Received Fragment
		 * \param timeout_usec Timeout for receive, in microseconds
		 * \return Rank of sender or RECV_TIMEOUT
		 */
		int receiveFragment(Fragment& frag, size_t timeout_usec) override;

	private:
		enum class status_t
		{
			SENDING,
			PENDING,
			DONE
		};

		void cancelReq_(size_t buf, bool blocking_wait = true);

		void post_(size_t buf);

		// Identify an available buffer.
		int findAvailable();

		TransferInterface::CopyStatus sendFragment(Fragment&& frag, size_t timeout_usec, bool force_async);

		int nextSource_();

		status_t src_status_; // Status of each sender.
		size_t recvd_count_;
		size_t expected_count_; // After EOD received: expected frags.

		Fragments payload_;

#if USE_TESTSOME
		int saved_wait_result_;
		std::vector<int> ready_indices_;
		std::vector<MPI_Status> ready_statuses_;
#endif
		static std::mutex mpi_mutex_;
		bool synchronous_sends_;

		std::vector<MPI_Request> reqs_;
		int pos_;
	};
}

#endif //define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
