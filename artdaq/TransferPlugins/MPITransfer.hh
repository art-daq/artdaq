#ifndef ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
#define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH

#include <vector>

#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

/*
  Protocol: want to do a send for each request object, then wait for for
  pending requests to complete, followed by a reset to allow another set
  of sends to be completed.

  This needs to be separated into a thing for sending and a thing for receiving.
  There probably needs to be a common class that both use.
*/

namespace artdaq {

	class MPITransfer : public TransferInterface {
	public:
		MPITransfer(fhicl::ParameterSet, TransferInterface::Role);
		~MPITransfer();

		// Send the fragment to the specified destination.
		virtual TransferInterface::CopyStatus copyFragment(Fragment& frag, size_t timeout_usec);
		virtual TransferInterface::CopyStatus moveFragment(Fragment&& frag, size_t timeout_usec);
		virtual int receiveFragment(Fragment& frag, size_t timeout_usec);
	private:
		enum class status_t { SENDING, PENDING, DONE };

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

		bool synchronous_sends_;

		std::vector<MPI_Request> reqs_;
		int pos_;
	};
}

#endif //define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
