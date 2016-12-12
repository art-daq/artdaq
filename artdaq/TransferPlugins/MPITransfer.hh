#ifndef ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
#define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH

#include <algorithm>
#include <vector>

#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "cetlib/container_algorithms.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"

#include "artdaq/DAQrate/MPITag.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQdata/Debug.hh"
#include "artdaq/DAQrate/Utils.hh"

#include "artdaq/TransferPlugins/TransferInterface.hh"

#define TRACE_NAME "MPI_Transfer"
#include "trace.h"		// TRACE

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

	typedef std::vector<MPI_Request> Requests;

	MPITransfer(fhicl::ParameterSet, TransferInterface::Role);
	~MPITransfer();

	// Send the fragment to the specified destination.
	virtual TransferInterface::CopyStatus copyFragment(Fragment& frag, size_t timeout_usec);
	virtual TransferInterface::CopyStatus moveFragment(Fragment&& frag, size_t timeout_usec);
  private:
	enum class status_t { SENDING, PENDING, DONE };

	void waitAll_();

	void cancelReq_(size_t buf, bool blocking_wait = true);
	void post_(size_t buf);
	void cancelAndRepost_(size_t buf);

	// Identify an available buffer.
	size_t findAvailable();

	TransferInterface::CopyStatus sendFragment(Fragment&& frag, size_t timeout_usec, bool force_async);

	size_t receiveFragment(Fragment& frag, size_t timeout_usec);

	int nextSource_();

	size_t buffer_count_;
	size_t max_payload_size_;
	status_t src_status_; // Status of each sender.
	size_t recvd_count_;
	size_t expected_count_; // After EOD received: expected frags.

	std::vector<int> req_sources_; // Source for each request.

	Fragments payload_;

	int saved_wait_result_;
	std::vector<int> ready_indices_;
	std::vector<MPI_Status> ready_statuses_;

	bool synchronous_sends_;

	Requests reqs_;
	size_t pos_;
  };
}

#endif //define ARTDAQ_TRANSFERPLUGINS_MPITRANSFER_HH
