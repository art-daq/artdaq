#ifndef ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH
#define ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH

#include <map>
#include <set>
#include <memory>
#include <thread>
#include <condition_variable>

#include <fhiclcpp/fwd.h>

#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"

namespace artdaq {
	class DataReceiverManager;
}

class artdaq::DataReceiverManager {
public:

	DataReceiverManager(fhicl::ParameterSet);
	~DataReceiverManager();

	// recvFragment() puts the next received fragment in frag, with the
	// source of that fragment as its return value.
	//
	// It is a precondition that a sources_sending() != 0.
	FragmentPtr recvFragment(int& rank, size_t timeout_usec = 0);

	// How many fragments have been received using this DataReceiverManager object?
	size_t count() const;

	// How many fragments have been received from a particular destination.
	size_t slotCount(size_t rank) const;

	// Total size received using this DataReceiverManager object
	size_t byteCount() const;

	void start_threads();

	std::set<int> enabled_sources() const { return enabled_sources_; }

	void suppressSource(int source) { suppressed_sources_.insert(source); }
	void unsuppressAll() { suppressed_sources_.clear(); }
void reject_fragment(int source_rank, FragmentPtr frag);

private:
	void runReceiver_(int);

	std::atomic<bool> stop_requested_;

	std::atomic<size_t> fragment_ready_;
	std::condition_variable fragment_ready_cv_;
	std::mutex ready_mutex_;

	std::condition_variable fragment_requested_;
	std::mutex req_mutex_;

	std::condition_variable fragment_sent_;
	std::mutex snt_mutex_;

	std::map<int, std::thread> source_threads_;
	std::map<int, std::unique_ptr<TransferInterface>> source_plugins_;
	std::set<int> enabled_sources_;
	std::set<int> suppressed_sources_;

	std::map<int, FragmentPtrs> fragment_store_;

	std::atomic<int> current_source_;
	std::mutex fragment_mutex_;

	detail::FragCounter recv_frag_count_; // Number of frags received per source.
	detail::FragCounter recv_frag_size_; // Number of bytes received per source.
	size_t suppression_threshold_;

	size_t receive_timeout_;
};


inline
size_t
artdaq::DataReceiverManager::
count() const
{
	return recv_frag_count_.count();
}

inline
size_t
artdaq::DataReceiverManager::
slotCount(size_t rank) const
{
	return recv_frag_count_.slotCount(rank);
}

inline
size_t
artdaq::DataReceiverManager::
byteCount() const
{
	return recv_frag_size_.count();
}
#endif //ARTDAQ_DAQRATE_DATATRANSFERMANAGER_HH
