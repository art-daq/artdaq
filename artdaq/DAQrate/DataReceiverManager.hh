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
	class FragmentStoreElement;
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

	void suppress_source(int source);
	void unsuppressAll();
	void reject_fragment(int source_rank, FragmentPtr frag);

private:
	void runReceiver_(int);
	bool fragments_ready_() const;
	int get_next_source_() const;

	std::atomic<bool> stop_requested_;

	std::map<int, std::thread> source_threads_;
	std::map<int, std::unique_ptr<TransferInterface>> source_plugins_;
	std::set<int> enabled_sources_;
	std::set<int> suppressed_sources_;

	std::map<int, FragmentStoreElement> fragment_store_;

	std::mutex input_cv_mutex_;
	std::condition_variable input_cv_;
	std::mutex output_cv_mutex_;
	std::condition_variable output_cv_;

	detail::FragCounter recv_frag_count_; // Number of frags received per source.
	detail::FragCounter recv_frag_size_; // Number of bytes received per source.
	detail::FragCounter recv_seq_count_; // For counting sequence IDs
	size_t suppression_threshold_;

	size_t receive_timeout_;
};

class artdaq::FragmentStoreElement {
public:
	FragmentStoreElement() : frags_(), empty_(true) {
		std::cout << "FragmentStoreElement CONSTRUCTOR" << std::endl;
	}

	bool empty() const {
		return empty_; 
	}

	void emplace_front(FragmentPtr&& frag) {
		std::unique_lock<std::mutex> lk(mutex_);
		frags_.emplace_front(std::move(frag));
		empty_ = false;
	}

	void emplace_back(FragmentPtr&& frag) {
		std::unique_lock<std::mutex> lk(mutex_);
		frags_.emplace_back(std::move(frag));
		empty_ = false;
	}

	FragmentPtr front() {
		std::unique_lock<std::mutex> lk(mutex_);
		auto current_fragment = std::move(frags_.front());
		frags_.pop_front();
		empty_ = frags_.size() == 0;
		return std::move(current_fragment);
	}
private:
	mutable std::mutex mutex_;
	FragmentPtrs frags_;
	std::atomic<bool> empty_;
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
