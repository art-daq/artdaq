#include <chrono>

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataReceiverManager::DataReceiverManager(fhicl::ParameterSet pset)
	: stop_requested_(false)
	, fragment_ready_(0)
	, source_threads_()
	, source_plugins_()
	, enabled_sources_()
	, suppressed_sources_()
	, fragment_store_()
	, current_source_(-1)
	, recv_frag_count_()
	, suppression_threshold_(pset.get<size_t>("max_receive_difference", 50))
	, receive_timeout_(pset.get<size_t>("receive_timeout_usec", 1000))
{
	mf::LogDebug("DataReceiverManager") << "Constructor";
	auto enabled_srcs = pset.get<std::vector<size_t>>("enabled_sources", std::vector<size_t>());
	auto enabled_srcs_empty = enabled_srcs.size() == 0;
	if (enabled_srcs_empty) {
		mf::LogInfo("DataReceiverManager") << "enabled_sources not specified, assuming all sources enabled.";
	}
	else {
		for (auto& s : enabled_srcs) {
			enabled_sources_.insert(s);
		}
	}

	auto srcs = pset.get<fhicl::ParameterSet>("sources", fhicl::ParameterSet());
	for (auto& s : srcs.get_pset_names()) {
		try {
			auto ss = std::stoi(s.substr(1));
			if (enabled_srcs_empty) enabled_sources_.insert(ss);
			source_plugins_[ss] = std::unique_ptr<TransferInterface>(MakeTransferPlugin(srcs, s, TransferInterface::Role::kReceive));
			fragment_store_[ss] = FragmentPtrs();
		}
		catch (std::invalid_argument) {
			TRACE(3, "Invalid source specification: " + s);
		}
		catch (cet::exception ex) {
			mf::LogWarning("DataReceiverManager") << "cet::exception caught while setting up source " << s << ": " << ex.what();
		}
		catch (...) {
			mf::LogWarning("DataReceiverManager") << "Non-cet exception caught while setting up source " << s << ".";
		}
	}
	if (srcs.get_pset_names().size() == 0) {
		mf::LogError("DataReceiverManager") << "No sources configured!";
	}
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
	mf::LogDebug("DataReceiverManager") << "Destructor";
	TRACE(5, "~DataReceiverManager: BEGIN: Setting stop_requested to true, frags=%zu, bytes=%zu", count(), byteCount());
	stop_requested_ = true;

	TRACE(5, "~DataReceiverManager: Notifying all threads");
	fragment_requested_.notify_all();

	TRACE(5, "~DataReceiverManager: Joining all threads");
	for (auto& s : source_threads_) {
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
	TRACE(5, "~DataReceiverManager: DONE");
}

void artdaq::DataReceiverManager::reject_fragment(int source_rank, FragmentPtr frag)
{
	if (frag == nullptr) return;
	std::unique_lock<std::mutex> lk(fragment_mutex_);
	fragment_store_[source_rank].emplace_front(std::move(frag));
	suppressed_sources_.insert(source_rank);
}

void artdaq::DataReceiverManager::start_threads()
{
	for (auto& source : source_plugins_) {
		auto& rank = source.first;
		if (enabled_sources_.count(rank)) {
			source_threads_[rank] = std::thread(&DataReceiverManager::runReceiver_, this, rank);
		}
	}
}

artdaq::FragmentPtr artdaq::DataReceiverManager::recvFragment(int& rank, size_t timeout_usec)
{
	TRACE(5, "DataReceiverManager::recvFragment entered tmo=%zu us", timeout_usec);

	if (timeout_usec == 0) timeout_usec = 1000000;

	TRACE(5, "DataReceiverManager::recvFragment fragment_ready_=%zu before wait", fragment_ready_.load());
	if (fragment_ready_ == 0) {
		std::unique_lock<std::mutex> lck(ready_mutex_);
		fragment_ready_cv_.wait_for(lck, std::chrono::microseconds(timeout_usec));
	}
	TRACE(5, "DataReceiverManager::recvFragment fragment_ready_=%zu after wait", fragment_ready_.load());
	if (fragment_ready_ == 0) {
		TRACE(5, "DataReceiverManager::recvFragment: No fragments ready, returning empty");
		rank = TransferInterface::RECV_TIMEOUT;
		return std::unique_ptr<Fragment>{};
	}

	while (current_source_ == -1)
	{
		TRACE(5, "DataReceiverManager::recvFragment: Fragment(s) are ready. Notifying one");
		fragment_requested_.notify_one();
		TRACE(5, "DataReceiverManager::recvFragment: Waiting for fragment to be sent from Transfer Plugin");
		std::unique_lock<std::mutex> lck2(snt_mutex_);
		fragment_sent_.wait_for(lck2, std::chrono::microseconds(10000));
	}

	// This function now holds ownership of current_fragment_
	std::unique_lock<std::mutex> lk(fragment_mutex_);
	FragmentPtr current_fragment;
	if (fragment_store_[current_source_].size() > 0) {
		current_fragment = std::move(fragment_store_[current_source_].front());
		fragment_store_[current_source_].pop_front();
	}
	rank = current_source_;
	current_source_ = -1;
	fragment_ready_--;

	if (current_fragment != nullptr) TRACE(5, "DataReceiverManager::recvFragment: Done  rank=%d, fragment size=%zu words, seqId=%zu", rank, current_fragment->size(), current_fragment->sequenceID());
	return std::move(current_fragment);
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	while (!stop_requested_ && enabled_sources_.count(source_rank)) {
		TRACE(5, "DataReceiverManager::runReceiver_: Begin loop");
		{
			std::unique_lock<std::mutex> lck(snt_mutex_);
			while (!stop_requested_ &&
				(recv_frag_count_.slotCount(source_rank) > suppression_threshold_ + recv_frag_count_.minCount()
					|| suppressed_sources_.count(source_rank) > 0
					|| fragment_store_[source_rank].size() > 0))
			{
				TRACE(5, "DataReceiverManager::runReceiver_: Suppressing receiver rank %d", source_rank);
				fragment_sent_.wait_for(lck, std::chrono::seconds(1));
				if (suppressed_sources_.count(source_rank) == 0 && fragment_store_[source_rank].size() > 0) {
					fragment_sent_.notify_all();
				}
			}
			if (stop_requested_) return;
		}

		auto start_time = std::chrono::steady_clock::now();
		TRACE(5, "DataRecevierManager::runReceiver_: Calling receiveFragment");
		auto fragment = std::unique_ptr<Fragment>(new Fragment());
		auto ret = source_plugins_[source_rank]->receiveFragment(*fragment, receive_timeout_);
		TRACE(5, "DataReceiverManager::runReceiver_: Done with receiveFragment, ret=%d (should be %d)", ret, source_rank);

		if (ret != source_rank) continue; // Receive timeout or other oddness

		recv_frag_count_.incSlot(source_rank);
		recv_frag_size_.incSlot(source_rank, fragment->size() * sizeof(RawDataType));

		bool endOfData = fragment->type() == artdaq::Fragment::EndOfDataFragmentType;

		if (metricMan && recv_frag_count_.slotCount(source_rank) % 100 == 0) {
			TRACE(5, "DataReceiverManager::runReceiver_: Sending receive stats");
			auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
			metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 1);
			metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), fragment->size() * sizeof(RawDataType), "B", 1);
			metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), fragment->size() * sizeof(RawDataType) / delta_t, "B/s", 1);
		}

		fragment_ready_++;
		fragment_ready_cv_.notify_all();

		{
			TRACE(5, "DataReceiverManager::runReceiver_: Entering wait for condition variable");
			auto sts = std::cv_status::timeout;
			while (!stop_requested_ && (sts == std::cv_status::timeout || current_source_ != -1)) {
				std::unique_lock<std::mutex> lck(req_mutex_);
				sts = fragment_requested_.wait_for(lck, std::chrono::seconds(1));
			}
			TRACE(5, "DataReceiverManager::runReceiver_: Exit wait for condition variable");
		}
		if (stop_requested_) return;

		TRACE(5, "DataReceiverManager::runReceiver_: Notifying people waiting on fragment_sent_ and setting current_source_ to %d", source_rank);
		current_source_ = source_rank;
		{
			// This function now holds ownership of current_fragment_
			std::unique_lock<std::mutex> fragLock(fragment_mutex_);
			fragment_store_[source_rank].emplace_back(std::move(fragment));
		}
		fragment_sent_.notify_all();

		if (endOfData) {
			return;
		}
	}
}
