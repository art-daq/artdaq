#include <chrono>

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataReceiverManager::DataReceiverManager(fhicl::ParameterSet pset)
	: stop_requested_(false)
	, source_threads_()
	, source_plugins_()
	, enabled_sources_()
	, suppressed_sources_()
	, fragment_store_()
	, recv_frag_count_()
	, recv_frag_size_()
	, recv_seq_count_()
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
	output_cv_.notify_all();

	TRACE(5, "~DataReceiverManager: Joining all threads");
	for (auto& s : source_threads_) {
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
	TRACE(5, "~DataReceiverManager: DONE");
}

bool artdaq::DataReceiverManager::fragments_ready_()
{
	std::unique_lock<std::mutex> lck(fragment_store_mutex_);
	for (auto& it : fragment_store_) {
		if (!enabled_sources_.count(it.first) || suppressed_sources_.count(it.first)) continue;
		if (it.second.size() > 0) return true;
	}
	return false;
}

int artdaq::DataReceiverManager::get_next_source_()
{
	std::unique_lock<std::mutex> lck(fragment_store_mutex_);
	for (auto& it : fragment_store_) {
		if (!enabled_sources_.count(it.first) || suppressed_sources_.count(it.first)) continue;
		if (it.second.size() > 0) return it.first;
	}
	return -1;
}

void artdaq::DataReceiverManager::unsuppressAll() {
	suppressed_sources_.clear();
	input_cv_.notify_all();
}

void artdaq::DataReceiverManager::suppress_source(int source)
{
	suppressed_sources_.insert(source);
}

void artdaq::DataReceiverManager::reject_fragment(int source_rank, FragmentPtr frag)
{
	if (frag == nullptr) return;
	std::unique_lock<std::mutex> lk(fragment_store_mutex_);
	suppress_source(source_rank);
	fragment_store_[source_rank].emplace_front(std::move(frag));
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

	auto ready = fragments_ready_();
	TRACE(5, "DataReceiverManager::recvFragment fragment_ready_=%d before wait", ready);
	if (!ready) {
		std::unique_lock<std::mutex> lck(fragment_store_mutex_);
		input_cv_.wait_for(lck, std::chrono::microseconds(timeout_usec));
	}
	ready = fragments_ready_();
	TRACE(5, "DataReceiverManager::recvFragment fragment_ready_=%d after wait", ready);
	if (!ready) {
		TRACE(5, "DataReceiverManager::recvFragment: No fragments ready, returning empty");
		rank = TransferInterface::RECV_TIMEOUT;
		return std::unique_ptr<Fragment>{};
	}

	int current_source = get_next_source_();
	FragmentPtr current_fragment;

	{
		std::unique_lock<std::mutex> lk(fragment_store_mutex_);
		current_fragment = std::move(fragment_store_[current_source].front());
		fragment_store_[current_source].pop_front();
	}
	output_cv_.notify_all();
	rank = current_source;

	if (current_fragment != nullptr) TRACE(5, "DataReceiverManager::recvFragment: Done  rank=%d, fragment size=%zu words, seqId=%zu", rank, current_fragment->size(), current_fragment->sequenceID());
	return std::move(current_fragment);
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	while (!stop_requested_ && enabled_sources_.count(source_rank)) {
		TRACE(6, "DataReceiverManager::runReceiver_: Begin loop");
		{
			std::unique_lock<std::mutex> lck(fragment_store_mutex_);
			while (!stop_requested_ &&
				(recv_seq_count_.slotCount(source_rank) > suppression_threshold_ + recv_seq_count_.minCount()
					|| suppressed_sources_.count(source_rank) > 0
					|| fragment_store_[source_rank].size() > 0))
			{
				TRACE(6, "DataReceiverManager::runReceiver_: Suppressing receiver rank %d", source_rank);
				output_cv_.wait_for(lck, std::chrono::seconds(1));
			}
		}
		if (stop_requested_) return;

		auto start_time = std::chrono::steady_clock::now();
		TRACE(6, "DataRecevierManager::runReceiver_: Calling receiveFragment");
		auto fragment = std::unique_ptr<Fragment>(new Fragment());
		auto ret = source_plugins_[source_rank]->receiveFragment(*fragment, receive_timeout_);
		TRACE(6, "DataReceiverManager::runReceiver_: Done with receiveFragment, ret=%d (should be %d)", ret, source_rank);

		if (ret != source_rank) continue; // Receive timeout or other oddness

		recv_frag_count_.incSlot(source_rank);
		recv_frag_size_.incSlot(source_rank, fragment->size() * sizeof(RawDataType));
		recv_seq_count_.setSlot(source_rank, fragment->sequenceID());

		bool endOfData = fragment->type() == artdaq::Fragment::EndOfDataFragmentType;

		if (metricMan && recv_frag_count_.slotCount(source_rank) % 100 == 0) {
			TRACE(6, "DataReceiverManager::runReceiver_: Sending receive stats");
			auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
			metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 1);
			metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(fragment->size() * sizeof(RawDataType)), "B", 1);
			metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), fragment->size() * sizeof(RawDataType) / delta_t, "B/s", 1);
		}

		if (stop_requested_) return;

		{
			// This function now holds ownership of current_fragment_
			std::unique_lock<std::mutex> fragLock(fragment_store_mutex_);
			fragment_store_[source_rank].emplace_back(std::move(fragment));
		}
		input_cv_.notify_all();

		if (endOfData) {
			return;
		}
	}
}
