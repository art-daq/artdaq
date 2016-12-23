#include <chrono>

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataReceiverManager::DataReceiverManager(fhicl::ParameterSet pset)
	: stop_requested_(false)
	, sources_()
	, enabled_sources_()
	, current_source_(-1)
	, recv_frag_count_()
	, suppression_threshold_(pset.get<size_t>("max_receive_difference", 50))
	, receive_timeout_(pset.get<size_t>("receive_timeout_usec", 1000))
{
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
			sources_[ss] = SourceInfo(s, srcs);
		}
		catch (std::invalid_argument) {
			TRACE(3, "Invalid source specification: " + s);
		}
	}
	if (srcs.get_pset_names().size() == 0) {
		mf::LogError("DataReceiverManager") << "No sources configured!";
	}
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
	mf::LogDebug("DataReceiverManager") << "Shutting down DataReceiverManager. Received " << count() << " fragments.";
	stop_requested_ = true;
	for (auto& s : sources_) {
		auto& thread = s.second.thread;
		if (thread.joinable()) thread.join();
	}
}

void artdaq::DataReceiverManager::start_threads()
{
	for (auto& source : sources_) {
		auto& rank = source.first;
		auto& info = source.second;
		if (enabled_sources_.count(rank)) {
			info.thread = std::thread(&DataReceiverManager::runReceiver_, this, rank);
		}
	}
}

artdaq::FragmentPtr&& artdaq::DataReceiverManager::recvFragment(int& rank, size_t timeout_usec)
{
	TRACE(5, "DataReceiverManager::recvFragment entered tmo=%zu us", timeout_usec);

	current_source_ = -1;
	{
		std::unique_lock<std::mutex> lck(req_mutex_);
		fragment_requested_.notify_one();
	}
	{
		std::unique_lock<std::mutex> lck(snt_mutex_);
		fragment_sent_.wait_for(lck, std::chrono::microseconds(timeout_usec));
	}
	if (current_source_ == -1) {
		rank = TransferInterface::RECV_TIMEOUT;
		return std::move(std::unique_ptr<Fragment>(nullptr)); 
	}

	rank = current_source_;

	TRACE(5, "DataReceiverManager: Done with recvFragment, current_source_=%d, fragment size=%zu words", current_source_, sources_[current_source_].fragment->size());
	auto frag = std::unique_ptr<Fragment>(new Fragment());
	frag.swap(sources_[current_source_].fragment);
	return std::move(frag);
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	auto& info = sources_[source_rank];
	std::unique_ptr<artdaq::TransferInterface> theSource(MakeTransferPlugin(info.ps, info.name, TransferInterface::Role::kReceive));

	while (!stop_requested_ && enabled_sources_.count(source_rank)) {
		{
			std::unique_lock<std::mutex> lck(snt_mutex_);
			while (recv_frag_count_.slotCount(source_rank) > suppression_threshold_ + recv_frag_count_.minCount()) {
				fragment_sent_.wait(lck);
			}
		}

		auto start_time = std::chrono::steady_clock::now();
		auto ret = theSource->receiveFragment(*info.fragment, receive_timeout_);

		if (ret != source_rank) continue; // Receive timeout or other oddness

		recv_frag_count_.incSlot(source_rank);

		if (metricMan && recv_frag_count_.slotCount(source_rank) % 100 == 0) {
			auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
			metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(current_source_), delta_t, "s", 1);
			metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(current_source_), info.fragment->size() * sizeof(RawDataType), "B", 1);
			metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(current_source_), info.fragment->size() * sizeof(RawDataType) / delta_t, "B/s", 1);
		}

		{
			std::unique_lock<std::mutex> lck(req_mutex_);
			fragment_requested_.wait(lck);
			current_source_ = source_rank;
			fragment_sent_.notify_all();
		}
	}
}
