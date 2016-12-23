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
	, current_fragment_(new Fragment())
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
  //size_t fragcount = count();
 //mf::LogDebug("DataReceiverManager") << "Shutting down DataReceiverManager. Received " << std::to_string((int)fragcount) << " fragments.";
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

	if(current_source_ == -1) // Protect against race condition where notify + wait_for isn't long enough
	{
		std::unique_lock<std::mutex> lck(req_mutex_);
		fragment_requested_.notify_one();
	}
	{
		std::unique_lock<std::mutex> lck(snt_mutex_);
		if(timeout_usec == 0) timeout_usec = 1000000; 
		fragment_sent_.wait_for(lck, std::chrono::microseconds(timeout_usec));
	}
	if (current_source_ == -1) {
		rank = TransferInterface::RECV_TIMEOUT;
		return std::move(std::unique_ptr<Fragment>(new Fragment())); 
	}

	rank = current_source_;
	current_source_ = -1;
	TRACE(5, "DataReceiverManager: Done with recvFragment, rank=%d, fragment size=%zu words", rank, current_fragment_->size());
	return std::move(current_fragment_);
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	auto& info = sources_[source_rank];
	TRACE(5, "DataReceiverManager: Seting up receiver with rank %d and name " + info.name, source_rank);
	std::unique_ptr<artdaq::TransferInterface> theSource(MakeTransferPlugin(info.ps, info.name, TransferInterface::Role::kReceive));

	while (!stop_requested_ && enabled_sources_.count(source_rank)) {
		{
			std::unique_lock<std::mutex> lck(snt_mutex_);
			while (!stop_requested_ && recv_frag_count_.slotCount(source_rank) > suppression_threshold_ + recv_frag_count_.minCount()) {
			  TRACE(5, "DataReceiverManager::runReceiver_: Suppressing receiver rank %d", source_rank);
			  fragment_sent_.wait_for(lck, std::chrono::seconds(1));
			}
			if(stop_requested_) return;
		}

		auto start_time = std::chrono::steady_clock::now();
		TRACE(5, "DataRecevierManager::runReceiver_: Calling receiveFragment");
		auto fragment = std::unique_ptr<Fragment>(new Fragment());
		auto ret = theSource->receiveFragment(*fragment, receive_timeout_);
		TRACE(5, "DataReceiverManager::runReceiver_: Done with receiveFragment, ret=%d (should be %d)",ret, source_rank);

		if (ret != source_rank) continue; // Receive timeout or other oddness

		recv_frag_count_.incSlot(source_rank);

		if (metricMan && recv_frag_count_.slotCount(source_rank) % 100 == 0) {
		  TRACE(5, "DataReceiverManager::runReceiver_: Sending receive stats");
			auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
			metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(current_source_), delta_t, "s", 1);
			metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(current_source_), fragment->size() * sizeof(RawDataType), "B", 1);
			metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(current_source_), fragment->size() * sizeof(RawDataType) / delta_t, "B/s", 1);
		}

		{
		  TRACE(5, "DataReceiverManager::runReceiver_: Entering wait for condition variable");
			std::unique_lock<std::mutex> lck(req_mutex_);
			auto sts = std::cv_status::timeout;
			while(!stop_requested_ && sts == std::cv_status::timeout) {
			  sts = fragment_requested_.wait_for(lck, std::chrono::seconds(1));
			}
			if(stop_requested_) return;

			TRACE(5, "DataReceiverManager::runReceiver_: Notifying people waiting on fragment_sent_ and setting current_source_ to %d", source_rank);
			current_source_ = source_rank;
			current_fragment_ = std::move(fragment);
			fragment_sent_.notify_all();
		}
	}
}
