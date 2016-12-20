#include <chrono>

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataReceiverManager::DataReceiverManager(fhicl::ParameterSet pset)
	: sources_()
	, enabled_sources_()
	, current_source_(0)
	, recv_frag_count_()
{
	auto srcs = pset.get<fhicl::ParameterSet>("sources", fhicl::ParameterSet());
	for (auto& s : srcs.get_pset_names()) {
		try {
			auto ss = std::stoi(s.substr(1));
			sources_.emplace(ss, MakeTransferPlugin(srcs, s, TransferInterface::Role::kReceive));
		}
		catch (std::invalid_argument) {
			TRACE(3, "Invalid source specification: " + s);
		}
	}
	if (sources_.size() == 0) {
		mf::LogError("DataReceiverManager") << "No sources configured!";
	}
	else {
		auto enabled_srcs = pset.get<std::vector<size_t>>("enabled_sources", std::vector<size_t>());
		if (enabled_srcs.size() == 0) {
			mf::LogInfo("DataReceiverManager") << "enabled_sources not specified, assuming all sources enabled.";
			for (auto& s : sources_) {
				enabled_sources_.insert(s.first);
			}
		}
		else {
			for (auto& s : enabled_srcs) {
				enabled_sources_.insert(s);
			}
		}
		current_source_ = *enabled_sources_.begin();
	}
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
	mf::LogDebug("DataReceiverManager") << "Shutting down DataReceiverManager. Received " << count() << " fragments.";
}

int artdaq::DataReceiverManager::calcSource() {
	size_t source = current_source_;
	auto next_iter = ++(enabled_sources_.find(source));
	if (next_iter == enabled_sources_.end()) next_iter = enabled_sources_.begin();
	if (next_iter == enabled_sources_.end()) return TransferInterface::RECV_TIMEOUT;
	return *next_iter;
}

int artdaq::DataReceiverManager::recvFragment(Fragment& frag, size_t timeout_usec)
{
  TRACE(5,"DataReceiverManager::recvFragment entered tmo=%zu us, frag.sizeofdata=%zu",timeout_usec,frag.size());
	auto start_time = std::chrono::steady_clock::now();
	current_source_ = calcSource();
	auto ret = current_source_;
	if (enabled_sources_.count(current_source_) && sources_.count(current_source_)) {
		ret = sources_[current_source_]->receiveFragment(frag, timeout_usec);
		recv_frag_count_.incSlot(current_source_);
	}
	if (metricMan) {
		auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
		metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(current_source_), delta_t, "s", 1);
		metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(current_source_), frag.size() * sizeof(RawDataType), "B", 1);
		metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(current_source_), frag.size() * sizeof(RawDataType) / delta_t, "B/s", 1);
	}
	TRACE(5, "DataReceiverManager: Done with recvFragment, ret=%d, current_source_=%d",ret, current_source_);
	return ret;
}
