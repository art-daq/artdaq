#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"
#include "artdaq/DAQdata/Globals.hh"

#include <chrono>

artdaq::DataSenderManager::DataSenderManager(fhicl::ParameterSet pset)
	: destinations_()
	, enabled_destinations_()
	, sent_frag_count_()
	, broadcast_sends_(pset.get<bool>("broadcast_sends", false))
{
	mf::LogDebug("DataSenderManager") << "Received pset: " << pset.to_string();
	auto dests = pset.get<fhicl::ParameterSet>("destinations", fhicl::ParameterSet());
	for (auto& d : dests.get_pset_names()) {
		try {
			auto dd = std::stoi(d.substr(1));
			destinations_.emplace(dd, MakeTransferPlugin(dests, d, TransferInterface::Role::kSend));
		}
		catch (std::invalid_argument) {
			TRACE(3, "Invalid destination specification: " + d);
		}
	}
	if (destinations_.size() == 0) {
		mf::LogError("DataSenderManager") << "No destinations specified!";
	}
	else {
		auto enabled_dests = pset.get<std::vector<size_t>>("enabled_destinations", std::vector<size_t>());
		if (enabled_dests.size() == 0) {
			mf::LogInfo("DataReceiverManager") << "enabled_destinations not specified, assuming all destinations enabled.";
			for (auto& d : destinations_) {
				enabled_destinations_.insert(d.first);
			}
		}
		else {
			for (auto& d : enabled_dests) {
				enabled_destinations_.insert(d);
			}
		}
	}
}

artdaq::DataSenderManager::~DataSenderManager()
{
	for (auto& dest : enabled_destinations_) {
		sendEODFrag(dest, sent_frag_count_.slotCount(dest));
	}
	mf::LogDebug("DataSenderManager") << "Shutting down DataSenderManager. Sent " << count() << " fragments.";
}

int artdaq::DataSenderManager::calcDest(Fragment::sequence_id_t sequence_id) const
{
	if (enabled_destinations_.size() == 0) return TransferInterface::RECV_TIMEOUT; // No destinations configured.
	auto index = sequence_id % enabled_destinations_.size();
	auto it = enabled_destinations_.begin();
	for (; index > 0; --index) {
		++it;
		if (it == enabled_destinations_.end()) it = enabled_destinations_.begin();
	}
	return *it;
}

void
artdaq::DataSenderManager::
sendEODFrag(int dest, size_t nFragments)
{
	if (destinations_.count(dest)) {
		destinations_[dest]->moveFragment(std::move(*Fragment::eodFrag(nFragments)));
		//  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
	}
}

int
artdaq::DataSenderManager::
sendFragment(Fragment && frag)
{
	// Precondition: Fragment must be complete and consistent (including
	// header information).
	auto start_time = std::chrono::steady_clock::now();
	if (frag.type() == Fragment::EndOfDataFragmentType) {
		throw cet::exception("LogicError")
			<< "EOD fragments should not be sent on as received: "
			<< "use sendEODFrag() instead.";
	}
	size_t seqID = frag.sequenceID();
	size_t fragSize = frag.sizeBytes();
	TRACE(13, "sendFragment start frag.fragmentHeader()=%p", (void*)(frag.headerBegin()));
	int dest = 0;
	if (broadcast_sends_) {
		for (auto& bdest : enabled_destinations_) {
		  TRACE(5,"DataSenderManager::sendFragment: Sending fragment with seqId %zu to destination %d (broadcast)",seqID,bdest);
			// Gross, we have to copy.
			Fragment fragCopy(frag);
			auto sts = destinations_[bdest]->copyFragment(fragCopy); 
			while (sts == TransferInterface::CopyStatus::kTimeout) {
				sts = destinations_[bdest]->copyFragment(fragCopy);
			}
			sent_frag_count_.incSlot(bdest);
		}
	}
	else {
		dest = calcDest(seqID);
		if (destinations_.count(dest) && enabled_destinations_.count(dest)) {
		  TRACE(5,"DataSenderManager::sendFragment: Sending fragment with seqId %zu to destination %d",seqID , dest);
			destinations_[dest]->moveFragment(std::move(frag));
			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
		}
	}
	if (metricMan) {
		auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
		metricMan->sendMetric("Data Send Time to Rank " + std::to_string(dest), delta_t, "s", 1);
		metricMan->sendMetric("Data Send Size to Rank " + std::to_string(dest), fragSize, "B", 1);
		metricMan->sendMetric("Data Send Rate to Rank " + std::to_string(dest), fragSize / delta_t, "B/s", 1);
	}
	TRACE(5,"DataSenderManager::sendFragment: Done sending fragment %zu", seqID);
	return dest;
}

