#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

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

size_t artdaq::DataSenderManager::calcDest(Fragment::sequence_id_t sequence_id) const
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
sendEODFrag(size_t dest, size_t nFragments)
{
	if (destinations_.count(dest)) {
		destinations_[dest]->moveFragment(std::move(*Fragment::eodFrag(nFragments)));
		//  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
	}
}

size_t
artdaq::DataSenderManager::
sendFragment(Fragment && frag)
{
	// Precondition: Fragment must be complete and consistent (including
	// header information).
	if (frag.type() == Fragment::EndOfDataFragmentType) {
		throw cet::exception("LogicError")
			<< "EOD fragments should not be sent on as received: "
			<< "use sendEODFrag() instead.";
	}
	size_t seqID = frag.sequenceID();
	TRACE(13, "sendFragment start frag.fragmentHeader()=%p", (void*)(frag.headerBegin()));
	size_t dest = 0;
	if (broadcast_sends_) {
		for (auto& bdest : enabled_destinations_) {
			mf::LogDebug("DataSenderManager") << "Sending fragment with seqId " << seqID << " to destination " << bdest << " (broadcast)";
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
			mf::LogDebug("DataSenderManager") << "Sending fragment with seqId " << seqID << " to destination " << dest;
			destinations_[dest]->moveFragment(std::move(frag));
			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
		}
	}
	mf::LogDebug("DataSenderManager") << "Done sending fragment " << seqID;
	return dest;
}

