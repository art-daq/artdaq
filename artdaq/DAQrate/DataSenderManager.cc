#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataSenderManager::DataSenderManager(fhicl::ParameterSet pset)
  : destinations_()
  , sent_frag_count_(0)
  , broadcast_sends_(pset.get<bool>("broadcast_sends",false))
{
  mf::LogDebug("DataSenderManager") << "Received pset: " << pset.to_string();
  auto dests = pset.get<fhicl::ParameterSet>("destinations");
  for(auto& d : dests.get_pset_names()) {
	try { 
	  auto dd = std::stoi(d);
      destinations_.emplace(dd, MakeTransferPlugin(dests.get<fhicl::ParameterSet>(d), d, TransferInterface::Role::kSend));
	}
	catch(std::invalid_argument) {
	  TRACE(3, "Invalid destination specification: " + d);
	}
  }
}

artdaq::DataSenderManager::~DataSenderManager()
{
  for (auto& dest : destinations_) {
    sendEODFrag(dest.first, sent_frag_count_.slotCount(dest.first));
  }
}

size_t artdaq::DataSenderManager::calcDest(Fragment::sequence_id_t sequence_id) const
{
  // Works if dest_count_ == 1
  auto index = sequence_id % destinations_.size();
  auto it = destinations_.begin();
  for(; index > 0; --index) {
	++it;
	if(it == destinations_.end()) it = destinations_.begin();
  }
  return (*it).first;
}

void
artdaq::DataSenderManager::
sendEODFrag(size_t dest, size_t nFragments)
{
  destinations_[dest]->copyFragmentTo(*Fragment::eodFrag(nFragments));
  //  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
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
  TRACE( 13, "sendFragment start frag.fragmentHeader()=%p", (void*)(frag.headerBegin()) );
  size_t dest = 0;
  if (broadcast_sends_) {
    for (auto& bdest : destinations_) {
      // Gross, we have to copy.
      Fragment fragCopy(frag);
	  bdest.second->copyFragmentTo(fragCopy);
      sent_frag_count_.incSlot(bdest.first);
    }
  } else {
    dest = calcDest(frag.sequenceID());
	destinations_[dest]->copyFragmentTo(frag);
    //sendFragTo(std::move(frag), dest);
    sent_frag_count_.incSlot(dest);
  }
  TRACE( 13, "sendFragment end frag.fragmentHeader()=%p", (void*)(frag.headerBegin()) );
  return dest;
}

