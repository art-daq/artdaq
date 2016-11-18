#include "artdaq/DAQrate/DataTransferManager.hh"
#include "trace.h"

artdaq::DataTransferManager::DataTransferManager(fhicl::ParameterSet pset)
  : destinations_()
  , sources_()
  , recv_frag_count_(0)
  , sent_frag_count_(0)
  , broadcast_sends_(pset.get<bool>("broadcast_sends",false))
{

  // recv_frag_count_ = detail::FragCounter(sources_.size());
  // sent_frag_count_ = detail::FragCounter(destinations_.size());
}

artdaq::DataTransferManager::~DataTransferManager()
{
  for (auto& dest : destinations_) {
    sendEODFrag(dest.first, sent_frag_count_.slotCount(dest.first));
  }
}

size_t artdaq::DataTransferManager::calcDest(Fragment::sequence_id_t sequence_id) const
{
  // Works if dest_count_ == 1
  return sequence_id % destinations_.size();
}

void
artdaq::DataTransferManager::
sendEODFrag(size_t dest, size_t nFragments)
{
  destinations_[dest]->copyFragmentTo(*Fragment::eodFrag(nFragments));
  //  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
}

size_t
artdaq::DataTransferManager::
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
  size_t dest;
  if (broadcast_sends_) {
    for (auto& dest : destinations_) {
      // Gross, we have to copy.
      Fragment fragCopy(frag);
	  dest.second->copyFragmentTo(fragCopy);
      sent_frag_count_.incSlot(dest.first);
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

size_t artdaq::DataTransferManager::recvFragment( Fragment& frag, size_t timeout_usec)
{
  TRACE( 6,"recvFragment entered tmo=%lu us, frag.sizeofdata=%zu",timeout_usec, frag.size()  );
  return 0;
}
