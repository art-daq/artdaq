
#include "artdaq/DAQrate/SHandles.hh"
#include "artdaq/DAQdata/Debug.hh"
#include "artdaq/DAQrate/MPITag.hh"
#include "artdaq/DAQrate/Utils.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "cetlib/exception.h"

#define TRACE_NAME "SHandles"
#include "trace.h"		// TRACE

#include <algorithm>

artdaq::SHandles::SHandles(size_t buffer_count,
                           uint64_t max_payload_size,
                           size_t dest_count,
                           size_t dest_start,
			   bool broadcast_sends,
                           bool synchronous_sends)
  :
  buffer_count_(buffer_count),
  max_payload_size_(max_payload_size),
  dest_count_(dest_count),
  dest_start_(dest_start),
  pos_(),
  sent_frag_count_(dest_count, dest_start),
  broadcast_sends_(broadcast_sends),
  synchronous_sends_(synchronous_sends),
  reqs_(buffer_count_, MPI_REQUEST_NULL),
  payload_(buffer_count_),
  my_mpi_rank_([](){ auto rank=0; MPI_Comm_rank(MPI_COMM_WORLD, &rank);return rank;}())
{
    std::ostringstream debugstream;
    debugstream << "SHandles construction: "
                << "rank " << my_mpi_rank_ << ", "
		<< buffer_count << " buffers, "
		<< dest_count << " destination starting at rank "
		<< dest_start << '\n';
    TRACE(4, debugstream.str().c_str());
}

artdaq::SHandles::~SHandles()
{
  size_t dest_end = dest_start_ + dest_count_;
  for (size_t dest = dest_start_; dest != dest_end; ++dest) {
    sendEODFrag(dest, sent_frag_count_.slotCount(dest));
  }
  waitAll();
}

size_t artdaq::SHandles::calcDest(Fragment::sequence_id_t sequence_id) const
{
  // Works if dest_count_ == 1
  return sequence_id % dest_count_ + dest_start_;
}

size_t artdaq::SHandles::findAvailable()
{
  size_t use_me = 0;
  int flag;
  do {
    use_me = pos_;
    MPI_Test(&reqs_[use_me], &flag, MPI_STATUS_IGNORE);
    pos_ = (pos_ + 1) % buffer_count_;
  }
  while (!flag);
  // pos_ is pointing at the next slot to check
  // use_me is pointing at the slot to use
  return use_me;
}

size_t
artdaq::SHandles::
sendFragment(Fragment && frag)
{
  // Precondition: Fragment must be complete and consistent (including
  // header information).
  if (frag.type() == Fragment::EndOfDataFragmentType) {
    throw cet::exception("LogicError")
        << "EOD fragments should not be sent on as received: "
        << "use sendEODFrag() instead.";
  }
  size_t dest;
  if (broadcast_sends_) {
    size_t dest_end = dest_start_ + dest_count_;
    for (dest = dest_start_; dest != dest_end; ++dest) {
      // Gross, we have to copy.
      Fragment fragCopy(frag);
      sendFragTo(std::move(fragCopy), dest);
      sent_frag_count_.incSlot(dest);
    }
  } else {
    dest = calcDest(frag.sequenceID());
    sendFragTo(std::move(frag), dest);
    sent_frag_count_.incSlot(dest);
  }

  return dest;
}

void
artdaq::SHandles::
sendEODFrag(size_t dest, size_t nFragments)
{
# if 0
  int my_rank;
  MPI_Comm_rank( MPI_COMM_WORLD, &my_rank );
  std::unique_ptr<Fragment> eod=Fragment::eodFrag(nFragments);
  (*eod).setFragmentID( my_rank );
  sendFragTo(std::move(*eod), dest);
# else
  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest);
# endif
}

void artdaq::SHandles::waitAll()
{
  MPI_Waitall(buffer_count_, &reqs_[0], MPI_STATUSES_IGNORE);
}

void
artdaq::SHandles::
sendFragTo(Fragment && frag, size_t dest)
{
  if (frag.dataSize() > max_payload_size_) {
    throw cet::exception("Unimplemented")
        << "Currently unable to deal with overlarge fragment payload ("
        << frag.dataSize()
        << " words > "
        << max_payload_size_
        << ").";
  }
  size_t buffer_idx = findAvailable();
  Fragment & curfrag = payload_[buffer_idx];
  curfrag = std::move(frag);
  TRACE( 5, "sendFragTo before send src=%i dest=%lu seqID=%lu", my_mpi_rank_ , dest, curfrag.sequenceID() );
  if (! synchronous_sends_) {
    // 14-Sep-2015, KAB: we should consider MPI_Issend here (see below)...
    MPI_Isend(&*curfrag.headerBegin(),
              curfrag.size() * sizeof(Fragment::value_type),
              MPI_BYTE,
              dest,
              MPITag::FINAL,
              MPI_COMM_WORLD,
              &reqs_[buffer_idx]);
  }
  else {
    // 14-Sep-2015, KAB: switched from MPI_Send to MPI_Ssend based on
    // http://www.mcs.anl.gov/research/projects/mpi/sendmode.html.
    // This change was made after we noticed that MPI buffering
    // downstream of NetMonOutput was causing EventBuilder memory
    // usage to grow when using MPI_Send with MPICH 3.1.4 and 3.1.2a.
    MPI_Ssend(&*curfrag.headerBegin(),
              curfrag.size() * sizeof(Fragment::value_type),
              MPI_BYTE,
              dest,
              MPITag::FINAL,
              MPI_COMM_WORLD );
  }
  TRACE( 5, "sendFragTo COMPLETE" );
  
  {
    std::ostringstream debugstream;
    debugstream << "send COMPLETE: "
		<< " buffer_idx=" << buffer_idx
		<< " send_size=" << curfrag.size()
		<< " src=" << my_mpi_rank_
		<< " dest=" << dest
		<< " sequenceID=" << curfrag.sequenceID()
		<< " fragID=" << curfrag.fragmentID()
		<< '\n';
    TRACE(11, debugstream.str().c_str());
  }
}
