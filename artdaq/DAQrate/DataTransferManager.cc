
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
  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
# endif
}
