#include "artdaq/DAQrate/SharedMemoryEventManager.hh"

artdaq::SharedMemoryEventManager::SharedMemoryEventManager(fhicl::ParameterSet pset, size_t num_fragments_per_event, run_id_t run,
														   size_t event_queue_depth, std::string art_fhicl)
	: SharedMemoryManager(pset.get<int>("shm_key", 0xBEE7), pset.get<size_t>("event_queue_depth", event_queue_depth), pset.get<size_t>("max_event_size_bytes"))
	, num_fragments_per_event_(pset.get<size_t>("fragment_count", num_fragments_per_event))
	, run_id_(run)
	, subrun_id_(0)
	, seqIDModulus_(1)
	, lastFlushedSeqID_(0)
	, highestSeqIDSeen_(0)
	, incomplete_event_report_interval_ms_(pset.get<int>("incomplete_event_report_interval_ms", -1))
	, last_incomplete_event_report_time_(std::chrono::steady_clock::now())
	, config_file_(std::tmpfile())
	, art_processes_()
	, requests_(pset)
{
	std::fputs(art_fhicl.c_str(), config_file_);

}

void artdaq::SharedMemoryEventManager::AddFragment(detail::RawFragmentHeader frag, void* dataPtr)
{
	auto buffer = getBufferForSequenceID_(frag.sequence_id);

	ResetReadPos(buffer);
	auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buffer));
	hdr->run_id = run_id_;
	hdr->subrun_id = subrun_id_;

	Write(buffer, dataPtr, frag.word_count * sizeof(RawDataType));

	hdr->is_complete = GetFragmentCount(buffer) == num_fragments_per_event_;

	if (hdr->is_complete)
	{
		SharedMemoryManager::ReleaseBuffer(buffer);
	}
}

bool artdaq::SharedMemoryEventManager::CheckSpace(Fragment::sequence_id_t seqID)
{
	if (ReadyForWrite(false)) return true;

	auto buffer = getBufferForSequenceID_(seqID);

	return buffer != -1;
}

size_t artdaq::SharedMemoryEventManager::GetOpenEventCount()
{
	return GetBuffersOwnedByManager().size();
}

size_t artdaq::SharedMemoryEventManager::GetFragmentCount(int buffer, Fragment::type_t type)
{
	ResetReadPos(buffer);
	IncrementReadPos(buffer, sizeof(detail::RawEventHeader));

	size_t count = 0;

	while (MoreDataInBuffer(buffer))
	{
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(buffer));
		IncrementReadPos(buffer, fragHdr->word_count * sizeof(RawDataType));
		if (type != Fragment::InvalidFragmentType && fragHdr->type != type) continue;
		++count;
	}

	return count;
}

int artdaq::SharedMemoryEventManager::getBufferForSequenceID_(Fragment::sequence_id_t seqID)
{
	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		ResetReadPos(buf);
		auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buf));
		if (hdr->sequence_id == seqID) return buf;
	}
	auto new_buffer = GetBufferForWriting(false);
	auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetNextWritePos(new_buffer));
	hdr->is_complete = false;
	hdr->run_id = 0;
	hdr->subrun_id = 0;
	hdr->sequence_id = seqID;
	return new_buffer;
}