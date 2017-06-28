#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include <iomanip>
#include <fstream>

artdaq::SharedMemoryEventManager::SharedMemoryEventManager(fhicl::ParameterSet pset, std::string art_fhicl)
	: SharedMemoryManager(pset.get<int>("shm_key", 0xBEE7),
						  pset.get<size_t>("event_queue_depth", 40),
						  pset.get<size_t>("max_event_size_bytes"),
						  pset.get<size_t>("stale_buffer_touch_count", 0x10000))
	, num_art_processes_(pset.get<size_t>("art_analyzer_count", 1))
	, num_fragments_per_event_(pset.get<size_t>("fragment_count"))
	, queue_size_(pset.get<size_t>("event_queue_depth", 40))
	, run_id_(0)
	, subrun_id_(0)
	, update_run_ids_(pset.get<bool>("update_run_ids_on_new_fragment", true))
	, overwrite_mode_(false)
	, buffer_writes_pending_()
	, buffer_write_mutexes_()
	, seqIDModulus_(1)
	, lastFlushedSeqID_(0)
	, highestSeqIDSeen_(0)
	, incomplete_event_report_interval_ms_(pset.get<int>("incomplete_event_report_interval_ms", -1))
	, last_incomplete_event_report_time_(std::chrono::steady_clock::now())
	, config_file_name_(std::tmpnam(nullptr))
	, art_processes_()
	, requests_(pset)
{
	std::ofstream of(config_file_name_);
	of << art_fhicl << std::endl;
	of.close();

	for (size_t ii = 0; ii < size(); ++ii)
	{
		buffer_writes_pending_[ii] = 0;
	}
	requests_.SendRoutingToken(size());
}

artdaq::SharedMemoryEventManager::~SharedMemoryEventManager()
{
	std::vector<int> ignored;
	endOfData(ignored);
}

bool artdaq::SharedMemoryEventManager::AddFragment(detail::RawFragmentHeader frag, void* dataPtr)
{
	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);
	if (buffer == -1) return false;

	ResetReadPos(buffer);
	auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buffer));
	if (update_run_ids_) {
		hdr->run_id = run_id_;
		hdr->subrun_id = subrun_id_;
	}

	{
		std::unique_lock<std::mutex>(buffer_write_mutexes_[buffer]);
		Write(buffer, dataPtr, frag.word_count * sizeof(RawDataType));
	}
	hdr->is_complete = GetFragmentCount(buffer) == num_fragments_per_event_ && buffer_writes_pending_[buffer] == 0;


	if (hdr->is_complete)
	{
		MarkBufferFull(buffer);
		requests_.RemoveRequest(frag.sequence_id);
		requests_.SendRoutingToken(1);
	}

	return true;
}

bool artdaq::SharedMemoryEventManager::AddFragment(FragmentPtr frag)
{
	auto hdr = *reinterpret_cast<detail::RawFragmentHeader*>(frag->headerAddress());
	auto data = frag->headerAddress() + hdr.num_words();

	return AddFragment(hdr, data);
}

artdaq::RawDataType* artdaq::SharedMemoryEventManager::GetFragmentLocation(detail::RawFragmentHeader frag)
{
	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);

	if (buffer == -1) return nullptr;

	buffer_writes_pending_[buffer]++;
	std::unique_lock<std::mutex>(buffer_write_mutexes_[buffer]);
	Write(buffer, &frag, frag.num_words() * sizeof(RawDataType));

	auto pos = reinterpret_cast<RawDataType*>(GetWritePos(buffer));
	IncrementWritePos(buffer, (frag.word_count - frag.num_words()) * sizeof(RawDataType));

	return pos;

}

void artdaq::SharedMemoryEventManager::DoneWritingFragment(detail::RawFragmentHeader frag)
{

	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);
	if (buffer == -1) throw cet::exception("SharedMemoryEventManager") << "getBufferForSequenceID_ returned -1 when it REALLY shouldn't have! Check program logic!";
	buffer_writes_pending_[buffer]--;
	ResetReadPos(buffer);
	auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buffer));
	if (update_run_ids_) {
		hdr->run_id = run_id_;
		hdr->subrun_id = subrun_id_;
	}
	hdr->is_complete = GetFragmentCount(buffer) == num_fragments_per_event_ && buffer_writes_pending_[buffer] == 0;


	if (hdr->is_complete)
	{
		MarkBufferFull(buffer);
		requests_.RemoveRequest(frag.sequence_id);
		requests_.SendRoutingToken(1);
	}
}

bool artdaq::SharedMemoryEventManager::CheckSpace(detail::RawFragmentHeader frag)
{
	if (ReadyForWrite(false)) return true;

	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);

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

void artdaq::SharedMemoryEventManager::RunArt()
{
	while (restart_art_)
	{
		art_process_return_codes_.push_back(system(("art -c " + config_file_name_).c_str()));
	}
}

void artdaq::SharedMemoryEventManager::StartArt()
{
	restart_art_ = true;
	for (size_t ii = 0; ii < num_art_processes_; ++ii)
	{
		art_processes_.emplace_back([=] {RunArt(); });
	}
}

void artdaq::SharedMemoryEventManager::ReconfigureArt(std::string art_fhicl, int n_art_processes, run_id_t newRun)
{
	std::vector<int> ignored;
	endOfData(ignored);
	if (newRun == 0) newRun = run_id_ + 1;
	run_id_ = newRun;
	std::ofstream of(config_file_name_, std::ofstream::trunc);
	of << art_fhicl << std::endl;
	of.close();
	if (n_art_processes != -1)
	{
		num_art_processes_ = n_art_processes;
	}
	StartArt();
}

bool artdaq::SharedMemoryEventManager::endOfData(std::vector<int>& readerReturnValues)
{
	restart_art_ = false;
	TLOG_DEBUG("SharedMemoryEventManager") << "SharedMemoryEventManager::endOfData" << TLOG_ENDL;
	broadcastFragment_(std::move(Fragment::eodFrag(0)));

	TRACE(4, "SharedMemoryEventManager::endOfData: Getting return codes from art processes");

	for (auto& proc : art_processes_)
	{
		if (proc.joinable()) proc.join();
	}
	readerReturnValues = art_process_return_codes_;

	return true;
}

void artdaq::SharedMemoryEventManager::setSeqIDModulus(unsigned int seqIDModulus)
{
	seqIDModulus_ = seqIDModulus;
}

bool artdaq::SharedMemoryEventManager::flushData()
{
	size_t initialStoreSize = GetOpenEventCount();
	TLOG_DEBUG("SharedMemoryEventManager") << "Flushing " << initialStoreSize
		<< " stale events from the SharedMemoryEventManager." << TLOG_ENDL;
	std::vector<sequence_id_t> flushList;
	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		MarkBufferFull(buf);
	}
	TLOG_DEBUG("SharedMemoryEventManager") << "Done flushing " << flushList.size()
		<< " stale events from the SharedMemoryEventManager." << TLOG_ENDL;

	lastFlushedSeqID_ = highestSeqIDSeen_;
	return true;
}

void artdaq::SharedMemoryEventManager::startRun(run_id_t runID)
{
	StartArt();
	run_id_ = runID;
	subrun_id_ = 1;
	lastFlushedSeqID_ = 0;
	highestSeqIDSeen_ = 0;
	requests_.SendRoutingToken(queue_size_);
	TLOG_DEBUG("SharedMemoryEventManager") << "Starting run " << run_id_
		<< ", max queue size = "
		<< queue_size_
		<< ", queue size = "
		<< GetOpenEventCount() << TLOG_ENDL;
	if (metricMan)
	{
		double runSubrun = run_id_ + ((double)subrun_id_ / 10000);
		metricMan->sendMetric("Run Number", runSubrun, "Run:Subrun", 1, false);
	}
}

void artdaq::SharedMemoryEventManager::startSubrun()
{
	++subrun_id_;
	if (metricMan)
	{
		double runSubrun = run_id_ + ((double)subrun_id_ / 10000);
		metricMan->sendMetric("Run Number", runSubrun, "Run:Subrun", 1, false);
	}
}

bool artdaq::SharedMemoryEventManager::endRun()
{
	FragmentPtr	endOfRunFrag(new
							 Fragment(static_cast<size_t>
							 (ceil(sizeof(my_rank) /
								   static_cast<double>(sizeof(Fragment::value_type))))));

	endOfRunFrag->setSystemType(Fragment::EndOfRunFragmentType);
	*endOfRunFrag->dataBegin() = my_rank;
	broadcastFragment_(std::move(endOfRunFrag));

	return true;
}

bool artdaq::SharedMemoryEventManager::endSubrun()
{
	std::unique_ptr<artdaq::Fragment>
		endOfSubrunFrag(new
						Fragment(static_cast<size_t>
						(ceil(sizeof(my_rank) /
							  static_cast<double>(sizeof(Fragment::value_type))))));

	endOfSubrunFrag->setSystemType(Fragment::EndOfSubrunFragmentType);
	*endOfSubrunFrag->dataBegin() = my_rank;

	broadcastFragment_(std::move(endOfSubrunFrag));
	return true;
}

void artdaq::SharedMemoryEventManager::sendMetrics()
{
	auto events = GetBuffersOwnedByManager();
	if (metricMan)
	{
		metricMan->sendMetric("Incomplete Event Count", events.size(), "events", 1);
	}
	if (incomplete_event_report_interval_ms_ > 0 && GetOpenEventCount())
	{
		if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_incomplete_event_report_time_).count() < incomplete_event_report_interval_ms_) return;
		last_incomplete_event_report_time_ = std::chrono::steady_clock::now();
		std::ostringstream oss;
		oss << "Incomplete Events (" << num_fragments_per_event_ << "): ";
		for (auto& ev : events)
		{
			ResetReadPos(ev);
			auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetReadPos(ev));
			oss << hdr->sequence_id << " (" << GetFragmentCount(ev) << "), ";
		}
		TLOG_DEBUG("SharedMemoryEventManager") << oss.str() << TLOG_ENDL;
	}
}

void artdaq::SharedMemoryEventManager::broadcastFragment_(FragmentPtr frag)
{
	auto hdr = *reinterpret_cast<detail::RawFragmentHeader*>(frag->headerAddress());

	for (auto ii = 0; ii < GetMaxId(); ++ii)
	{
		if (ii == GetMyId()) continue;
		hdr.sequence_id = 0xFFFFFFFFFF00 + 1 + ii;
		auto buffer = getBufferForSequenceID_(hdr.sequence_id);
		while (buffer == -1)
		{
			usleep(10000);
			buffer = getBufferForSequenceID_(hdr.sequence_id);
		}
		AddFragment(hdr, frag->headerAddress());
		MarkBufferFull(buffer, ii);
	}
}

int artdaq::SharedMemoryEventManager::getBufferForSequenceID_(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp)
{
	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		ResetReadPos(buf);
		auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buf));
		if (hdr->sequence_id == seqID) return buf;
	}
	auto new_buffer = GetBufferForWriting(overwrite_mode_);
	if (new_buffer == -1) return -1;
	auto hdr = reinterpret_cast<detail::RawEventHeader*>(GetWritePos(new_buffer));
	hdr->is_complete = false;
	hdr->run_id = run_id_;
	hdr->subrun_id = subrun_id_;
	hdr->sequence_id = seqID;
	buffer_writes_pending_[new_buffer] = 0;
	if (timestamp != Fragment::InvalidTimestamp) {
		requests_.AddRequest(seqID, timestamp);
	}
	requests_.SendRequest();
	return new_buffer;
}
