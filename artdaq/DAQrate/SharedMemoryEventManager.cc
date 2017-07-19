#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include <iomanip>
#include <fstream>

artdaq::SharedMemoryEventManager::SharedMemoryEventManager(fhicl::ParameterSet pset, fhicl::ParameterSet art_pset)
	: SharedMemoryManager(pset.get<uint32_t>("shared_memory_key", seedAndRandom()),
						  pset.get<size_t>("buffer_count"),
						  pset.has_key("max_event_size_bytes") ? pset.get<size_t>("max_event_size_bytes") : pset.get<size_t>("expected_fragments_per_event") * pset.get<size_t>("max_fragment_size_bytes"),
						  pset.get<size_t>("stale_buffer_timeout_usec", 1000000))
	, num_art_processes_(pset.get<size_t>("art_analyzer_count", 1))
	, num_fragments_per_event_(pset.get<size_t>("expected_fragments_per_event"))
	, queue_size_(pset.get<size_t>("buffer_count"))
	, run_id_(0)
	, subrun_id_(0)
	, update_run_ids_(pset.get<bool>("update_run_ids_on_new_fragment", true))
	, overwrite_mode_(!pset.get<bool>("use_art", true))
	, buffer_writes_pending_()
	, buffer_write_mutexes_()
	, incomplete_event_report_interval_ms_(pset.get<int>("incomplete_event_report_interval_ms", -1))
	, last_incomplete_event_report_time_(std::chrono::steady_clock::now())
	, broadcast_timeout_ms_(pset.get<int>("fragment_broadcast_timeout_ms", 3000))
	, config_file_name_(std::tmpnam(nullptr))
	, art_processes_()
	, restart_art_(false)
	, requests_(pset)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "BEGIN CONSTRUCTOR" << TLOG_ENDL;
	std::ofstream of(config_file_name_);

	configureArt_(art_pset);

	if (pset.get<bool>("use_art", true) == false) num_art_processes_ = 0;

	for (size_t ii = 0; ii < size(); ++ii)
	{
		buffer_writes_pending_[ii] = 0;
	}
	requests_.SendRoutingToken(size());

	if (!IsValid()) throw cet::exception("SharedMemoryEventManager") << "Unable to attach to Shared Memory!";
	SetRank(my_rank);
	TLOG_DEBUG("SharedMemoryEventManager") << "END CONSTRUCTOR" << TLOG_ENDL;
}

artdaq::SharedMemoryEventManager::~SharedMemoryEventManager()
{
	TLOG_DEBUG("SharedMemoryEventManager") << "DESTRUCTOR" << TLOG_ENDL;
	std::vector<int> ignored;
	endOfData(ignored);
	remove(config_file_name_.c_str());
}

bool artdaq::SharedMemoryEventManager::AddFragment(detail::RawFragmentHeader frag, void* dataPtr, bool skipCheck)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "AddFragment(Header, ptr) BEGIN frag.word_count=" << std::to_string(frag.word_count)
		<< ", sequence_id=" << std::to_string(frag.sequence_id) << TLOG_ENDL;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);
	TLOG_DEBUG("SharedMemoryEventManager") << "Using buffer " << std::to_string(buffer) << TLOG_ENDL;
	if (buffer == -1) return false;

	std::unique_lock<std::mutex> lk(buffer_write_mutexes_[buffer]);
	auto hdr = getEventHeader_(buffer);
	if (update_run_ids_) {
		hdr->run_id = run_id_;
		hdr->subrun_id = subrun_id_;
	}

	TLOG_DEBUG("SharedMemoryEventManager") << "AddFragment before Write calls" << TLOG_ENDL;
	Write(buffer, dataPtr, frag.word_count * sizeof(RawDataType));

	if (!skipCheck) {
		TLOG_DEBUG("SharedMemoryEventManager") << "Checking for complete event" << TLOG_ENDL;
		auto fragmentCount = GetFragmentCount(buffer);
		hdr->is_complete = fragmentCount == num_fragments_per_event_ && buffer_writes_pending_[buffer] == 0;
		TLOG_DEBUG("SharedMemoryEventManager") << "hdr->is_complete=" << std::boolalpha << hdr->is_complete
			<< ", fragmentCount=" << std::to_string(fragmentCount)
			<< ", num_fragments_per_event=" << std::to_string(num_fragments_per_event_)
			<< ", buffer_writes_pending_[buffer]=" << std::to_string(buffer_writes_pending_[buffer]) << TLOG_ENDL;

		if (hdr->is_complete)
		{
			TLOG_DEBUG("SharedMemoryEventManager") << "Closing event" << TLOG_ENDL;
			MarkBufferFull(buffer);
			requests_.RemoveRequest(frag.sequence_id);
			requests_.SendRoutingToken(1);
		}
	}

	TLOG_DEBUG("SharedMemoryEventManager") << "AddFragment END" << TLOG_ENDL;
	return true;
}

bool artdaq::SharedMemoryEventManager::AddFragment(FragmentPtr frag, int64_t timeout_usec, FragmentPtr& outfrag)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "AddFragment(FragmentPtr) BEGIN" << TLOG_ENDL;
	auto hdr = *reinterpret_cast<detail::RawFragmentHeader*>(frag->headerAddress());
	auto data = frag->headerAddress();
	auto start = std::chrono::steady_clock::now();
	bool sts = false;
	while (!sts && std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count() < timeout_usec)
	{
		sts = AddFragment(hdr, data);
		if (!sts) usleep(1000);
	}
	if (!sts)
	{
		outfrag = std::move(frag);
	}
	return sts;
}

artdaq::RawDataType* artdaq::SharedMemoryEventManager::WriteFragmentHeader(detail::RawFragmentHeader frag)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "WriteFragmentHeader BEGIN" << TLOG_ENDL;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);

	if (buffer == -1) return nullptr;

	buffer_writes_pending_[buffer]++;
	std::unique_lock<std::mutex> lk(buffer_write_mutexes_[buffer]);
	Write(buffer, &frag, frag.num_words() * sizeof(RawDataType));

	auto pos = reinterpret_cast<RawDataType*>(GetWritePos(buffer));
	IncrementWritePos(buffer, (frag.word_count - frag.num_words()) * sizeof(RawDataType));

	TLOG_DEBUG("SharedMemoryEventManager") << "WriteFragmentHeader END" << TLOG_ENDL;
	return pos;

}

void artdaq::SharedMemoryEventManager::DoneWritingFragment(detail::RawFragmentHeader frag)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "DoneWritingFragment BEGIN" << TLOG_ENDL;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);
	if (buffer == -1) throw cet::exception("SharedMemoryEventManager") << "getBufferForSequenceID_ returned -1 when it REALLY shouldn't have! Check program logic!";
	buffer_writes_pending_[buffer]--;
	std::unique_lock<std::mutex> lk(buffer_write_mutexes_[buffer]);
	auto hdr = getEventHeader_(buffer);
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
	TLOG_DEBUG("SharedMemoryEventManager") << "DoneWritingFragment END" << TLOG_ENDL;
}

//bool artdaq::SharedMemoryEventManager::CheckSpace(detail::RawFragmentHeader frag)
//{
//	if (ReadyForWrite(false)) return true;
//
//	auto buffer = getBufferForSequenceID_(frag.sequence_id, frag.timestamp);
//
//	return buffer != -1;
//}

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
		TLOG_DEBUG("GetFragmentCount") << "Adding Fragment with size=" << std::to_string(fragHdr->word_count) << " to Fragment count" << TLOG_ENDL;
		++count;
	}

	return count;
}

void artdaq::SharedMemoryEventManager::RunArt()
{
	while (restart_art_)
	{
		TLOG_INFO("SharedMemoryEventManager") << "Starting art process with config file " << config_file_name_ << TLOG_ENDL;
		art_process_return_codes_.push_back(system(("art -c " + config_file_name_).c_str()));
	}
}

void artdaq::SharedMemoryEventManager::StartArt()
{
	restart_art_ = true;
	auto initialCount = GetAttachedCount();
	auto startTime = std::chrono::steady_clock::now();
	for (size_t ii = 0; ii < num_art_processes_; ++ii)
	{
		art_processes_.emplace_back([=] {RunArt(); });
	}

	while (static_cast<uint16_t>(GetAttachedCount() - initialCount) < num_art_processes_ &&
		   std::chrono::duration_cast<TimeUtils::seconds>(std::chrono::steady_clock::now() - startTime).count() < 5)
	{
		usleep(1000);
	}
	if (static_cast<uint16_t>(GetAttachedCount() - initialCount) < num_art_processes_)
	{
		TLOG_WARNING("SharedMemoryEventManager") << std::to_string(GetAttachedCount() - initialCount - num_art_processes_)
			<< " art processes have not started after 5s. Check art configuration!" << TLOG_ENDL;
	}
	else
	{
		TLOG_INFO("SharedMemoryEventManager") << std::setw(4) << std::fixed << "art initialization took "
			<< std::chrono::duration_cast<TimeUtils::seconds>(std::chrono::steady_clock::now() - startTime).count() << "seconds." << TLOG_ENDL;
	}

}

void artdaq::SharedMemoryEventManager::ReconfigureArt(fhicl::ParameterSet art_pset, run_id_t newRun, int n_art_processes)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "ReconfigureArt BEGIN" << TLOG_ENDL;
	if (restart_art_) // Art is running
	{
		std::vector<int> ignored;
		endOfData(ignored);
	}
	if (newRun == 0) newRun = run_id_ + 1;
	std::ofstream of(config_file_name_, std::ofstream::trunc);
	configureArt_(art_pset);
	if (n_art_processes != -1)
	{
		TLOG_INFO("SharedMemoryEventManager") << "Setting number of art processes to " << n_art_processes << TLOG_ENDL;
		num_art_processes_ = n_art_processes;
	}
	startRun(newRun);
	TLOG_DEBUG("SharedMemoryEventManager") << "ReconfigureArt END" << TLOG_ENDL;
}

bool artdaq::SharedMemoryEventManager::endOfData(std::vector<int>& readerReturnValues)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "SharedMemoryEventManager::endOfData" << TLOG_ENDL;
	restart_art_ = false;

	size_t initialStoreSize = GetOpenEventCount();
	TLOG_DEBUG("SharedMemoryEventManager") << "endOfData: Flushing " << initialStoreSize
		<< " stale events from the SharedMemoryEventManager." << TLOG_ENDL;
	std::vector<sequence_id_t> flushList;
	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		MarkBufferFull(buf);
	}
	TLOG_DEBUG("SharedMemoryEventManager") << "endOfData: Done flushing " << flushList.size()
		<< " stale events from the SharedMemoryEventManager." << TLOG_ENDL;


	TLOG_INFO("SharedMemoryEventManager") << "Waiting for outstanding buffers..." << TLOG_ENDL;
	auto start = std::chrono::steady_clock::now();
	auto lastReadCount = ReadReadyCount();

	// We will wait until no buffer has been read for 1 second.
	while (lastReadCount > 0 && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() < 1000)
	{
		auto temp = ReadReadyCount();
		if (temp != lastReadCount)
		{
			lastReadCount = temp;
			start = std::chrono::steady_clock::now();
		}
		if (lastReadCount > 0) usleep(1000);
	}

	broadcastFragment_(std::move(Fragment::eodFrag(0)));

	TLOG_ARB(4, "SharedMemoryEventManager") << "endOfData: Getting return codes from art processes" << TLOG_ENDL;

	for (auto& proc : art_processes_)
	{
		if (proc.joinable()) proc.join();
	}
	readerReturnValues = art_process_return_codes_;
	if (readerReturnValues.size() == 0) readerReturnValues.push_back(0);

	ResetAttachedCount();

	TLOG_DEBUG("SharedMemoryEventManager") << "endOfData END" << TLOG_ENDL;
	return true;
}

void artdaq::SharedMemoryEventManager::startRun(run_id_t runID)
{
	StartArt();
	run_id_ = runID;
	subrun_id_ = 1;
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
			auto hdr = getEventHeader_(ev);
			oss << hdr->sequence_id << " (" << GetFragmentCount(ev) << "), ";
		}
		TLOG_DEBUG("SharedMemoryEventManager") << oss.str() << TLOG_ENDL;
	}
}

std::string artdaq::SharedMemoryEventManager::toString()
{
	std::ostringstream ostr;
	ostr << SharedMemoryManager::toString() << std::endl;

	ostr << "Buffer Fragment Counts: " << std::endl;
	for (size_t ii = 0; ii < size(); ++ii)
	{
		ostr << "Buffer " << std::to_string(ii) << ": " << std::to_string(GetFragmentCount(ii)) << std::endl;
	}
	return ostr.str();
}

void artdaq::SharedMemoryEventManager::broadcastFragment_(FragmentPtr frag)
{
	auto hdr = *reinterpret_cast<detail::RawFragmentHeader*>(frag->headerAddress());

	for (auto ii = 0; ii <= GetAttachedCount(); ++ii)
	{
		if (ii == GetMyId()) continue;
		hdr.sequence_id = 0xFFFFFFFFF000 + 1 + ii;
		auto buffer = getBufferForSequenceID_(hdr.sequence_id);
		auto start_time = std::chrono::steady_clock::now();
		while (buffer == -1 && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() < broadcast_timeout_ms_)
		{
			usleep(10000);
			buffer = getBufferForSequenceID_(hdr.sequence_id);
		}
		AddFragment(hdr, frag->headerAddress(), true);
		getEventHeader_(buffer)->is_complete = true;
		MarkBufferFull(buffer, ii);
		requests_.RemoveRequest(hdr.sequence_id);
	}
}

artdaq::detail::RawEventHeader* artdaq::SharedMemoryEventManager::getEventHeader_(int buffer)
{
	return reinterpret_cast<detail::RawEventHeader*>(GetBufferStart(buffer));
}

int artdaq::SharedMemoryEventManager::getBufferForSequenceID_(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp)
{
	TLOG_TRACE("SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " BEGIN" << TLOG_ENDL;
	std::unique_lock<std::mutex> lk(seq_id_buffer_mutex_);
	TLOG_TRACE("SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " AFTER MUTEX" << TLOG_ENDL;
	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		std::unique_lock<std::mutex> lk2(buffer_write_mutexes_[buf]);
		auto hdr = getEventHeader_(buf);
		if (hdr->sequence_id == seqID) {
			TLOG_TRACE("SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " returning " << buf << TLOG_ENDL;
			return buf;
		}
	}
	auto new_buffer = GetBufferForWriting(overwrite_mode_);
	if (new_buffer == -1) return -1;
	std::unique_lock<std::mutex> lk3(buffer_write_mutexes_[new_buffer]);
	auto hdr = getEventHeader_(new_buffer);
	hdr->is_complete = false;
	hdr->run_id = run_id_;
	hdr->subrun_id = subrun_id_;
	hdr->sequence_id = seqID;
	buffer_writes_pending_[new_buffer] = 0;
	if (timestamp != Fragment::InvalidTimestamp) {
		requests_.AddRequest(seqID, timestamp);
	}
	requests_.SendRequest();
	IncrementWritePos(new_buffer, sizeof(detail::RawEventHeader));
	TLOG_TRACE("SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " returning newly initialized buffer " << new_buffer << TLOG_ENDL;
	return new_buffer;
}

void artdaq::SharedMemoryEventManager::configureArt_(fhicl::ParameterSet art_pset)
{
	std::ofstream of(config_file_name_, std::ofstream::trunc);
	of << art_pset.to_string();

	if(art_pset.has_key("services.NetMonTransportServiceInterface"))
	{
		of << " services.NetMonTransportServiceInterface.shared_memory_key: 0x" << std::hex << GetKey();
	}
	of << " source.shared_memory_key: 0x" << std::hex << GetKey();
	of.close();
}