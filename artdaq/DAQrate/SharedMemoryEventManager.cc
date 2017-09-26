#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq-core/Utilities/TraceLock.hh"
#include <sys/wait.h>
#include "SharedMemoryEventManager.hh"

artdaq::SharedMemoryEventManager::SharedMemoryEventManager(fhicl::ParameterSet pset, fhicl::ParameterSet art_pset)
	: SharedMemoryManager(pset.get<uint32_t>("shared_memory_key", 0xBEE70000 + getpid()),
						  pset.get<size_t>("buffer_count"),
						  pset.has_key("max_event_size_bytes") ? pset.get<size_t>("max_event_size_bytes") : pset.get<size_t>("expected_fragments_per_event") * pset.get<size_t>("max_fragment_size_bytes"),
						  pset.get<size_t>("stale_buffer_timeout_usec", pset.get<size_t>("event_queue_wait_time", 5) * 1000000),
						  !pset.get<bool>("broadcast_mode", false))
	, num_art_processes_(pset.get<size_t>("art_analyzer_count", 1))
	, num_fragments_per_event_(pset.get<size_t>("expected_fragments_per_event"))
	, queue_size_(pset.get<size_t>("buffer_count"))
	, run_id_(0)
	, subrun_id_(0)
	, sequence_id_(1)
	, update_run_ids_(pset.get<bool>("update_run_ids_on_new_fragment", true))
	, overwrite_mode_(!pset.get<bool>("use_art", true) || pset.get<bool>("overwrite_mode", false) || pset.get<bool>("broadcast_mode", false))
	, every_seqid_expected_(pset.get<bool>("every_sequence_id_should_be_present", false))
	, buffer_writes_pending_()
	, incomplete_event_report_interval_ms_(pset.get<int>("incomplete_event_report_interval_ms", -1))
	, last_incomplete_event_report_time_(std::chrono::steady_clock::now())
	, broadcast_timeout_ms_(pset.get<int>("fragment_broadcast_timeout_ms", 3000))
	, broadcast_count_(0)
	, subrun_event_count_(0)
	, art_processes_()
	, restart_art_(false)
	, current_art_pset_(art_pset)
	, requests_(pset)
	, broadcasts_(pset.get<uint32_t>("broadcast_shared_memory_key", 0xCEE70000 + getpid()), pset.get<size_t>("broadcast_buffer_count", 10), pset.get<size_t>("broadcast_buffer_size", 0x100000), pset.get<int>("fragment_broadcast_timeout_ms", 3000) * 1000, false)
{
	SetMinWriteSize(sizeof(detail::RawEventHeader) + sizeof(detail::RawFragmentHeader));
	broadcasts_.SetMinWriteSize(sizeof(detail::RawEventHeader) + sizeof(detail::RawFragmentHeader));
	TLOG_TRACE("SharedMemoryEventManager") << "BEGIN CONSTRUCTOR" << TLOG_ENDL;

	if (pset.get<bool>("use_art", true) == false) num_art_processes_ = 0;
	current_art_config_file_ = std::make_shared<art_config_file>(art_pset/*, GetKey(), GetBroadcastKey()*/);

	if (overwrite_mode_ && num_art_processes_ > 0)
	{
		TLOG_WARNING("SharedMemoryEventManager") << "Art is configured to run, but overwrite mode is enabled! Check your configuration if this in unintentional!" << TLOG_ENDL;
	}
	else if (overwrite_mode_)
	{
		TLOG_INFO("SharedMemoryEventManager") << "Overwrite Mode enabled, no configured art processes at startup" << TLOG_ENDL;
	}

	for (size_t ii = 0; ii < size(); ++ii)
	{
		buffer_writes_pending_[ii] = 0;
	}
	requests_.SendRoutingToken(size());

	if (!IsValid()) throw cet::exception("SharedMemoryEventManager") << "Unable to attach to Shared Memory!";

	TLOG_TRACE("SharedMemoryEventManager") << "Setting Writer rank to " << my_rank << TLOG_ENDL;
	SetRank(my_rank);
	TLOG_DEBUG("SharedMemoryEventManager") << "Writer Rank is " << GetRank() << TLOG_ENDL;


	TLOG_TRACE("SharedMemoryEventManager") << "END CONSTRUCTOR" << TLOG_ENDL;
}

artdaq::SharedMemoryEventManager::~SharedMemoryEventManager()
{
	TLOG_TRACE("SharedMemoryEventManager") << "DESTRUCTOR" << TLOG_ENDL;
	endOfData();
	TLOG_TRACE("SharedMemoryEventManager") << "Destructor END" << TLOG_ENDL;
}

bool artdaq::SharedMemoryEventManager::AddFragment(detail::RawFragmentHeader frag, void* dataPtr)
{
	TLOG_TRACE("SharedMemoryEventManager") << "AddFragment(Header, ptr) BEGIN frag.word_count=" << std::to_string(frag.word_count)
		<< ", sequence_id=" << std::to_string(frag.sequence_id) << TLOG_ENDL;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, true, frag.timestamp);
	TLOG_TRACE("SharedMemoryEventManager") << "Using buffer " << std::to_string(buffer) << TLOG_ENDL;
	if (buffer == -1) return false;
	if (buffer == -2)
	{
		TLOG_ERROR("SharedMemoryEventManager") << "Dropping event because data taking has already passed this event number: " << std::to_string(frag.sequence_id) << TLOG_ENDL;
		return true;
	}

	auto hdr = getEventHeader_(buffer);
	if (update_run_ids_)
	{
		hdr->run_id = run_id_;
		hdr->subrun_id = subrun_id_;
	}

	TLOG_TRACE("SharedMemoryEventManager") << "AddFragment before Write calls" << TLOG_ENDL;
	Write(buffer, dataPtr, frag.word_count * sizeof(RawDataType));

	TLOG_TRACE("SharedMemoryEventManager") << "Checking for complete event" << TLOG_ENDL;
	auto fragmentCount = GetFragmentCount(frag.sequence_id);
	hdr->is_complete = fragmentCount == num_fragments_per_event_ && buffer_writes_pending_[buffer] == 0;
	TLOG_TRACE("SharedMemoryEventManager") << "hdr->is_complete=" << std::boolalpha << hdr->is_complete
		<< ", fragmentCount=" << std::to_string(fragmentCount)
		<< ", num_fragments_per_event=" << std::to_string(num_fragments_per_event_)
		<< ", buffer_writes_pending_[buffer]=" << std::to_string(buffer_writes_pending_[buffer]) << TLOG_ENDL;

	complete_buffer_(buffer);
	requests_.SendRequest(true);

	TLOG_TRACE("SharedMemoryEventManager") << "AddFragment END" << TLOG_ENDL;
	return true;
}

bool artdaq::SharedMemoryEventManager::AddFragment(FragmentPtr frag, int64_t timeout_usec, FragmentPtr& outfrag)
{
	TLOG_TRACE("SharedMemoryEventManager") << "AddFragment(FragmentPtr) BEGIN" << TLOG_ENDL;
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
	TLOG_TRACE("SharedMemoryEventManager") << "AddFragment(FragmentPtr) RETURN " << std::boolalpha << sts << TLOG_ENDL;
	return sts;
}

artdaq::RawDataType* artdaq::SharedMemoryEventManager::WriteFragmentHeader(detail::RawFragmentHeader frag)
{
	TLOG_ARB(14, "SharedMemoryEventManager") << "WriteFragmentHeader BEGIN" << TLOG_ENDL;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, true, frag.timestamp);

	if (buffer == -1) return nullptr;
	if (buffer == -2)
	{
		TLOG_ERROR("SharedMemoryEventManager") << "Dropping fragment because data taking has already passed this event number: " << std::to_string(frag.sequence_id) << TLOG_ENDL;
		dropped_data_.reset(new Fragment(frag.word_count - frag.num_words()));
		return dropped_data_->dataBegin();
	}

	buffer_writes_pending_[buffer]++;
	TraceLock lk(buffer_mutexes_[buffer], 50, "WriteFragmentHeader");
	Write(buffer, &frag, frag.num_words() * sizeof(RawDataType));

	auto pos = reinterpret_cast<RawDataType*>(GetWritePos(buffer));
	IncrementWritePos(buffer, (frag.word_count - frag.num_words()) * sizeof(RawDataType));

	TLOG_ARB(14, "SharedMemoryEventManager") << "WriteFragmentHeader END" << TLOG_ENDL;
	return pos;

}

void artdaq::SharedMemoryEventManager::DoneWritingFragment(detail::RawFragmentHeader frag)
{
	TLOG_TRACE("SharedMemoryEventManager") << "DoneWritingFragment BEGIN" << TLOG_ENDL;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, false, frag.timestamp);
	if (buffer == -1) Detach(true, "SharedMemoryEventManager", "getBufferForSequenceID_ returned -1 when it REALLY shouldn't have! Check program logic!");
	if (buffer == -2) return;
	buffer_writes_pending_[buffer]--;
	auto hdr = getEventHeader_(buffer);
	if (update_run_ids_)
	{
		hdr->run_id = run_id_;
		hdr->subrun_id = subrun_id_;
	}
	hdr->is_complete = GetFragmentCount(frag.sequence_id) == num_fragments_per_event_ && buffer_writes_pending_[buffer] == 0;

	complete_buffer_(buffer);
	requests_.SendRequest(true);
	TLOG_TRACE("SharedMemoryEventManager") << "DoneWritingFragment END" << TLOG_ENDL;
}

size_t artdaq::SharedMemoryEventManager::GetFragmentCount(Fragment::sequence_id_t seqID, Fragment::type_t type)
{
	auto buffer = getBufferForSequenceID_(seqID, false);
	if (buffer == -1) return 0;
	ResetReadPos(buffer);
	IncrementReadPos(buffer, sizeof(detail::RawEventHeader));

	size_t count = 0;

	while (MoreDataInBuffer(buffer))
	{
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(buffer));
		IncrementReadPos(buffer, fragHdr->word_count * sizeof(RawDataType));
		if (type != Fragment::InvalidFragmentType && fragHdr->type != type) continue;
		TLOG_TRACE("GetFragmentCount") << "Adding Fragment with size=" << std::to_string(fragHdr->word_count) << " to Fragment count" << TLOG_ENDL;
		++count;
	}

	return count;
}


void artdaq::SharedMemoryEventManager::RunArt(std::shared_ptr<art_config_file> config_file, pid_t& pid_out)
{
	while (restart_art_)
	{
		send_init_frag_();
		TLOG_INFO("SharedMemoryEventManager") << "Starting art process with config file " << config_file->getFileName() << TLOG_ENDL;
		std::vector<char*> args{ (char*)"art", (char*)"-c", &config_file->getFileName()[0], NULL };

		auto pid = fork();
		if (pid == 0)
		{ /* child */
			execvp("art", &args[0]);
			exit(1);
		}
		pid_out = pid;

		TLOG_INFO("SharedMemoryEventManager") << "PID of new art process is " << pid << TLOG_ENDL;
		art_processes_.insert(pid);
		int status;
		waitpid(pid, &status, 0);
		TLOG_INFO("SharedMemoryEventManager") << "Removing PID " << pid << " from process list" << TLOG_ENDL;
		art_processes_.erase(pid);
		if (status == 0)
		{
			TLOG_INFO("SharedMemoryEventManager") << "art process " << pid << " exited normally, " << (restart_art_ ? "restarting" : "not restarting") << TLOG_ENDL;
		}
		else
		{
			TLOG_WARNING("SharedMemoryEventManager") << "art process " << pid << " exited with status code 0x" << std::hex << status << " (" << std::dec << status << "), " << (restart_art_ ? "restarting" : "not restarting") << TLOG_ENDL;
		}
	}
}

void artdaq::SharedMemoryEventManager::StartArt()
{
	restart_art_ = true;
	if (num_art_processes_ == 0) return;
	for (size_t ii = 0; ii < num_art_processes_; ++ii)
	{
		StartArtProcess(current_art_pset_);
	}
}

pid_t artdaq::SharedMemoryEventManager::StartArtProcess(fhicl::ParameterSet pset)
{
	static std::mutex start_art_mutex;
	TraceLock lk(start_art_mutex, 15, "StartArtLock");
	restart_art_ = true;
	auto initialCount = GetAttachedCount();
	auto startTime = std::chrono::steady_clock::now();

	if (pset != current_art_pset_)
	{
		current_art_pset_ = pset;
		current_art_config_file_ = std::make_shared<art_config_file>(pset/*, GetKey(), GetBroadcastKey()*/);
	}
	pid_t pid = -1;
	std::thread thread([&] {RunArt(current_art_config_file_, pid); });
	thread.detach();


	while ((GetAttachedCount() - initialCount < 1 || pid <= 0)
		   && std::chrono::duration_cast<TimeUtils::seconds>(std::chrono::steady_clock::now() - startTime).count() < 5)
	{
		usleep(1000);
	}
	if (GetAttachedCount() - initialCount < 1 || pid <= 0)
	{
		TLOG_WARNING("SharedMemoryEventManager") << "art process has not started after 5s. Check art configuration!"
			<< " (pid=" << pid << ", attachedCount=" << std::to_string(GetAttachedCount() - initialCount) << ")" << TLOG_ENDL;
		return 0;
	}
	else
	{
		TLOG_INFO("SharedMemoryEventManager") << std::setw(4) << std::fixed << "art initialization took "
			<< std::chrono::duration_cast<TimeUtils::seconds>(std::chrono::steady_clock::now() - startTime).count() << " seconds." << TLOG_ENDL;

		return pid;
	}

}

void artdaq::SharedMemoryEventManager::ShutdownArtProcesses(std::set<pid_t> pids)
{
	restart_art_ = false;
	current_art_config_file_ = nullptr;
	current_art_pset_ = fhicl::ParameterSet();

	for (auto pid : pids)
	{
		if (kill(pid, 0) >= 0)
		{
			pids.erase(pid);
		}
	}
	if (pids.size() == 0)
	{
		TLOG_ARB(14, "SharedMemoryEventManager") << "All art processes already exited, nothing to do." << TLOG_ENDL;
		usleep(1000);
		return;
	}

	TLOG_TRACE("SharedMemoryEventManager") << "Gently informing art processes that it is time to shut down" << TLOG_ENDL;
	for (auto pid : pids)
	{
		kill(pid, SIGQUIT);
	}

	int graceful_wait_ms = 1000;
	int int_wait_ms = 100;

	TLOG_TRACE("SharedMemoryEventManager") << "Waiting up to " << graceful_wait_ms << " ms for all art processes to exit gracefully" << TLOG_ENDL;
	for (int ii = 0; ii < graceful_wait_ms; ++ii)
	{
		usleep(1000);

		for (auto pid : pids)
		{
			if (kill(pid, 0) < 0)
			{
				pids.erase(pid);
			}
		}
		if (pids.size() == 0)
		{
			TLOG_TRACE("SharedMemoryEventManager") << "All art processes exited after " << ii << " ms." << TLOG_ENDL;
			return;
		}
	}

	TLOG_TRACE("SharedMemoryEventManager") << "Insisting that the art processes shut down" << TLOG_ENDL;
	for (auto pid : pids)
	{
		kill(pid, SIGINT);
	}

	TLOG_TRACE("SharedMemoryEventManager") << "Waiting up to " << int_wait_ms << " ms for all art processes to exit" << TLOG_ENDL;
	for (int ii = graceful_wait_ms; ii < graceful_wait_ms + int_wait_ms; ++ii)
	{
		usleep(1000);

		for (auto pid : pids)
		{
			if (kill(pid, 0) < 0)
			{
				pids.erase(pid);
			}
		}

		if (pids.size() == 0)
		{
			TLOG_TRACE("SharedMemoryEventManager") << "All art processes exited after " << ii << " ms." << TLOG_ENDL;
			return;
		}
	}

	TLOG_TRACE("SharedMemoryEventManager") << "Killing remaning art processes with extreme prejudice" << TLOG_ENDL;
	while (pids.size() > 0)
	{
		kill(*pids.begin(), SIGKILL);
	}
}

void artdaq::SharedMemoryEventManager::ReconfigureArt(fhicl::ParameterSet art_pset, run_id_t newRun, int n_art_processes)
{
	TLOG_DEBUG("SharedMemoryEventManager") << "ReconfigureArt BEGIN" << TLOG_ENDL;
	if (restart_art_) // Art is running
	{
		endOfData();
	}
	for (size_t ii = 0; ii < broadcasts_.size(); ++ii)
	{
		broadcasts_.MarkBufferEmpty(ii, true);
	}
	if (newRun == 0) newRun = run_id_ + 1;
	current_art_pset_ = art_pset;
	current_art_config_file_ = std::make_shared<art_config_file>(art_pset/*, GetKey(), GetBroadcastKey()*/);

	if (n_art_processes != -1)
	{
		TLOG_INFO("SharedMemoryEventManager") << "Setting number of art processes to " << n_art_processes << TLOG_ENDL;
		num_art_processes_ = n_art_processes;
	}
	startRun(newRun);
	TLOG_DEBUG("SharedMemoryEventManager") << "ReconfigureArt END" << TLOG_ENDL;
}

bool artdaq::SharedMemoryEventManager::endOfData()
{
	init_fragment_.reset(nullptr);
	TLOG_TRACE("SharedMemoryEventManager") << "SharedMemoryEventManager::endOfData" << TLOG_ENDL;
	restart_art_ = false;

	size_t initialStoreSize = GetInactiveEventCount();
	TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Flushing " << initialStoreSize
		<< " inactive events from the SharedMemoryEventManager." << TLOG_ENDL;
	for (auto& buf : inactive_buffers_)
	{
		MarkBufferEmpty(buf, true);
		inactive_buffers_.erase(buf);
	}
	TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Done flushing, there are now " << GetInactiveEventCount()
		<< " inactive events in the SharedMemoryEventManager." << TLOG_ENDL;
	initialStoreSize = GetIncompleteEventCount();
	TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Flushing " << initialStoreSize
		<< " stale events from the SharedMemoryEventManager." << TLOG_ENDL;
	for (auto& buf : active_buffers_)
	{
		complete_buffer_(buf);
	}
	TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Done flushing, there are now " << GetIncompleteEventCount()
		<< " stale events in the SharedMemoryEventManager." << TLOG_ENDL;


	TLOG_TRACE("SharedMemoryEventManager") << "Waiting for " << std::to_string(ReadReadyCount() + (size() - WriteReadyCount(overwrite_mode_))) << " outstanding buffers..." << TLOG_ENDL;
	auto start = std::chrono::steady_clock::now();
	auto lastReadCount = ReadReadyCount() + (size() - WriteReadyCount(overwrite_mode_));

	// We will wait until no buffer has been read for 1 second.
	while (lastReadCount > 0 && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() < 1000)
	{
		auto temp = ReadReadyCount() + (size() - WriteReadyCount(overwrite_mode_));
		if (temp != lastReadCount)
		{
			TLOG_TRACE("SharedMemoryEventManager") << "Waiting for " << std::to_string(temp) << " outstanding buffers..." << TLOG_ENDL;
			lastReadCount = temp;
			start = std::chrono::steady_clock::now();
		}
		if (lastReadCount > 0) usleep(1000);
	}

	TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Broadcasting EndOfData Fragment" << TLOG_ENDL;
	FragmentPtr outFrag = std::move(Fragment::eodFrag(GetBufferCount()));
	bool success = broadcastFragment_(std::move(outFrag), outFrag);
	if (!success)
	{
		TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Clearing buffers to make room for EndOfData Fragment" << TLOG_ENDL;
		for (size_t ii = 0; ii < size(); ++ii)
		{
			broadcasts_.MarkBufferEmpty(ii, true);
		}
		broadcastFragment_(std::move(outFrag), outFrag);
	}

	while (art_processes_.size() > 0)
	{
		TLOG_DEBUG("SharedMemoryEventManager") << "Waiting for all art processes to exit, there are " << std::to_string(art_processes_.size()) << " remaining." << TLOG_ENDL;
		ShutdownArtProcesses(art_processes_);
	}
	ResetAttachedCount();

	TLOG_TRACE("SharedMemoryEventManager") << "endOfData: Clearing buffers" << TLOG_ENDL;
	for (size_t ii = 0; ii < size(); ++ii)
	{
		MarkBufferEmpty(ii, true);
	}

	TLOG_TRACE("SharedMemoryEventManager") << "endOfData END" << TLOG_ENDL;
	TLOG_INFO("SharedMemoryEventManager") << "EndOfData Complete. There were " << GetLastSeenBufferID() << " events processed in this run." << TLOG_ENDL;
	return true;
}



void artdaq::SharedMemoryEventManager::startRun(run_id_t runID)
{
	init_fragment_.reset(nullptr);
	StartArt();
	run_id_ = runID;
	subrun_id_ = 1;
	sequence_id_ = 1;
	requests_.SendRoutingToken(queue_size_);
	TLOG_DEBUG("SharedMemoryEventManager") << "Starting run " << run_id_
		<< ", max queue size = "
		<< queue_size_
		<< ", queue size = "
		<< GetLockedBufferCount() << TLOG_ENDL;
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
	broadcastFragment_(std::move(endOfRunFrag), endOfRunFrag);

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

	broadcastFragment_(std::move(endOfSubrunFrag), endOfSubrunFrag);

	TLOG_INFO("SharedMemoryEventManager") << "Subrun " << subrun_id_ << " in run " << run_id_ << " has ended. There were " << subrun_event_count_ << " events in this subrun." << TLOG_ENDL;
	subrun_event_count_ = 0;

	return true;
}

void artdaq::SharedMemoryEventManager::sendMetrics()
{
	if (metricMan)
	{
		metricMan->sendMetric("Incomplete Event Count", GetIncompleteEventCount(), "events", 1);
		metricMan->sendMetric("Reserved Event Buffers", GetInactiveEventCount(), "events", 1);
		metricMan->sendMetric("Pending Event Count", GetPendingEventCount(), "events", 1);
	}
	check_pending_buffers_();
	if (incomplete_event_report_interval_ms_ > 0 && GetLockedBufferCount())
	{
		if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_incomplete_event_report_time_).count() < incomplete_event_report_interval_ms_) return;
		last_incomplete_event_report_time_ = std::chrono::steady_clock::now();
		std::ostringstream oss;
		oss << "Incomplete Events (" << num_fragments_per_event_ << "): ";
		for (auto& ev : active_buffers_)
		{
			auto hdr = getEventHeader_(ev);
			oss << hdr->sequence_id << " (" << GetFragmentCount(hdr->sequence_id) << "), ";
		}
		TLOG_DEBUG("SharedMemoryEventManager") << oss.str() << TLOG_ENDL;
	}
}

bool artdaq::SharedMemoryEventManager::broadcastFragment_(FragmentPtr frag, FragmentPtr& outFrag)
{
	auto buffer = broadcasts_.GetBufferForWriting(false);
	auto start_time = std::chrono::steady_clock::now();
	while (buffer == -1 && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() < broadcast_timeout_ms_)
	{
		usleep(10000);
		buffer = broadcasts_.GetBufferForWriting(false);
	}
	if (buffer == -1)
	{
		TLOG_ERROR("SharedMemoryEventManager") << "Broadcast of fragment type " << frag->typeString() << " failed due to timeout waiting for buffer!" << TLOG_ENDL;
		outFrag.swap(frag);
		return false;
	}

	auto hdr = reinterpret_cast<detail::RawEventHeader*>(broadcasts_.GetBufferStart(buffer));
	hdr->run_id = run_id_;
	hdr->subrun_id = subrun_id_;
	hdr->sequence_id = frag->sequenceID();
	hdr->is_complete = true;
	broadcasts_.IncrementWritePos(buffer, sizeof(detail::RawEventHeader));

	TLOG_TRACE("SharedMemoryEventManager") << "broadcastFragment_ before Write calls" << TLOG_ENDL;
	broadcasts_.Write(buffer, frag->headerAddress(), frag->size() * sizeof(RawDataType));

	broadcasts_.MarkBufferFull(buffer, -1);
	outFrag.swap(frag);
	return true;
}

artdaq::detail::RawEventHeader* artdaq::SharedMemoryEventManager::getEventHeader_(int buffer)
{
	return reinterpret_cast<detail::RawEventHeader*>(GetBufferStart(buffer));
}

int artdaq::SharedMemoryEventManager::getBufferForSequenceID_(Fragment::sequence_id_t seqID, bool create_new, Fragment::timestamp_t timestamp)
{
	check_pending_buffers_();
	std::unique_lock<std::mutex> lk(sequence_id_mutex_);
	TLOG_ARB(14, "SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " BEGIN" << TLOG_ENDL;
	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		auto hdr = getEventHeader_(buf);
		if (hdr->sequence_id == seqID)
		{
			TLOG_ARB(14, "SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " returning " << buf << TLOG_ENDL;
			if (inactive_buffers_.count(buf))
			{
				inactive_buffers_.erase(buf);
				active_buffers_.insert(buf);
				if (timestamp != Fragment::InvalidTimestamp) requests_.AddRequest(seqID, timestamp);
			}

			return buf;
		}
	}

	if (seqID < sequence_id_)
	{
		TLOG_ERROR("SharedMemoryEventManager") << "Received request for buffer for sequence ID " << std::to_string(seqID) << ", but that buffer no longer exists!" << TLOG_ENDL;
		return -2;
	}

	if (!create_new) return -1;

	int new_buffer;
	while (seqID >= sequence_id_)
	{
		new_buffer = GetBufferForWriting(overwrite_mode_);
		if (new_buffer == -1 && !every_seqid_expected_)
		{
			auto bufs = inactive_buffers_;
			Fragment::sequence_id_t lowestSeqId = seqID;
			int buffer = -1;
			for (auto buf : bufs)
			{
				auto hdr = getEventHeader_(buf);
				if (hdr->sequence_id < lowestSeqId)
				{
					lowestSeqId = hdr->sequence_id;
					buffer = buf;
				}
			}

			if (buffer == -1) return -1;

			ResetWritePos(buffer);
			new_buffer = buffer;
		}

		if (new_buffer == -1) return -1;
		TraceLock(buffer_mutexes_[new_buffer], 34, "getBufferForSequenceID");
		auto hdr = getEventHeader_(new_buffer);
		hdr->is_complete = false;
		hdr->run_id = run_id_;
		hdr->subrun_id = subrun_id_;
		hdr->sequence_id = sequence_id_;
		buffer_writes_pending_[new_buffer] = 0;
		IncrementWritePos(new_buffer, sizeof(detail::RawEventHeader));
		sequence_id_++;
		inactive_buffers_.insert(new_buffer);
	}

	inactive_buffers_.erase(new_buffer);
	active_buffers_.insert(new_buffer);

	if (timestamp != Fragment::InvalidTimestamp)
	{
		requests_.AddRequest(seqID, timestamp);
	}
	requests_.SendRequest();
	TLOG_ARB(14, "SharedMemoryEventManager") << "getBufferForSequenceID " << std::to_string(seqID) << " returning newly initialized buffer " << new_buffer << TLOG_ENDL;
	return new_buffer;
}

bool artdaq::SharedMemoryEventManager::hasFragments_(int buffer)
{
	if (buffer == -1) return true;
	if (!CheckBuffer(buffer, BufferSemaphoreFlags::Writing))
	{
		return true;
	}
	ResetReadPos(buffer);
	IncrementReadPos(buffer, sizeof(detail::RawEventHeader));
	return MoreDataInBuffer(buffer);
}

void artdaq::SharedMemoryEventManager::complete_buffer_(int buffer)
{
	auto hdr = getEventHeader_(buffer);
	if (hdr->is_complete)
	{
		TLOG_DEBUG("SharedMemoryEventManager") << "complete_buffer_: This fragment completes event " << std::to_string(hdr->sequence_id) << "." << TLOG_ENDL;

		requests_.RemoveRequest(hdr->sequence_id);
		requests_.SendRoutingToken(1);
		{
			std::unique_lock<std::mutex> lk(sequence_id_mutex_);
			active_buffers_.erase(buffer);
			pending_buffers_.insert(buffer);
		}
	}
	check_pending_buffers_();
}

bool artdaq::SharedMemoryEventManager::bufferComparator(int bufA, int bufB)
{
	return getEventHeader_(bufA)->sequence_id < getEventHeader_(bufB)->sequence_id;
}

void artdaq::SharedMemoryEventManager::check_pending_buffers_()
{
	TLOG_TRACE("SharedMemoryEventManager") << "check_pending_buffers_ BEGIN" << TLOG_ENDL;
	{
		std::unique_lock<std::mutex> lk(sequence_id_mutex_);
		auto buffers = GetBuffersOwnedByManager();
		for (auto buf : buffers)
		{
			if (ResetBuffer(buf) && !pending_buffers_.count(buf))
			{
				auto hdr = getEventHeader_(buf);
				if (active_buffers_.count(buf))
				{
					TLOG_WARNING("SharedMemoryEventManager") << "Active event " << std::to_string(hdr->sequence_id) << " is stale. Scheduling release of incomplete event to art." << TLOG_ENDL;
					requests_.RemoveRequest(hdr->sequence_id);
					requests_.SendRoutingToken(1);
					active_buffers_.erase(buf);
					pending_buffers_.insert(buf);
				}
				else if (inactive_buffers_.count(buf))
				{
					TLOG_DEBUG("SharedMemoryEventManager") << "Inactive event " << std::to_string(hdr->sequence_id) << " is stale. Resetting to Empty." << TLOG_ENDL;
					inactive_buffers_.erase(buf);
					MarkBufferEmpty(buf, true);
				}

			}
		}

		Fragment::sequence_id_t lowestSeqId = Fragment::InvalidSequenceID;

		for (auto buf : inactive_buffers_)
		{
			auto hdr = getEventHeader_(buf);
			TLOG_TRACE("SharedMemoryEventManager") << "Buffer: " << buf << ", SeqID: " << std::to_string(hdr->sequence_id) << ", INACTIVE" << TLOG_ENDL;
			if (hdr->sequence_id < lowestSeqId)
			{
				lowestSeqId = hdr->sequence_id;
			}
		}
		for (auto buf : active_buffers_)
		{
			auto hdr = getEventHeader_(buf);
			TLOG_TRACE("SharedMemoryEventManager") << "Buffer: " << buf << ", SeqID: " << std::to_string(hdr->sequence_id) << ", ACTIVE" << TLOG_ENDL;
			if (hdr->sequence_id < lowestSeqId)
			{
				lowestSeqId = hdr->sequence_id;
			}
		}
		TLOG_TRACE("SharedMemoryEventManager") << "Lowest SeqID held: " << std::to_string(lowestSeqId) << TLOG_ENDL;

		std::list<int> sorted_buffers(pending_buffers_.begin(), pending_buffers_.end());
		sorted_buffers.sort([this](int a, int b) {return bufferComparator(a, b); });
		for (auto buf : sorted_buffers)
		{
			auto hdr = getEventHeader_(buf);
			if (hdr->sequence_id > lowestSeqId) break;
			TLOG_DEBUG("SharedMemoryEventManager") << "Releasing event " << std::to_string(hdr->sequence_id) << " in buffer " << buf << " to art." << TLOG_ENDL;
			MarkBufferFull(buf);
			subrun_event_count_++;
			pending_buffers_.erase(buf);
		}
	}

	TLOG_TRACE("SharedMemoryEventManager") << "check_pending_buffers_: Sending Metrics" << TLOG_ENDL;
	if (metricMan)
	{
		auto full = ReadReadyCount();
		auto empty = WriteReadyCount(overwrite_mode_);
		auto total = size();
		metricMan->sendMetric("Shared Memory Full Buffers", full, "buffers", 2);
		metricMan->sendMetric("Shared Memory Available Buffers", empty, "buffers", 2);
		metricMan->sendMetric("Shared Memory Full %", full * 100 / static_cast<double>(total), "%", 2);
		metricMan->sendMetric("Shared Memory Available %", empty * 100 / static_cast<double>(total), "%", 2);
	}
	TLOG_TRACE("SharedMemoryEventManager") << "check_pending_buffers_ END" << TLOG_ENDL;
}

void artdaq::SharedMemoryEventManager::send_init_frag_()
{
	if (init_fragment_ != nullptr)
	{
		TLOG_TRACE("SharedMemoryEventManager") << "Sending init Fragment to art..." << TLOG_ENDL;

#if 0
		std::string fileName = "receiveInitMessage_" + std::to_string(my_rank) + ".bin";
		std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
		ostream.write(reinterpret_cast<char*>(init_fragment_->dataBeginBytes()), init_fragment_->dataSizeBytes());
		ostream.close();
#endif

		broadcastFragment_(std::move(init_fragment_), init_fragment_);
		TLOG_TRACE("SharedMemoryEventManager") << "Init Fragment sent" << TLOG_ENDL;
	}
	else
	{
		TLOG_WARNING("SharedMemoryEventManager") << "Cannot send init fragment because I haven't yet received one!" << TLOG_ENDL;
	}
}

void artdaq::SharedMemoryEventManager::SetInitFragment(FragmentPtr frag)
{
	if (!init_fragment_ || init_fragment_ == nullptr)
	{
		init_fragment_.swap(frag);
		send_init_frag_();
	}
}
