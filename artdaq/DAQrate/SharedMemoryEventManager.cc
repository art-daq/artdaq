
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include <sys/wait.h>

#include <poll.h>
#include <memory>
#include <numeric>

#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq-core/Data/MetadataFragment.hh"
#include "artdaq-core/Utilities/TraceLock.hh"

#define TRACE_NAME (app_name + "_SharedMemoryEventManager").c_str()

// clang-format off
#define TLVL_ADDFRAGMENT            32
#define TLVL_ADDINITFRAGMENT        33
#define TLVL_BROADCASTFRAGMENT      34
#define TLVL_BROADCASTFRAGMENTS     35
#define TLVL_BROADCASTFRAGMENTS_2   36
#define TLVL_CHECKPENDINGBROADCASTS 37
#define TLVL_CHECKPENDINGBUFFERS    38
#define TLVL_CHECKPENDINGBUFFERS_2  39
#define TLVL_CHECKPENDINGBUFFERS_3  40
#define TLVL_CHECKPENDINGBUFFERS_4  41
#define TLVL_COMPLETEBUFFER         42
#define TLVL_CONSTRUCTOR            43
#define TLVL_DESTRUCTOR             43
#define TLVL_DONEWRITINGFRAGMENT    44
#define TLVL_ENDOFDATA              45
#define TLVL_ENDOFDATA_2            46
#define TLVL_ENDRUN                 47
#define TLVL_GETBUFFER              48
#define TLVL_GETFRAGMENTCOUNT       49
#define TLVL_GETSUBRUN              50
#define TLVL_GETEVENTID             51
#define TLVL_PARSEARTCOMMANDLINE    52
#define TLVL_RECONFIGUREART         53
#define TLVL_RUNART                 54
#define TLVL_RUNART_2               55
#define TLVL_SENDINIT               56
#define TLVL_SENDMETRICS            57
#define TLVL_SHUTDOWN               58
#define TLVL_STARTRUN               58
#define TLVL_UPDATEARTCONFIG        59
#define TLVL_WRITEFRAGMENTHEADER    60
#define TLVL_BUFFER                 61
#define TLVL_BUFLCK                 62
// clang-format on

std::mutex artdaq::SharedMemoryEventManager::sequence_id_mutex_;
std::mutex artdaq::SharedMemoryEventManager::subrun_event_map_mutex_;
const std::string artdaq::SharedMemoryEventManager::
    FRAGMENTS_RECEIVED_STAT_KEY("SharedMemoryEventManagerFragmentsReceived");
const std::string artdaq::SharedMemoryEventManager::
    EVENTS_RELEASED_STAT_KEY("SharedMemoryEventManagerEventsReleased");

artdaq::SharedMemoryEventManager::SharedMemoryEventManager(const fhicl::ParameterSet& pset, fhicl::ParameterSet art_pset)
    : SharedMemoryManager(pset.get<uint32_t>("shared_memory_key", Globals::SharedMemoryKey(0xEE000000)),
                          pset.get<size_t>("buffer_count"),
                          pset.has_key("max_event_size_bytes") ? pset.get<size_t>("max_event_size_bytes") : pset.get<size_t>("expected_fragments_per_event") * pset.get<size_t>("max_fragment_size_bytes"),
                          pset.get<size_t>("stale_buffer_timeout_usec", pset.get<size_t>("event_queue_wait_time", 5) * 1000000),
                          !pset.get<bool>("broadcast_mode", false))
    , num_art_processes_(pset.get<size_t>("art_analyzer_count", 1))
    , num_fragments_per_event_(pset.get<size_t>("expected_fragments_per_event"))
    , queue_size_(pset.get<size_t>("buffer_count"))
    , run_id_(0)
    , subrun_id_(0)
    , max_subrun_event_map_length_(pset.get<size_t>("max_subrun_lookup_table_size", 100))
    , subrun_transition_hold_time_s_(pset.get<double>("subrun_transition_hold_time_s", 0.001))
    , max_event_list_length_(pset.get<size_t>("max_event_list_length", 100))
    , use_sequence_id_for_event_number_(pset.get<bool>("use_sequence_id_for_event_number", true))
    , reset_event_number_for_subruns_(pset.get<bool>("reset_event_number_for_subruns", false))
    , overwrite_mode_(!pset.get<bool>("use_art", true) || pset.get<bool>("overwrite_mode", false) || pset.get<bool>("broadcast_mode", false))
    , shared_memory_ordering_(pset.get<bool>("shared_memory_ordering", false))
    , init_fragment_count_(pset.get<size_t>("init_fragment_count", pset.get<bool>("send_init_fragments", true) ? 1 : 0))
    , running_(false)
    , buffer_writes_pending_()
    , open_event_report_interval_ms_(pset.get<int>("open_event_report_interval_ms", pset.get<int>("incomplete_event_report_interval_ms", -1)))
    , last_open_event_report_time_(std::chrono::steady_clock::now())
    , last_backpressure_report_time_(std::chrono::steady_clock::now())
    , last_fragment_header_write_time_(std::chrono::steady_clock::now())
    , event_timing_(pset.get<size_t>("buffer_count"))
    , broadcast_timeout_ms_(pset.get<int>("fragment_broadcast_timeout_ms", 3000))
    , run_event_count_(0)
    , run_incomplete_event_count_(0)
    , subrun_event_count_(0)
    , subrun_incomplete_event_count_(0)
    , oversize_fragment_count_(0)
    , maximum_oversize_fragment_count_(pset.get<int>("maximum_oversize_fragment_count", 1))
    , restart_art_(false)
    , always_restart_art_(pset.get<bool>("restart_crashed_art_processes", true))
    , manual_art_(pset.get<bool>("manual_art", false))
    , current_art_pset_(art_pset)
    , art_cmdline_(pset.get<std::string>("art_command_line", "art -c #CONFIG_FILE#"))
    , art_process_index_offset_(pset.get<size_t>("art_index_offset", 0))
    , minimum_art_lifetime_s_(pset.get<double>("minimum_art_lifetime_s", 2.0))
    , art_event_processing_time_us_(pset.get<size_t>("expected_art_event_processing_time_us", 1000000))
    , capture_art_stdout_(pset.get<bool>("capture_art_stdout", true))
    , capture_art_stderr_(pset.get<bool>("capture_art_stderr", true))
    , requests_(nullptr)
    , tokens_(nullptr)
    , data_pset_(pset)
    , broadcasts_(pset.get<uint32_t>("broadcast_shared_memory_key", Globals::SharedMemoryKey(0xBB000000)),
                  pset.get<size_t>("broadcast_buffer_count", 10),
                  pset.get<size_t>("broadcast_buffer_size", 0x100000),
                  pset.get<int>("expected_art_event_processing_time_us", 100000) * pset.get<size_t>("buffer_count"), false)
{
	subrun_event_map_[0] = 1;
	SetMinWriteSize(sizeof(detail::RawEventHeader) + sizeof(detail::RawFragmentHeader));
	broadcasts_.SetMinWriteSize(sizeof(detail::RawEventHeader) + sizeof(detail::RawFragmentHeader));

	RegisterWriter();
	broadcasts_.RegisterWriter();

	if (!pset.get<bool>("use_art", true))
	{
		TLOG(TLVL_INFO) << "BEGIN SharedMemoryEventManager CONSTRUCTOR with use_art:false";
		num_art_processes_ = 0;
	}
	else
	{
		TLOG(TLVL_INFO) << "BEGIN SharedMemoryEventManager CONSTRUCTOR with use_art:true";
		TLOG(TLVL_CONSTRUCTOR) << "art_pset is " << art_pset.to_string();
	}

	if (manual_art_)
		current_art_config_file_ = std::make_shared<art_config_file>(art_pset, GetKey(), GetBroadcastKey());
	else
		current_art_config_file_ = std::make_shared<art_config_file>(art_pset);

	if (overwrite_mode_ && num_art_processes_ > 0)
	{
		TLOG(TLVL_WARNING) << "Art is configured to run, but overwrite mode is enabled! Check your configuration if this in unintentional!";
	}
	else if (overwrite_mode_)
	{
		TLOG(TLVL_INFO) << "Overwrite Mode enabled, no configured art processes at startup";
	}

	for (size_t ii = 0; ii < size(); ++ii)
	{
		buffer_writes_pending_[ii] = 0;
		// Make sure the mutexes are created once
		std::lock_guard<std::mutex> lk(buffer_mutexes_[ii]);
	}

	if (!IsValid())
	{
		TLOG(TLVL_ERROR) << "Unable to attach to Shared Memory!";
		throw cet::exception(app_name + "_SharedMemoryEventManager") << "Unable to attach to Shared Memory!";  // NOLINT(cert-err60-cpp)
	}

	TLOG(TLVL_CONSTRUCTOR) << "Setting Writer rank to " << my_rank;
	SetRank(my_rank);
	TLOG(TLVL_CONSTRUCTOR) << "Writer Rank is " << GetRank();

	statsHelper_.addMonitoredQuantityName(FRAGMENTS_RECEIVED_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(EVENTS_RELEASED_STAT_KEY);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(pset, 100, 30.0, 60.0, EVENTS_RELEASED_STAT_KEY);

	TLOG(TLVL_CONSTRUCTOR) << "END CONSTRUCTOR";
}

artdaq::SharedMemoryEventManager::~SharedMemoryEventManager() noexcept
{
	TLOG(TLVL_DESTRUCTOR) << "DESTRUCTOR";
	if (running_)
	{
		try
		{
			endOfData();
		}
		catch (...)
		{
			// IGNORED
		}
	}

	UnregisterWriter();
	broadcasts_.UnregisterWriter();
	TLOG(TLVL_DESTRUCTOR) << "Destructor END";
}

bool artdaq::SharedMemoryEventManager::AddFragment(detail::RawFragmentHeader frag, void* dataPtr)
{
	if (!running_) return true;

	TLOG(TLVL_ADDFRAGMENT) << "AddFragment(Header, ptr) BEGIN frag.word_count=" << frag.word_count
	                       << ", sequence_id=" << frag.sequence_id;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, true, frag.timestamp);
	TLOG(TLVL_ADDFRAGMENT) << "Using buffer " << buffer << " for seqid=" << frag.sequence_id;
	if (buffer == -1)
	{
		return false;
	}
	if (buffer == -2)
	{
		TLOG(TLVL_ERROR) << "Dropping event because data taking has already passed this event number: " << frag.sequence_id;
		return true;
	}

	auto hdr = getEventHeader_(buffer);
	hdr->run_id = run_id_;
	hdr->subrun_id = GetSubrunForSequenceID(frag.sequence_id);
	hdr->event_id = GetEventIDForFragment(frag.sequence_id, frag.timestamp);

	TLOG(TLVL_ADDFRAGMENT) << "AddFragment before Write calls";
	Write(buffer, dataPtr, frag.word_count * sizeof(RawDataType));

	TLOG(TLVL_ADDFRAGMENT) << "Checking for complete event";
	auto fragmentCount = GetFragmentCount(frag.sequence_id);
	hdr->is_complete = fragmentCount == num_fragments_per_event_ && buffer_writes_pending_[buffer] == 0;
	TLOG(TLVL_ADDFRAGMENT) << "hdr->is_complete=" << std::boolalpha << hdr->is_complete
	                       << ", fragmentCount=" << fragmentCount
	                       << ", num_fragments_per_event=" << num_fragments_per_event_
	                       << ", buffer_writes_pending_[buffer]=" << buffer_writes_pending_[buffer];

	complete_buffer_(buffer);

	TLOG(TLVL_ADDFRAGMENT) << "AddFragment END";
	statsHelper_.addSample(FRAGMENTS_RECEIVED_STAT_KEY, frag.word_count * sizeof(RawDataType));
	return true;
}

bool artdaq::SharedMemoryEventManager::AddFragment(FragmentPtr frag, size_t timeout_usec, FragmentPtr& outfrag)
{
	TLOG(TLVL_ADDFRAGMENT) << "AddFragment(FragmentPtr) BEGIN";
	auto hdr = *reinterpret_cast<detail::RawFragmentHeader*>(frag->headerAddress());  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	auto data = frag->headerAddress();
	auto start = std::chrono::steady_clock::now();
	bool sts = false;
	while (!sts && TimeUtils::GetElapsedTimeMicroseconds(start) < timeout_usec)
	{
		sts = AddFragment(hdr, data);
		if (!sts)
		{
			usleep(1000);
		}
	}
	if (!sts)
	{
		outfrag = std::move(frag);
	}
	TLOG(TLVL_ADDFRAGMENT) << "AddFragment(FragmentPtr) RETURN " << std::boolalpha << sts;
	return sts;
}

artdaq::RawDataType* artdaq::SharedMemoryEventManager::WriteFragmentHeader(detail::RawFragmentHeader frag, bool dropIfNoBuffersAvailable)
{
	if (!running_) return nullptr;
	TLOG(TLVL_WRITEFRAGMENTHEADER) << "WriteFragmentHeader BEGIN, seqID=" << frag.sequence_id;
	auto buffer = getBufferForSequenceID_(frag.sequence_id, true, frag.timestamp);

	if (buffer < 0)
	{
		if (buffer == -1 && !dropIfNoBuffersAvailable)
		{
			std::unique_lock<std::mutex> bp_lk(sequence_id_mutex_);
			if (TimeUtils::GetElapsedTime(last_backpressure_report_time_) > 1.0)
			{
				TLOG(TLVL_WARNING) << app_name << ": Back-pressure condition: All Shared Memory buffers have been full for " << TimeUtils::GetElapsedTime(last_fragment_header_write_time_) << " s! There are " << (GetAttachedCount() - 1) << " art processes connected.";
				last_backpressure_report_time_ = std::chrono::steady_clock::now();
				if (GetAttachedCount() == 1 && !restart_art_)
				{
					TLOG(TLVL_ERROR) << "All art processes have died, and restarting was unsuccessful. Check PMT log file for error messages";
					throw cet::exception(app_name + "_SharedMemoryEventManager") << "All art processes have died, and restarting was unsuccessful. Check PMT log file for error messages";  // NOLINT(cert-err60-cpp)
				}
			}
			if (metricMan)
			{
				metricMan->sendMetric("Back-pressure wait time", TimeUtils::GetElapsedTime(last_fragment_header_write_time_), "s", 1, MetricMode::LastPoint);
			}
			TLOG(TLVL_WRITEFRAGMENTHEADER) << "No shared memory buffers available, seqID=" << frag.sequence_id;
			return nullptr;
		}
		if (buffer == -2)
		{
			TLOG(TLVL_ERROR) << "Dropping fragment with sequence id " << frag.sequence_id << " and fragment id " << frag.fragment_id << " because data taking has already passed this event.";
		}
		else
		{
			TLOG(TLVL_INFO) << "Dropping fragment with sequence id " << frag.sequence_id << " and fragment id " << frag.fragment_id << " because there is no room in the queue and reliable mode is off.";
		}
		dropped_data_.emplace_back(frag, std::make_unique<Fragment>(frag.word_count - frag.num_words()));
		auto it = dropped_data_.rbegin();

		TLOG(TLVL_WRITEFRAGMENTHEADER) << "Dropping fragment with sequence id " << frag.sequence_id << " and fragment id " << frag.fragment_id << " into "
		                               << static_cast<void*>(it->second->dataBegin()) << " sz=" << it->second->dataSizeBytes();

		return it->second->dataBegin();
	}

	last_backpressure_report_time_ = std::chrono::steady_clock::now();
	last_fragment_header_write_time_ = std::chrono::steady_clock::now();
	// Increment this as soon as we know we want to use the buffer
	buffer_writes_pending_[buffer]++;

	if (metricMan)
	{
		metricMan->sendMetric("Input Fragment Rate", 1, "Fragments/s", 1, MetricMode::Rate);
	}

	TLOG(TLVL_BUFLCK) << "WriteFragmentHeader: obtaining buffer_mutexes lock for buffer " << buffer << ", seqID=" << frag.sequence_id;
	;

	std::unique_lock<std::mutex> lk(buffer_mutexes_.at(buffer));

	TLOG(TLVL_BUFLCK) << "WriteFragmentHeader: obtained buffer_mutexes lock for buffer " << buffer << ", seqID=" << frag.sequence_id;

	auto hdrpos = reinterpret_cast<RawDataType*>(GetWritePos(buffer));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	Write(buffer, &frag, frag.num_words() * sizeof(RawDataType));

	auto pos = reinterpret_cast<RawDataType*>(GetWritePos(buffer));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	if (frag.word_count - frag.num_words() > 0)
	{
		auto sts = IncrementWritePos(buffer, (frag.word_count - frag.num_words()) * sizeof(RawDataType));

		if (!sts)
		{
			reinterpret_cast<detail::RawFragmentHeader*>(hdrpos)->word_count = frag.num_words();       // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
			reinterpret_cast<detail::RawFragmentHeader*>(hdrpos)->type = Fragment::ErrorFragmentType;  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
			TLOG(TLVL_ERROR) << "Dropping over-size fragment with sequence id " << frag.sequence_id << " and fragment id " << frag.fragment_id << " because there is no room in the current buffer for this Fragment! (Keeping header)";
			dropped_data_.emplace_back(frag, std::make_unique<Fragment>(frag.word_count - frag.num_words()));
			auto it = dropped_data_.rbegin();

			oversize_fragment_count_++;

			if (maximum_oversize_fragment_count_ > 0 && oversize_fragment_count_ >= maximum_oversize_fragment_count_)
			{
				lk.unlock();
				TLOG(TLVL_ERROR) << "Too many over-size Fragments received! Please adjust max_event_size_bytes or max_fragment_size_bytes!";
				throw cet::exception("Too many over-size Fragments received! Please adjust max_event_size_bytes or max_fragment_size_bytes!");
			}

			TLOG(TLVL_WRITEFRAGMENTHEADER) << "Dropping over-size fragment with sequence id " << frag.sequence_id << " and fragment id " << frag.fragment_id
			                               << " into " << static_cast<void*>(it->second->dataBegin());
			return it->second->dataBegin();
		}
	}
	TLOG(TLVL_WRITEFRAGMENTHEADER) << "WriteFragmentHeader END, seqID=" << frag.sequence_id;
	return pos;
}

void artdaq::SharedMemoryEventManager::DoneWritingFragment(detail::RawFragmentHeader frag)
{
	TLOG(TLVL_DONEWRITINGFRAGMENT) << "DoneWritingFragment BEGIN";

	auto buffer = getBufferForSequenceID_(frag.sequence_id, false, frag.timestamp);
	if (buffer < 0)
	{
		for (auto it = dropped_data_.begin(); it != dropped_data_.end(); ++it)
		{
			if (frag.operator==(it->first))  // TODO, ELF 5/26/2023: Workaround until artdaq_core can be fixed for C++20
			{
				dropped_data_.erase(it);
				return;
			}
		}
		if (buffer == -1)
		{
			Detach(true, app_name + "SharedMemoryEventManager",
			       "getBufferForSequenceID_ returned -1 in DoneWritingFragment. This indicates a possible mismatch between expected Fragment count and the actual number of Fragments received.");
		}
		return;
	}

	if (!frag.valid)
	{
		UpdateFragmentHeader(buffer, frag);
	}

	statsHelper_.addSample(FRAGMENTS_RECEIVED_STAT_KEY, frag.word_count * sizeof(RawDataType));
	{
		TLOG(TLVL_BUFLCK) << "DoneWritingFragment: obtaining buffer_mutexes lock for buffer " << buffer;

		std::unique_lock<std::mutex> lk(buffer_mutexes_.at(buffer));

		TLOG(TLVL_BUFLCK) << "DoneWritingFragment: obtained buffer_mutexes lock for buffer " << buffer;

		TLOG(TLVL_DONEWRITINGFRAGMENT) << "DoneWritingFragment: Received Fragment with sequence ID " << frag.sequence_id << " and fragment id " << frag.fragment_id << " (type " << static_cast<int>(frag.type) << ")";
		auto hdr = getEventHeader_(buffer);
		hdr->run_id = run_id_;
		hdr->subrun_id = GetSubrunForSequenceID(frag.sequence_id);
		hdr->event_id = GetEventIDForFragment(frag.sequence_id, frag.timestamp);

		TLOG(TLVL_DONEWRITINGFRAGMENT) << "DoneWritingFragment: Updating buffer touch time";
		TouchBuffer(buffer);

		if (buffer_writes_pending_[buffer] > 1)
		{
			TLOG(TLVL_DONEWRITINGFRAGMENT) << "Done writing fragment, but there's another writer. Not doing bookkeeping steps.";
			buffer_writes_pending_[buffer]--;
			return;
		}
		TLOG(TLVL_DONEWRITINGFRAGMENT) << "Done writing fragment, and no other writer. Doing bookkeeping steps.";
		auto frag_count = GetFragmentCount(frag.sequence_id);
		hdr->is_complete = frag_count >= num_fragments_per_event_;

		if (frag_count > num_fragments_per_event_)
		{
			TLOG(TLVL_WARNING) << "DoneWritingFragment: This Event has more Fragments ( " << frag_count << " ) than specified in configuration ( " << num_fragments_per_event_ << " )!"
			                   << " This is probably due to a misconfiguration and is *not* a reliable mode!";
		}

		TLOG(TLVL_DONEWRITINGFRAGMENT) << "DoneWritingFragment: Received Fragment with sequence ID " << frag.sequence_id << " and fragment id " << frag.fragment_id << ", count/expected = " << frag_count << "/" << num_fragments_per_event_;
#if ART_SUPPORTS_DUPLICATE_EVENTS
		if (!hdr->is_complete && released_incomplete_events_.count(frag.sequence_id))
		{
			hdr->is_complete = frag_count >= released_incomplete_events_[frag.sequence_id] && buffer_writes_pending_[buffer] == 0;
		}
#endif

		complete_buffer_(buffer);

		// Move this down here to avoid race condition
		buffer_writes_pending_[buffer]--;
	}
	TLOG(TLVL_DONEWRITINGFRAGMENT) << "DoneWritingFragment END";
}

size_t artdaq::SharedMemoryEventManager::GetFragmentCount(Fragment::sequence_id_t seqID, Fragment::type_t type)
{
	return GetFragmentCountInBuffer(getBufferForSequenceID_(seqID, false), type);
}

size_t artdaq::SharedMemoryEventManager::GetFragmentCountInBuffer(int buffer, Fragment::type_t type)
{
	if (buffer < 0)
	{
		return 0;
	}
	ResetReadPos(buffer);
	IncrementReadPos(buffer, sizeof(detail::RawEventHeader));

	size_t count = 0;

	while (MoreDataInBuffer(buffer))
	{
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(buffer));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
		IncrementReadPos(buffer, fragHdr->word_count * sizeof(RawDataType));
		if (type != Fragment::InvalidFragmentType && fragHdr->type != type)
		{
			// Skip fragments with the wrong type, as they were over-size and truncated to the header
			continue;
		}
		TLOG(TLVL_GETFRAGMENTCOUNT) << "Adding Fragment with size=" << fragHdr->word_count << " to Fragment count";
		++count;
	}

	return count;
}

void artdaq::SharedMemoryEventManager::UpdateFragmentHeader(int buffer, artdaq::detail::RawFragmentHeader hdr)
{
	if (buffer < 0)
	{
		return;
	}
	ResetReadPos(buffer);
	IncrementReadPos(buffer, sizeof(detail::RawEventHeader));

	while (MoreDataInBuffer(buffer))
	{
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(buffer));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
		if (hdr.fragment_id == fragHdr->fragment_id)
		{
			*fragHdr = hdr;
			break;
		}
	}

	return;
}

void artdaq::SharedMemoryEventManager::RunArt(size_t process_index, const std::shared_ptr<std::atomic<pid_t>>& pid_out)
{
	do
	{
		auto start_time = std::chrono::steady_clock::now();
		send_init_frags_();
		TLOG(TLVL_INFO) << "Starting art process with config file " << current_art_config_file_->getFileName();

		pid_t pid = 0;
		bool piped_output = true;
		int stdoutpipefd[2];
		int stderrpipefd[2];

		if (!manual_art_)
		{
			if (pipe(stdoutpipefd) == -1)
			{
				TLOG(TLVL_ERROR) << "Error creating pipe for art process stdout: " << errno << " (" << strerror(errno) << ").";
				piped_output = false;
			}
			if (piped_output && pipe(stderrpipefd) == -1)
			{
				TLOG(TLVL_ERROR) << "Error creating pipe for art process stderr: " << errno << " (" << strerror(errno) << ").";
				close(stdoutpipefd[0]);
				close(stdoutpipefd[1]);
				piped_output = false;
			}
			pid = fork();
			if (pid == 0)
			{ /* child */
				if (piped_output)
				{
					close(stdoutpipefd[0]);                // Close read end of stdout pipe
					close(stderrpipefd[0]);                // Close read end of stderr pipe
					dup2(stdoutpipefd[1], STDOUT_FILENO);  // Redirect stdout to pipe
					dup2(stderrpipefd[1], STDERR_FILENO);  // Redirect stderr to pipe
				}

				// 23-May-2018, KAB: added the setting of the partition number env var
				// in the environment of the child art process so that Globals.hh
				// will pick it up there and provide it to the artdaq classes that
				// are used in data transfers, etc. within the art process.
				std::string envVarKey = "ARTDAQ_PARTITION_NUMBER";
				std::string envVarValue = std::to_string(Globals::GetPartitionNumber());
				if (setenv(envVarKey.c_str(), envVarValue.c_str(), 1) != 0)
				{
					TLOG(TLVL_ERROR) << "Error setting environment variable \"" << envVarKey
					                 << "\" in the environment of a child art process. "
					                 << "This may result in incorrect TCP port number "
					                 << "assignments or other issues, and data may "
					                 << "not flow through the system correctly.";
				}
				envVarKey = "ARTDAQ_APPLICATION_NAME";
				envVarValue = app_name;
				if (setenv(envVarKey.c_str(), envVarValue.c_str(), 1) != 0)
				{
					TLOG(TLVL_RUNART) << "Error setting environment variable \"" << envVarKey
					                  << "\" in the environment of a child art process. ";
				}
				envVarKey = "ARTDAQ_RANK";
				envVarValue = std::to_string(my_rank);
				if (setenv(envVarKey.c_str(), envVarValue.c_str(), 1) != 0)
				{
					TLOG(TLVL_RUNART) << "Error setting environment variable \"" << envVarKey
					                  << "\" in the environment of a child art process. ";
				}

				TLOG(TLVL_RUNART_2) << "Parsing art command line";
				auto args = parse_art_command_line_(current_art_config_file_, process_index);

				TLOG(TLVL_RUNART_2) << "Calling execvp with application name " << args[0];
				execvp(args[0], &args[0]);

				TLOG(TLVL_RUNART_2) << "Application exited, cleaning up";
				for (auto& arg : args)
				{
					delete[] arg;
				}

				exit(1);
			}
		}
		else
		{
			// Using cin/cout here to ensure console is active (artdaqDriver)
			std::cout << "Please run the following command in a separate terminal:" << std::endl
			          << "art -c " << current_art_config_file_->getFileName() << std::endl
			          << "Then, in a third terminal, execute: \"ps aux|grep [a]rt -c " << current_art_config_file_->getFileName() << "\" and note the PID of the art process." << std::endl
			          << "Finally, return to this window and enter the pid: " << std::endl;
			std::cin >> pid;
		}
		*pid_out = pid;

		TLOG(TLVL_INFO) << "PID of new art process is " << pid;
		{
			std::unique_lock<std::mutex> lk(art_process_mutex_);
			art_processes_.insert(pid);
		}
		siginfo_t status;
		auto sts = 0;
		if (!manual_art_)
		{
			// Read output from art process and report it to TRACE
			if (piped_output)
			{
				close(stdoutpipefd[1]);  // Close write end of stdout pipe
				close(stderrpipefd[1]);  // Close write end of stderr pipe

				std::string art_tname = app_name + "_art_stdout";
				char buf[PIPE_BUF];
				struct pollfd fds[2];
				fds[0].fd = stdoutpipefd[0];
				fds[0].events = POLLIN;
				fds[1].fd = stderrpipefd[0];
				fds[1].events = POLLIN;

				do
				{
					sts = waitid(P_PID, pid, &status, WEXITED | WNOHANG);
					poll(fds, 2, 1000);
					if (fds[0].revents & POLLIN)
					{
						ssize_t count = read(stdoutpipefd[0], buf, sizeof(buf) - 1);
						if (count > 0 && capture_art_stdout_)
						{
							TLOG(TLVL_INFO, art_tname) << "art[" << pid << "]" << std::string(buf, count);
						}
					}
					if (fds[1].revents & POLLIN)
					{
						ssize_t count = read(stderrpipefd[0], buf, sizeof(buf) - 1);
						if (count > 0 && capture_art_stderr_)
						{
							TLOG(TLVL_ERROR, art_tname) << "art[" << pid << "]" << std::string(buf, count);
						}
					}
				} while (status.si_code != CLD_DUMPED && status.si_code != CLD_KILLED && status.si_code != CLD_EXITED && sts == 0);
			}
			else
			{
				sts = waitid(P_PID, pid, &status, WEXITED);
			}
		}
		else
		{
			while (kill(pid, 0) >= 0) usleep(10000);

			TLOG(TLVL_INFO) << "Faking good exit status, please see art process for actual exit status!";
			status.si_code = CLD_EXITED;
			status.si_status = 0;
		}
		TLOG(TLVL_INFO) << "Removing PID " << pid << " from process list";
		{
			std::unique_lock<std::mutex> lk(art_process_mutex_);
			art_processes_.erase(pid);
		}
		if (sts < 0)
		{
			TLOG(TLVL_WARNING) << "Error occurred in waitid for art process " << pid << ": " << errno << " (" << strerror(errno) << ").";
		}
		else if (status.si_code == CLD_EXITED && status.si_status == 0)
		{
			TLOG(TLVL_INFO) << "art process " << pid << " exited normally, " << (restart_art_ ? "restarting" : "not restarting");
		}
		else
		{
			auto art_lifetime = TimeUtils::GetElapsedTime(start_time);
			if (art_lifetime < minimum_art_lifetime_s_)
			{
				restart_art_ = false;
			}

			auto exit_type = "exited with status code";
			switch (status.si_code)
			{
				case CLD_DUMPED:
				case CLD_KILLED:
					exit_type = "was killed with signal";
					break;
				case CLD_EXITED:
				default:
					break;
			}

			TLOG((restart_art_ ? TLVL_WARNING : TLVL_ERROR))
			    << "art process " << pid << " " << exit_type << " " << status.si_status
			    << (status.si_code == CLD_DUMPED ? " (core dumped)" : "")
			    << " after running for " << std::setprecision(2) << std::fixed << art_lifetime << " seconds, "
			    << (restart_art_ ? "restarting" : "not restarting");
		}
	} while (restart_art_);
}

bool artdaq::SharedMemoryEventManager::StartArt()
{
	size_t initialCount = GetAttachedCount();
	restart_art_ = always_restart_art_;
	if (num_art_processes_ == 0)
	{
		return true;
	}
	for (size_t ii = 0; ii < num_art_processes_; ++ii)
	{
		StartArtProcess(current_art_pset_, ii);
	}
	auto startTime = std::chrono::steady_clock::now();
	while (GetAttachedCount() - initialCount != num_art_processes_)
	{
		TLOG(TLVL_INFO) << "Waiting for all art processes to connect to shared memory, " << TimeUtils::GetElapsedTime(startTime) << " s elapsed.";
		std::this_thread::sleep_for(std::chrono::seconds(1));
		if (!restart_art_)
		{
			TLOG(TLVL_ERROR) << "Error occurred while starting art processes, aborting. Check PMT log for error messages.";
			return false;
		}
	}
	return true;
}

pid_t artdaq::SharedMemoryEventManager::StartArtProcess(fhicl::ParameterSet pset, size_t process_index)
{
	static std::mutex start_art_mutex;
	std::unique_lock<std::mutex> lk(start_art_mutex);
	// TraceLock lk(start_art_mutex, 15, "StartArtLock");
	restart_art_ = always_restart_art_;
	auto initialCount = GetAttachedCount();
	auto startTime = std::chrono::steady_clock::now();

	if (pset != current_art_pset_ || !current_art_config_file_)
	{
		current_art_pset_ = pset;
		if (manual_art_)
			current_art_config_file_ = std::make_shared<art_config_file>(pset, GetKey(), GetBroadcastKey());
		else
			current_art_config_file_ = std::make_shared<art_config_file>(pset);
	}
	std::shared_ptr<std::atomic<pid_t>> pid(new std::atomic<pid_t>(-1));
	boost::thread thread([this, process_index, pid] { RunArt(process_index, pid); });
	thread.detach();

	auto currentCount = GetAttachedCount() - initialCount;
	while ((currentCount < 1 || *pid <= 0) && (TimeUtils::GetElapsedTime(startTime) < 5 || manual_art_))
	{
		usleep(10000);
		currentCount = GetAttachedCount() - initialCount;
	}
	if ((currentCount < 1 || *pid <= 0) && manual_art_)
	{
		TLOG(TLVL_WARNING) << "Manually-started art process has not connected to shared memory or has bad PID: connected:" << currentCount << ", PID:" << pid;
		return 0;
	}
	if (currentCount < 1 || *pid <= 0)
	{
		TLOG(TLVL_WARNING) << "art process has not started after 5s. Check art configuration!"
		                   << " (pid=" << *pid << ", attachedCount=" << currentCount << ")";
		return 0;
	}

	TLOG(TLVL_INFO) << std::setw(4) << std::fixed << "art initialization took "
	                << TimeUtils::GetElapsedTime(startTime) << " seconds.";

	return *pid;
}

void artdaq::SharedMemoryEventManager::ShutdownArtProcesses(std::set<pid_t>& pids)
{
	restart_art_ = false;
	// current_art_config_file_ = nullptr;
	// current_art_pset_ = fhicl::ParameterSet();

	auto check_pids = [&](bool print) {
		std::unique_lock<std::mutex> lk(art_process_mutex_);
		for (auto pid = pids.begin(); pid != pids.end();)
		{
			// 08-May-2018, KAB: protect against killing invalid PIDS

			if (*pid <= 0)
			{
				TLOG(TLVL_WARNING) << "Removing an invalid PID (" << *pid
				                   << ") from the shutdown list.";
				pid = pids.erase(pid);
			}
			else if (kill(*pid, 0) < 0)
			{
				pid = pids.erase(pid);
			}
			else
			{
				if (print)
				{
					std::cout << *pid << " ";
				}
				++pid;
			}
		}
	};
	auto count_pids = [&]() {
		std::unique_lock<std::mutex> lk(art_process_mutex_);
		return pids.size();
	};
	check_pids(false);
	if (count_pids() == 0)
	{
		TLOG(TLVL_SHUTDOWN) << "All art processes already exited, nothing to do.";
		usleep(1000);
		return;
	}

	if (!manual_art_)
	{
		int graceful_wait_ms = art_event_processing_time_us_ * size() * 10 / 1000;
		int gentle_wait_ms = art_event_processing_time_us_ * size() * 2 / 1000;
		int int_wait_ms = art_event_processing_time_us_ * size() / 1000;
		auto shutdown_start = std::chrono::steady_clock::now();

		//		if (!overwrite_mode_)
		{
			TLOG(TLVL_SHUTDOWN) << "Waiting up to " << graceful_wait_ms << " ms for all art processes to exit gracefully";
			for (int ii = 0; ii < graceful_wait_ms; ++ii)
			{
				usleep(1000);

				check_pids(false);
				if (count_pids() == 0)
				{
					TLOG(TLVL_INFO) << "All art processes exited after " << TimeUtils::GetElapsedTimeMilliseconds(shutdown_start) << " ms.";
					return;
				}
			}
		}

		{
			TLOG(TLVL_SHUTDOWN) << "Gently informing art processes that it is time to shut down";
			std::unique_lock<std::mutex> lk(art_process_mutex_);
			for (auto pid : pids)
			{
				TLOG(TLVL_SHUTDOWN) << "Sending SIGQUIT to pid " << pid;
				kill(pid, SIGQUIT);
			}
		}

		TLOG(TLVL_SHUTDOWN) << "Waiting up to " << gentle_wait_ms << " ms for all art processes to exit from SIGQUIT";
		for (int ii = 0; ii < gentle_wait_ms; ++ii)
		{
			usleep(1000);

			check_pids(false);
			if (count_pids() == 0)
			{
				TLOG(TLVL_INFO) << "All art processes exited after " << TimeUtils::GetElapsedTimeMilliseconds(shutdown_start) << " ms (SIGQUIT).";
				return;
			}
		}

		{
			TLOG(TLVL_SHUTDOWN) << "Insisting that the art processes shut down";
			std::unique_lock<std::mutex> lk(art_process_mutex_);
			for (auto pid : pids)
			{
				kill(pid, SIGINT);
			}
		}

		TLOG(TLVL_SHUTDOWN) << "Waiting up to " << int_wait_ms << " ms for all art processes to exit from SIGINT";
		for (int ii = 0; ii < int_wait_ms; ++ii)
		{
			usleep(1000);

			check_pids(false);

			if (count_pids() == 0)
			{
				TLOG(TLVL_INFO) << "All art processes exited after " << TimeUtils::GetElapsedTimeMilliseconds(shutdown_start) << " ms (SIGINT).";
				return;
			}
		}

		TLOG(TLVL_SHUTDOWN) << "Killing remaning art processes with extreme prejudice";
		while (count_pids() > 0)
		{
			{
				std::unique_lock<std::mutex> lk(art_process_mutex_);
				kill(*pids.begin(), SIGKILL);
				usleep(1000);
			}
			check_pids(false);
		}
		TLOG(TLVL_INFO) << "All art processes exited after " << TimeUtils::GetElapsedTimeMilliseconds(shutdown_start) << " ms (SIGKILL).";
	}
	else
	{
		std::cout << "Please shut down all art processes, then hit return/enter" << std::endl;
		while (count_pids() > 0)
		{
			std::cout << "The following PIDs are running: ";
			check_pids(true);
			std::cout << std::endl;
			usleep(500000);
		}
	}
}

void artdaq::SharedMemoryEventManager::ReconfigureArt(fhicl::ParameterSet art_pset, run_id_t newRun, int n_art_processes)
{
	TLOG(TLVL_RECONFIGUREART) << "ReconfigureArt BEGIN";
	if (restart_art_ || !always_restart_art_)  // Art is running
	{
		endOfData();
	}
	for (size_t ii = 0; ii < broadcasts_.size(); ++ii)
	{
		broadcasts_.MarkBufferEmpty(ii, true);
	}
	if (newRun == 0)
	{
		newRun = run_id_ + 1;
	}

	if (art_pset != current_art_pset_ || !current_art_config_file_)
	{
		current_art_pset_ = art_pset;
		if (manual_art_)
			current_art_config_file_ = std::make_shared<art_config_file>(art_pset, GetKey(), GetBroadcastKey());
		else
			current_art_config_file_ = std::make_shared<art_config_file>(art_pset);
	}

	if (n_art_processes != -1)
	{
		TLOG(TLVL_INFO) << "Setting number of art processes to " << n_art_processes;
		num_art_processes_ = n_art_processes;
	}
	startRun(newRun);
	TLOG(TLVL_RECONFIGUREART) << "ReconfigureArt END";
}

bool artdaq::SharedMemoryEventManager::endOfData()
{
	running_ = false;
	{
		std::lock_guard<std::mutex> lk(init_fragments_mutex_);
		init_fragment_map_.clear();
	}
	init_frags_sent_ = false;
	TLOG(TLVL_ENDOFDATA) << "SharedMemoryEventManager::endOfData";
	restart_art_ = false;

	auto start = std::chrono::steady_clock::now();
	auto pendingWriteCount = std::accumulate(buffer_writes_pending_.begin(), buffer_writes_pending_.end(), 0, [](int a, auto& b) { return a + b.second.load(); });
	TLOG(TLVL_ENDOFDATA) << "endOfData: Waiting for " << pendingWriteCount << " pending writes to complete";
	while (pendingWriteCount > 0 && TimeUtils::GetElapsedTimeMicroseconds(start) < 1000000)
	{
		usleep(10000);
		pendingWriteCount = std::accumulate(buffer_writes_pending_.begin(), buffer_writes_pending_.end(), 0, [](int a, auto& b) { return a + b.second.load(); });
	}

	size_t initialStoreSize = GetOpenEventCount();
	TLOG(TLVL_ENDOFDATA) << "endOfData: Flushing " << initialStoreSize
	                     << " stale events from the SharedMemoryEventManager.";
	int counter = initialStoreSize;
	while (!active_buffers_.empty() && counter > 0)
	{
		complete_buffer_(*active_buffers_.begin());
		counter--;
	}
	TLOG(TLVL_ENDOFDATA) << "endOfData: Done flushing, there are now " << GetOpenEventCount()
	                     << " stale events in the SharedMemoryEventManager.";

	size_t incomplete_at_shutdown = active_buffers_.size();
	size_t force_flushed_count = 0;

	// Grace period: wait up to one stale_buffer_timeout for late fragments to arrive and complete events
	if (!active_buffers_.empty())
	{
		auto grace_timeout_us = GetBufferTimeout();
		TLOG(TLVL_ENDOFDATA) << "endOfData: Waiting up to " << (grace_timeout_us / 1000000.0)
		                     << " s for " << active_buffers_.size() << " incomplete events to finish";
		start = std::chrono::steady_clock::now();
		while (!active_buffers_.empty() && TimeUtils::GetElapsedTimeMicroseconds(start) < grace_timeout_us)
		{
			usleep(10000);
			counter = active_buffers_.size();
			while (!active_buffers_.empty() && counter > 0)
			{
				complete_buffer_(*active_buffers_.begin());
				counter--;
			}
		}

		// Force-flush any remaining incomplete events
		if (!active_buffers_.empty())
		{
			TLOG(TLVL_ENDOFDATA) << "endOfData: Force-flushing " << active_buffers_.size() << " incomplete events after grace period";
			std::unique_lock<std::mutex> lk(sequence_id_mutex_);
			auto buffers_to_flush = active_buffers_;
			for (auto buf : buffers_to_flush)
			{
				if (buffer_writes_pending_[buf].load() != 0)
				{
					continue;
				}
				auto hdr = getEventHeader_(buf);
				if (requests_)
				{
					requests_->RemoveRequest(hdr->sequence_id);
				}
				active_buffers_.erase(buf);
				pending_buffers_.insert(buf);
				run_incomplete_event_count_++;
				force_flushed_count++;
				if (released_incomplete_events_.count(hdr->sequence_id) == 0u)
				{
					released_incomplete_events_[hdr->sequence_id] = num_fragments_per_event_ - GetFragmentCountInBuffer(buf);
				}
				else
				{
					released_incomplete_events_[hdr->sequence_id] -= GetFragmentCountInBuffer(buf);
				}
				TLOG(TLVL_WARNING) << "endOfData: Event " << hdr->sequence_id
				                   << " is incomplete (missing " << released_incomplete_events_[hdr->sequence_id]
				                   << " Fragments). Force-releasing to art.";
			}
			check_pending_buffers_(lk);
		}
	}

	if (incomplete_at_shutdown == 0)
	{
		TLOG(TLVL_INFO) << "endOfData summary: All events were complete at shutdown.";
	}
	else if (force_flushed_count == 0)
	{
		TLOG(TLVL_INFO) << "endOfData summary: " << incomplete_at_shutdown << " incomplete events all completed during grace period.";
	}
	else
	{
		TLOG(TLVL_INFO) << "endOfData summary: " << force_flushed_count << " of " << incomplete_at_shutdown
		                << " incomplete events were force-flushed after grace period.";
	}

	TLOG(TLVL_ENDOFDATA) << "Waiting for " << (ReadReadyCount() + (size() - WriteReadyCount(overwrite_mode_))) << " outstanding buffers...";
	start = std::chrono::steady_clock::now();
	auto lastReadCount = ReadReadyCount() + (size() - WriteReadyCount(overwrite_mode_));
	auto end_of_data_wait_us = art_event_processing_time_us_ * (lastReadCount > 0 ? lastReadCount : 1);  // size();

	auto outstanding_buffer_wait_time = art_event_processing_time_us_ > 100000 ? 100000 : art_event_processing_time_us_;

	// We will wait until no buffer has been read for the end of data wait seconds, or no art processes are left.
	while (lastReadCount > 0 && (end_of_data_wait_us == 0 || TimeUtils::GetElapsedTimeMicroseconds(start) < end_of_data_wait_us) && get_art_process_count_() > 0)
	{
		auto temp = ReadReadyCount() + (size() - WriteReadyCount(overwrite_mode_));
		if (temp != lastReadCount)
		{
			TLOG(TLVL_ENDOFDATA_2) << "Waiting for " << temp << " outstanding buffers...";
			lastReadCount = temp;
			start = std::chrono::steady_clock::now();
		}
		if (lastReadCount > 0)
		{
			TLOG(TLVL_ENDOFDATA_2) << "About to sleep " << outstanding_buffer_wait_time << " us - lastReadCount=" << lastReadCount << " size=" << size() << " end_of_data_wait_us=" << end_of_data_wait_us;
			usleep(outstanding_buffer_wait_time);
		}
	}

	TLOG(TLVL_ENDOFDATA) << "endOfData: After wait for outstanding buffers. Still outstanding: " << lastReadCount << ", time waited: "
	                     << TimeUtils::GetElapsedTime(start) << " s / " << (end_of_data_wait_us / 1000000.0) << " s, art process count: " << get_art_process_count_();

	TLOG(TLVL_ENDOFDATA) << "endOfData: Broadcasting EndOfData Fragment";
	FragmentPtrs broadcast;
	broadcast.emplace_back(Fragment::eodFrag(GetBufferCount()));
	bool success = broadcastFragments_(broadcast);
	if (!success)
	{
		TLOG(TLVL_ENDOFDATA) << "endOfData: Clearing buffers to make room for EndOfData Fragment";
		for (size_t ii = 0; ii < broadcasts_.size(); ++ii)
		{
			broadcasts_.MarkBufferEmpty(ii, true);
		}
		broadcastFragments_(broadcast);
	}
	auto endOfDataProcessingStart = std::chrono::steady_clock::now();
	while (get_art_process_count_() > 0)
	{
		TLOG(TLVL_ENDOFDATA) << "There are " << get_art_process_count_() << " art processes remaining. Proceeding to shutdown.";

		ShutdownArtProcesses(art_processes_);
	}
	TLOG(TLVL_ENDOFDATA) << "It took " << TimeUtils::GetElapsedTime(endOfDataProcessingStart) << " s for all art processes to close after sending EndOfData Fragment";

	ResetAttachedCount();

	TLOG(TLVL_ENDOFDATA) << "endOfData: Clearing buffers";
	for (size_t ii = 0; ii < size(); ++ii)
	{
		MarkBufferEmpty(ii, true);
	}

	released_events_.clear();
	released_incomplete_events_.clear();

	TLOG(TLVL_ENDOFDATA) << "endOfData END";
	TLOG(TLVL_INFO) << "EndOfData Complete. There were " << GetLastSeenBufferID() << " buffers processed.";
	return true;
}

bool artdaq::SharedMemoryEventManager::startRun(run_id_t runID)
{
	running_ = true;
	{
		std::lock_guard<std::mutex> lk(init_fragments_mutex_);
		init_fragment_map_.clear();
	}
	init_frags_sent_ = false;
	statsHelper_.resetStatistics();
	TLOG(TLVL_STARTRUN) << "startRun: Clearing broadcast buffers";
	for (size_t ii = 0; ii < broadcasts_.size(); ++ii)
	{
		broadcasts_.MarkBufferEmpty(ii, true);
	}
	released_events_.clear();
	released_incomplete_events_.clear();
	// If we fail to start the art processes, abort Start
	if (!StartArt())
	{
		return false;
	}
	run_id_ = runID;
	subrun_id_ = 1;
	{
		std::unique_lock<std::mutex> lk(subrun_event_map_mutex_);
		subrun_event_map_.clear();
		subrun_event_map_[0] = 1;
	}
	run_event_count_ = 0;
	run_incomplete_event_count_ = 0;
	requests_ = std::make_unique<RequestSender>(data_pset_);
	if (requests_)
	{
		requests_->SetRunNumber(static_cast<uint32_t>(run_id_));
	}
	if (data_pset_.has_key("routing_token_config"))
	{
		auto rmPset = data_pset_.get<fhicl::ParameterSet>("routing_token_config");
		if (rmPset.get<bool>("use_routing_manager", false))
		{
			tokens_ = std::make_unique<TokenSender>(rmPset);
			tokens_->SetRunNumber(static_cast<uint32_t>(run_id_));
			tokens_->SendRoutingToken(queue_size_, run_id_);
		}
	}
	TLOG(TLVL_STARTRUN) << "Starting run " << run_id_
	                    << ", max queue size = "
	                    << queue_size_
	                    << ", queue size = "
	                    << GetLockedBufferCount();
	if (metricMan)
	{
		metricMan->sendMetric("Run Number", static_cast<uint64_t>(run_id_), "Run", 1, MetricMode::LastPoint | MetricMode::Persist);
	}
	return true;
}

bool artdaq::SharedMemoryEventManager::endRun()
{
	TLOG(TLVL_INFO) << "Ending run " << run_id_;
	TLOG(TLVL_ENDRUN) << "Shutting down RequestSender";
	requests_.reset(nullptr);
	TLOG(TLVL_ENDRUN) << "Shutting down TokenSender";
	tokens_.reset(nullptr);

	TLOG(TLVL_INFO) << "Run " << run_id_ << " has ended. There were " << run_event_count_ << " events in this run.";
	run_event_count_ = 0;
	run_incomplete_event_count_ = 0;
	oversize_fragment_count_ = 0;
	{
		std::unique_lock<std::mutex> lk(subrun_event_map_mutex_);
		subrun_event_map_.clear();
		subrun_event_map_[0] = 1;
	}
	return true;
}

void artdaq::SharedMemoryEventManager::rolloverSubrun(sequence_id_t boundary, subrun_id_t subrun, bool sendFragment)
{
	// Generated EndOfSubrun Fragments have Sequence ID 0 and should be ignored
	if (boundary == 0 || boundary == Fragment::InvalidSequenceID)
	{
		return;
	}

	std::unique_lock<std::mutex> lk(subrun_event_map_mutex_);

	// Don't re-rollover to an already-defined subrun
	if (!subrun_event_map_.empty() && subrun_event_map_.rbegin()->second >= subrun)
	{
		return;
	}
	TLOG(TLVL_INFO) << "Will roll over to subrun " << subrun << " when I reach Sequence ID " << (boundary + 1);
	subrun_event_map_[boundary + 1] = subrun;
	while (subrun_event_map_.size() > max_subrun_event_map_length_)
	{
		subrun_event_map_.erase(subrun_event_map_.begin());
	}

	if (sendFragment)
	{
		auto endOfSubrunFrag = artdaq::MetadataFragment::CreateEndOfSubrunFragment(my_rank, boundary, subrun, 0);
		BroadcastFragment(endOfSubrunFrag);
	}
}

void artdaq::SharedMemoryEventManager::rolloverSubrun(bool sendFragment)
{
	Fragment::sequence_id_t seqID = 0;
	subrun_id_t subrun = 0;
	{
		std::unique_lock<std::mutex> lk(subrun_event_map_mutex_);
		for (auto& it : subrun_event_map_)
		{
			if (it.first >= seqID)
			{
				seqID = it.first + 1;
			}
			if (it.second >= subrun)
			{
				subrun = it.second + 1;
			}
		}
	}
	rolloverSubrun(seqID, subrun, sendFragment);
}

void artdaq::SharedMemoryEventManager::sendMetrics()
{
	if (metricMan)
	{
		metricMan->sendMetric("Open Event Count", GetOpenEventCount(), "events", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Pending Event Count", GetPendingEventCount(), "events", 1, MetricMode::LastPoint);
	}

	if (open_event_report_interval_ms_ > 0 && GetLockedBufferCount() != 0u)
	{
		if (TimeUtils::GetElapsedTimeMilliseconds(last_open_event_report_time_) < static_cast<size_t>(open_event_report_interval_ms_))
		{
			return;
		}

		last_open_event_report_time_ = std::chrono::steady_clock::now();
		std::ostringstream oss;
		oss << "Open Events (expecting " << num_fragments_per_event_ << " Fragments): ";
		for (auto& ev : active_buffers_)
		{
			auto hdr = getEventHeader_(ev);
			oss << hdr->sequence_id << " (has " << GetFragmentCount(hdr->sequence_id) << " Fragments), ";
		}
		TLOG(TLVL_SENDMETRICS) << oss.str();
	}
}

artdaq::detail::RawEventHeader* artdaq::SharedMemoryEventManager::getEventHeader_(int buffer)
{
	return reinterpret_cast<detail::RawEventHeader*>(GetBufferStart(buffer));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

artdaq::SharedMemoryEventManager::subrun_id_t artdaq::SharedMemoryEventManager::GetSubrunForSequenceID(Fragment::sequence_id_t seqID)
{
	subrun_id_t subrun = 1;
	if (init_fragment_count_ > 0)
	{
		TLOG(TLVL_GETSUBRUN) << "init_fragment_count > 0 (processing art events): Decoding subrun from sequenceID " << seqID;
		subrun = seqID >> 32;
	}
	else
	{
		std::unique_lock<std::mutex> lk(subrun_event_map_mutex_);

		TLOG(TLVL_GETSUBRUN) << "GetSubrunForSequenceID BEGIN map size = " << subrun_event_map_.size();
		auto it = subrun_event_map_.begin();

		while (it->first <= seqID && it != subrun_event_map_.end())
		{
			TLOG(TLVL_GETSUBRUN) << "Map has sequence ID " << it->first << ", subrun " << it->second << " (looking for <= " << seqID << ")";
			subrun = it->second;
			++it;
		}
	}

	TLOG(TLVL_GETSUBRUN) << "GetSubrunForSequenceID returning subrun " << subrun << " for sequence ID " << seqID;
	return subrun;
}

artdaq::SharedMemoryEventManager::event_id_t artdaq::SharedMemoryEventManager::GetEventIDForFragment(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp)
{
	event_id_t event = 1;
	if (init_fragment_count_ > 0)
	{
		TLOG(TLVL_GETEVENTID) << "init_fragment_count > 0 (processing art events): Decoding event ID from sequenceID " << seqID;
		event = seqID & 0xFFFFFFFF;
	}
	else
	{
		sequence_id_t subrun_start = 0;
		if (reset_event_number_for_subruns_)
		{
			std::unique_lock<std::mutex> lk(subrun_event_map_mutex_);
			TLOG(TLVL_GETEVENTID) << "GetEventIDForFragment BEGIN map size = " << subrun_event_map_.size();
			auto it = subrun_event_map_.begin();
			while (it->first < seqID && it != subrun_event_map_.end())
			{
				TLOG(TLVL_GETEVENTID) << "Map has sequence ID " << it->first << ", event " << it->second << " (looking for <= " << seqID << ")";
				subrun_start = it->first;
				++it;
			}
		}

		event = use_sequence_id_for_event_number_ ? (seqID - subrun_start) : (timestamp - subrun_start);
	}
	TLOG(TLVL_GETEVENTID) << "GetEventIDForFragment returning event ID " << event << " for sequence ID " << seqID;
	return event;
}

int artdaq::SharedMemoryEventManager::getBufferForSequenceID_(Fragment::sequence_id_t seqID, bool create_new, Fragment::timestamp_t timestamp)
{
	TLOG(TLVL_GETBUFFER) << "getBufferForSequenceID " << seqID << " BEGIN";
	std::unique_lock<std::mutex> lk(sequence_id_mutex_);

	TLOG(TLVL_GETBUFFER) << "getBufferForSequenceID obtained sequence_id_mutex for seqid=" << seqID;

	auto buffers = GetBuffersOwnedByManager();
	for (auto& buf : buffers)
	{
		auto hdr = getEventHeader_(buf);
		if (hdr->sequence_id == seqID)
		{
			TLOG(TLVL_GETBUFFER) << "getBufferForSequenceID " << seqID << " returning " << buf;
			return buf;
		}
	}

#if !ART_SUPPORTS_DUPLICATE_EVENTS
	if (released_incomplete_events_.count(seqID) != 0u)
	{
		TLOG(TLVL_ERROR) << "Event " << seqID << " has already been marked \"Incomplete\" and sent to art!";
		return -2;
	}
	if (released_events_.count(seqID) != 0u)
	{
		TLOG(TLVL_ERROR) << "Event " << seqID << " has already been completed and released to art! Check configuration for inconsistent Fragment count per event!";
		return -2;
	}
#endif

	if (!create_new)
	{
		return -1;
	}

	check_pending_buffers_(lk);
	int new_buffer = GetBufferForWriting(false, shared_memory_ordering_ ? static_cast<size_t>(seqID) : 0);

	if (new_buffer == -1)
	{
		new_buffer = GetBufferForWriting(overwrite_mode_, shared_memory_ordering_ ? static_cast<size_t>(seqID) : 0);
	}

	if (new_buffer == -1)
	{
		return -1;
	}
	TLOG(TLVL_BUFLCK) << "getBufferForSequenceID_: obtaining buffer_mutexes lock for buffer " << new_buffer;
	std::unique_lock<std::mutex> buffer_lk(buffer_mutexes_.at(new_buffer));
	TLOG(TLVL_BUFLCK) << "getBufferForSequenceID_: obtained buffer_mutexes lock for buffer " << new_buffer;

	event_timing_[new_buffer] = std::chrono::steady_clock::now();

	auto hdr = getEventHeader_(new_buffer);
	hdr->is_complete = false;
	hdr->run_id = run_id_;
	hdr->subrun_id = GetSubrunForSequenceID(seqID);
	hdr->event_id = GetEventIDForFragment(seqID, timestamp);
	hdr->sequence_id = seqID;
	hdr->timestamp = timestamp;
	buffer_writes_pending_[new_buffer] = 0;
	IncrementWritePos(new_buffer, sizeof(detail::RawEventHeader));
	Globals::SetMFIteration("Sequence ID " + std::to_string(seqID));

	TLOG(TLVL_BUFFER) << "getBufferForSequenceID placing " << new_buffer << " to active.";
	active_buffers_.insert(new_buffer);
	TLOG(TLVL_BUFFER) << "Buffer occupancy now (total,full,reading,empty,pending,active)=("
	                  << size() << ","
	                  << ReadReadyCount() << ","
	                  << WriteReadyCount(true) - WriteReadyCount(false) - ReadReadyCount() << ","
	                  << WriteReadyCount(false) << ","
	                  << pending_buffers_.size() << ","
	                  << active_buffers_.size() << ")";

	if (requests_)
	{
		requests_->AddRequest(seqID, timestamp);
	}
	TLOG(TLVL_GETBUFFER) << "getBufferForSequenceID " << seqID << " returning newly initialized buffer " << new_buffer;
	return new_buffer;
}

bool artdaq::SharedMemoryEventManager::hasFragments_(int buffer)
{
	if (buffer == -1)
	{
		return true;
	}
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
	if (hdr != nullptr && hdr->is_complete)
	{
		TLOG(TLVL_COMPLETEBUFFER) << "complete_buffer_: This fragment completes event " << hdr->sequence_id << ".";

		{
			TLOG(TLVL_BUFFER) << "complete_buffer_ moving " << buffer << " from active to pending.";

			TLOG(TLVL_BUFLCK) << "complete_buffer_: obtaining sequence_id_mutex lock for seqid=" << hdr->sequence_id;
			std::unique_lock<std::mutex> lk(sequence_id_mutex_);
			TLOG(TLVL_BUFLCK) << "complete_buffer_: obtained sequence_id_mutex lock for seqid=" << hdr->sequence_id;
			active_buffers_.erase(buffer);
			pending_buffers_.insert(buffer);
			released_events_.insert(hdr->sequence_id);
			while (released_events_.size() > max_event_list_length_)
			{
				released_events_.erase(released_events_.begin());
			}

			TLOG(TLVL_BUFFER) << "Buffer occupancy now (total,full,reading,empty,pending,active)=("
			                  << size() << ","
			                  << ReadReadyCount() << ","
			                  << WriteReadyCount(true) - WriteReadyCount(false) - ReadReadyCount() << ","
			                  << WriteReadyCount(false) << ","
			                  << pending_buffers_.size() << ","
			                  << active_buffers_.size() << ")";
			check_pending_buffers_(lk);
		}
		if (requests_)
		{
			requests_->RemoveRequest(hdr->sequence_id);
		}
	}
	check_pending_broadcasts_();
}

bool artdaq::SharedMemoryEventManager::bufferComparator(int bufA, int bufB)
{
	return getEventHeader_(bufA) < getEventHeader_(bufB);
}

void artdaq::SharedMemoryEventManager::CheckPendingBuffers()
{
	{
		TLOG(TLVL_BUFLCK) << "Obtaining sequence_id_mutex_";
		std::unique_lock<std::mutex> lk(sequence_id_mutex_);
		TLOG(TLVL_BUFLCK) << "Obtained sequence_id_mutex_";

		check_pending_buffers_(lk);
	}
	check_pending_broadcasts_();
}

void artdaq::SharedMemoryEventManager::check_pending_buffers_(std::unique_lock<std::mutex> const& lock)
{
	TLOG(TLVL_CHECKPENDINGBUFFERS) << "check_pending_buffers_ BEGIN Locked=" << std::boolalpha << lock.owns_lock();

	auto buffers = GetBuffersOwnedByManager();
	for (auto buf : buffers)
	{
		if (ResetBuffer(buf) && (pending_buffers_.count(buf) == 0u))
		{
			TLOG(TLVL_CHECKPENDINGBUFFERS) << "check_pending_buffers_ Incomplete buffer detected, buf=" << buf << " active_bufers_.count(buf)=" << active_buffers_.count(buf) << " buffer_writes_pending_[buf]=" << buffer_writes_pending_[buf].load();
			auto hdr = getEventHeader_(buf);
			if ((active_buffers_.count(buf) != 0u) && buffer_writes_pending_[buf].load() == 0)
			{
				if (requests_)
				{
					requests_->RemoveRequest(hdr->sequence_id);
				}
				TLOG(TLVL_BUFFER) << "check_pending_buffers_ moving buffer " << buf << " from active to pending";
				active_buffers_.erase(buf);
				pending_buffers_.insert(buf);
				TLOG(TLVL_BUFFER) << "Buffer occupancy now (total,full,reading,empty,pending,active)=("
				                  << size() << ","
				                  << ReadReadyCount() << ","
				                  << WriteReadyCount(true) - WriteReadyCount(false) - ReadReadyCount() << ","
				                  << WriteReadyCount(false) << ","
				                  << pending_buffers_.size() << ","
				                  << active_buffers_.size() << ")";

				run_incomplete_event_count_++;
				if (metricMan)
				{
					metricMan->sendMetric("Incomplete Event Rate", 1, "events/s", 3, MetricMode::Rate);
				}
				if (released_incomplete_events_.count(hdr->sequence_id) == 0u)
				{
					released_incomplete_events_[hdr->sequence_id] = num_fragments_per_event_ - GetFragmentCountInBuffer(buf);
				}
				else
				{
					released_incomplete_events_[hdr->sequence_id] -= GetFragmentCountInBuffer(buf);
				}

				TLOG(TLVL_WARNING) << "Event " << hdr->sequence_id
				                   << " was opened " << TimeUtils::GetElapsedTime(event_timing_[buf]) << " s ago"
				                   << " and has timed out (missing " << released_incomplete_events_[hdr->sequence_id] << " Fragments)."
				                   << "Scheduling release to art.";
			}
		}
	}

	std::list<int> sorted_buffers(pending_buffers_.begin(), pending_buffers_.end());
	sorted_buffers.sort([this](int a, int b) { return bufferComparator(a, b); });

	auto available_buffers = WriteReadyCount(overwrite_mode_);
	auto counter = 0;
	double eventSize = 0;
	double eventTime = 0;
	for (auto buf : sorted_buffers)
	{
		auto hdr = getEventHeader_(buf);
		auto thisEventSize = BufferDataSize(buf);

		bool currentSubrun = hdr->subrun_id == subrun_id_;

		if (hdr->subrun_id > subrun_id_ && (available_buffers > 0 || TimeUtils::GetElapsedTime(last_event_time_) < subrun_transition_hold_time_s_))
		{
			TLOG(TLVL_CHECKPENDINGBUFFERS_4) << "Holding event " << std::to_string(hdr->sequence_id) << " (sr=" << hdr->subrun_id << ") in buffer " << buf << ", "
			                                 << "event_size=" << thisEventSize << ", buffer_size=" << BufferSize();
			continue;
		}

		TLOG(TLVL_CHECKPENDINGBUFFERS_4) << "Releasing event " << std::to_string(hdr->sequence_id) << " (sr=" << hdr->subrun_id << ") in buffer " << buf << " to art, "
		                                 << "event_size=" << thisEventSize << ", buffer_size=" << BufferSize();
		statsHelper_.addSample(EVENTS_RELEASED_STAT_KEY, thisEventSize);

		TLOG(TLVL_BUFFER) << "check_pending_buffers_ removing buffer " << buf << " moving from pending to full";
		MarkBufferFull(buf);
		run_event_count_++;
		counter++;
		eventSize += thisEventSize;
		eventTime += TimeUtils::GetElapsedTime(event_timing_[buf]);
		pending_buffers_.erase(buf);
		if (currentSubrun)
		{
			last_event_time_ = std::chrono::steady_clock::now();
		}
	}
	TLOG(TLVL_BUFFER) << "Buffer occupancy now (total,full,reading,empty,pending,active)=("
	                  << size() << ","
	                  << ReadReadyCount() << ","
	                  << WriteReadyCount(true) - WriteReadyCount(false) - ReadReadyCount() << ","
	                  << WriteReadyCount(false) << ","
	                  << pending_buffers_.size() << ","
	                  << active_buffers_.size() << ")";

	if (tokens_ && tokens_->RoutingTokenSendsEnabled())
	{
		TLOG(TLVL_CHECKPENDINGBUFFERS_3) << "Sent tokens: " << tokens_->GetSentTokenCount() << ", Event count: " << run_event_count_;
		auto outstanding_tokens = tokens_->GetSentTokenCount() - run_event_count_;

		TLOG(TLVL_CHECKPENDINGBUFFERS_3) << "check_pending_buffers_: outstanding_tokens: " << outstanding_tokens << ", available_buffers: " << available_buffers
		                                 << ", tokens_to_send: " << available_buffers - outstanding_tokens;

		if (available_buffers > outstanding_tokens)
		{
			auto tokens_to_send = available_buffers - outstanding_tokens;

			while (tokens_to_send > 0)
			{
				TLOG(TLVL_CHECKPENDINGBUFFERS_3) << "check_pending_buffers_: Sending a Routing Token";
				tokens_->SendRoutingToken(1, run_id_);
				tokens_to_send--;
			}
		}
	}

	if (statsHelper_.readyToReport())
	{
		std::string statString = buildStatisticsString_();
		TLOG(TLVL_INFO) << statString;
	}

	if (metricMan)
	{
		TLOG(TLVL_CHECKPENDINGBUFFERS_2) << "check_pending_buffers_: Sending Metrics";
		metricMan->sendMetric("Event Rate", counter, "Events", 1, MetricMode::Rate);
		metricMan->sendMetric("Data Rate", eventSize, "Bytes", 1, MetricMode::Rate);
		if (counter > 0)
		{
			metricMan->sendMetric("Average Event Size", eventSize / counter, "Bytes", 1, MetricMode::Average);
			metricMan->sendMetric("Average Event Building Time", eventTime / counter, "s", 1, MetricMode::Average);
		}

		metricMan->sendMetric("Events Released to art this run", run_event_count_, "Events", 1, MetricMode::LastPoint);
		metricMan->sendMetric("Incomplete Events Released to art this run", run_incomplete_event_count_, "Events", 1, MetricMode::LastPoint);
		if (tokens_ && tokens_->RoutingTokenSendsEnabled())
		{
			metricMan->sendMetric("Tokens sent", tokens_->GetSentTokenCount(), "Tokens", 2, MetricMode::LastPoint);
		}

		auto bufferReport = GetBufferReport();
		int full = 0, empty = 0, writing = 0, reading = 0;
		for (auto& buf : bufferReport)
		{
			switch (buf.second)
			{
				case BufferSemaphoreFlags::Full:
					full++;
					break;
				case BufferSemaphoreFlags::Empty:
					empty++;
					break;
				case BufferSemaphoreFlags::Writing:
					writing++;
					break;
				case BufferSemaphoreFlags::Reading:
					reading++;
					break;
			}
		}
		auto total = size();
		TLOG(TLVL_CHECKPENDINGBUFFERS_2) << "Buffer usage: full=" << full << ", empty=" << empty << ", writing=" << writing << ", reading=" << reading << ", total=" << total;

		metricMan->sendMetric("Shared Memory Full Buffers", full, "buffers", 2, MetricMode::LastPoint);
		metricMan->sendMetric("Shared Memory Available Buffers", empty, "buffers", 2, MetricMode::LastPoint);
		metricMan->sendMetric("Shared Memory Pending Buffers", writing, "buffers", 2, MetricMode::LastPoint);
		metricMan->sendMetric("Shared Memory Reading Buffers", reading, "buffers", 2, MetricMode::LastPoint);
		if (total > 0)
		{
			metricMan->sendMetric("Shared Memory Full %", full * 100 / static_cast<double>(total), "%", 2, MetricMode::LastPoint);
			metricMan->sendMetric("Shared Memory Available %", empty * 100 / static_cast<double>(total), "%", 2, MetricMode::LastPoint);
		}
	}
	TLOG(TLVL_CHECKPENDINGBUFFERS) << "check_pending_buffers_ END";
}

void artdaq::SharedMemoryEventManager::BroadcastFragment(FragmentPtr& frag)
{
	{
		std::lock_guard<std::mutex> lk(broadcast_mutex_);

		bool entry_found = false;
		for (auto& entry : pending_broadcasts_)
		{
			if (entry.type == frag->type() && entry.sequence_id == frag->sequenceID())
			{
				TLOG(TLVL_BROADCASTFRAGMENT) << "Received BroadcastFragment of type " << static_cast<int>(frag->type()) << ", seqID " << frag->sequenceID() << " matching current pending_broadcasts_ entry. frags=" << entry.fragments.size() + 1 << "/" << init_fragment_count_;
				entry.fragments.push_back(std::move(frag));
				entry_found = true;
				break;
			}
		}
		if (!entry_found)
		{
			TLOG(TLVL_BROADCASTFRAGMENT) << "Received BroadcastFragment of type " << static_cast<int>(frag->type()) << ", seqID " << frag->sequenceID() << ", creating new pending_broadcasts_ entry";
			pending_broadcasts_.emplace_back();
			pending_broadcasts_.back().deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(GetBufferTimeout());
			pending_broadcasts_.back().type = frag->type();
			pending_broadcasts_.back().sequence_id = frag->sequenceID();
			pending_broadcasts_.back().subrun_id = GetSubrunForSequenceID(frag->sequenceID());
			pending_broadcasts_.back().fragments.push_back(std::move(frag));
		}
	}
	check_pending_broadcasts_();
}

void artdaq::SharedMemoryEventManager::check_pending_broadcasts_()
{
	std::lock_guard<std::mutex> lk(broadcast_mutex_);

	auto entry = pending_broadcasts_.begin();
	auto now = std::chrono::steady_clock::now();
	while (entry != pending_broadcasts_.end())
	{
		if (running_)
		{
			if ((!init_frags_sent_ || entry->fragments.size() == 0 || (entry->fragments.size() < init_fragment_count_ && now < entry->deadline)))
			{
				entry++;
				continue;
			}
			if (entry->fragments.front()->type() == Fragment::EndOfSubrunFragmentType || entry->fragments.front()->type() == artdaq::Fragment::SubrunDataFragmentType)
			{
				if (entry->subrun_id == subrun_id_ && TimeUtils::GetElapsedTime(last_event_time_) < subrun_transition_hold_time_s_)
				{
					TLOG(TLVL_CHECKPENDINGBROADCASTS) << "Holding entry size = " << entry->fragments.size() << " / " << init_fragment_count_ << ", lead SeqID = " << entry->fragments.front()->sequenceID() << ", subrun=" << entry->subrun_id << " because it is EndOfSubrun and hold time has not expired";
					entry++;
					continue;
				}
			}
		}

		TLOG(TLVL_CHECKPENDINGBROADCASTS) << "Broadcasting entry init_frags_sent_=" << init_frags_sent_ << ", size=" << entry->fragments.size() << "/" << init_fragment_count_ << ", subrun=" << entry->subrun_id << ", lead SeqID=" << entry->fragments.front()->sequenceID() << " deadline delta=" << std::chrono::duration_cast<std::chrono::microseconds>(now - entry->deadline).count() << " us";
		broadcastFragments_(entry->fragments);
		entry = pending_broadcasts_.erase(entry);
	}
}

bool artdaq::SharedMemoryEventManager::broadcastFragments_(FragmentPtrs& frags)
{
	if (frags.empty())
	{
		TLOG(TLVL_ERROR) << "Requested broadcast but no Fragments given!";
		return false;
	}
	if (!broadcasts_.IsValid())
	{
		TLOG(TLVL_ERROR) << "Broadcast attempted but broadcast shared memory is unavailable!";
		return false;
	}
	TLOG(TLVL_BROADCASTFRAGMENTS) << "Broadcasting " << frags.size() << " Fragments with lead seqID=" << frags.front()->sequenceID()
	                              << ", type " << detail::RawFragmentHeader::SystemTypeToString(frags.front()->type())
	                              << ", size=" << frags.front()->sizeBytes() << "B.";
	auto buffer = broadcasts_.GetBufferForWriting(false);
	TLOG(TLVL_BROADCASTFRAGMENTS) << "broadcastFragments_: after getting buffer 1st buffer=" << buffer;
	auto start_time = std::chrono::steady_clock::now();
	while (buffer == -1 && TimeUtils::GetElapsedTimeMilliseconds(start_time) < static_cast<size_t>(broadcast_timeout_ms_))
	{
		usleep(10000);
		buffer = broadcasts_.GetBufferForWriting(true);  // Go into overwrite mode
	}
	TLOG(TLVL_BROADCASTFRAGMENTS) << "broadcastFragments_: after getting buffer w/timeout, buffer=" << buffer << ", elapsed time=" << TimeUtils::GetElapsedTime(start_time) << " s.";
	if (buffer == -1)
	{
		TLOG(TLVL_ERROR) << "Broadcast of fragment type " << frags.front()->typeString() << " failed due to timeout waiting for buffer!";
		return false;
	}

	TLOG(TLVL_BROADCASTFRAGMENTS) << "broadcastFragments_: Filling in RawEventHeader";
	auto hdr = reinterpret_cast<detail::RawEventHeader*>(broadcasts_.GetBufferStart(buffer));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	hdr->run_id = run_id_;
	hdr->subrun_id = GetSubrunForSequenceID(frags.front()->sequenceID());
	hdr->sequence_id = frags.front()->sequenceID();
	hdr->is_complete = true;
	broadcasts_.IncrementWritePos(buffer, sizeof(detail::RawEventHeader));

	if (frags.front()->type() == artdaq::Fragment::EndOfSubrunFragmentType || frags.front()->type() == artdaq::Fragment::SubrunDataFragmentType)
	{
		subrun_id_ = hdr->subrun_id + 1;
	}

	for (auto& frag : frags)
	{
		if (frag->sequenceID() != hdr->sequence_id || frag->type() != frags.front()->type())
		{
			TLOG(TLVL_WARNING) << "Skipping fragment due to Type/SeqID mismatch! seqID=" << frag->sequenceID() << " (expected " << hdr->sequence_id << "), type=" << static_cast<int>(frag->type()) << " (" << static_cast<int>(frags.front()->type()) << ")";
			continue;
		}
		TLOG(TLVL_BROADCASTFRAGMENTS_2) << "broadcastFragments_ before Fragment Write call seqID=" << frag->sequenceID() << ", fragID=" << frag->fragmentID() << ", type=" << static_cast<int>(frag->type());
		broadcasts_.Write(buffer, frag->headerAddress(), frag->size() * sizeof(RawDataType));
	}

	TLOG(TLVL_BROADCASTFRAGMENTS) << "broadcastFragments_ Marking buffer full";
	broadcasts_.MarkBufferFull(buffer, -1);
	TLOG(TLVL_BROADCASTFRAGMENTS) << "broadcastFragments_ Complete";
	return true;
}

std::vector<char*> artdaq::SharedMemoryEventManager::parse_art_command_line_(const std::shared_ptr<art_config_file>& config_file, size_t process_index)
{
	auto offset_index = process_index + art_process_index_offset_;
	TLOG(TLVL_PARSEARTCOMMANDLINE) << "parse_art_command_line_: Parsing command line " << art_cmdline_ << ", config_file: " << config_file->getFileName() << ", index: " << process_index << " (w/offset: " << offset_index << ")";
	std::string art_cmdline_tmp = art_cmdline_;
	auto filenameit = art_cmdline_tmp.find("#CONFIG_FILE#");
	if (filenameit != std::string::npos)
	{
		art_cmdline_tmp.replace(filenameit, 13, config_file->getFileName());
	}
	auto indexit = art_cmdline_tmp.find("#PROCESS_INDEX#");
	if (indexit != std::string::npos)
	{
		art_cmdline_tmp.replace(indexit, 15, std::to_string(offset_index));
	}
	TLOG(TLVL_PARSEARTCOMMANDLINE) << "parse_art_command_line_: After replacing index and config parameters, command line is " << art_cmdline_tmp;

	std::istringstream iss(art_cmdline_tmp);
	auto tokens = std::vector<std::string>{std::istream_iterator<std::string>{iss}, std::istream_iterator<std::string>{}};
	std::vector<char*> output;

	for (auto& token : tokens)
	{
		TLOG(TLVL_PARSEARTCOMMANDLINE) << "parse_art_command_line_: Adding cmdline token " << token << " to output list";
		output.emplace_back(new char[token.length() + 1]);
		memcpy(output.back(), token.c_str(), token.length());
		output.back()[token.length()] = '\0';  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	}
	output.emplace_back(nullptr);

	return output;
}

void artdaq::SharedMemoryEventManager::send_init_frags_()
{
	std::lock_guard<std::mutex> lk(init_fragments_mutex_);
	if (init_fragment_map_size_() >= init_fragment_count_ && init_fragment_count_ > 0)
	{
		TLOG(TLVL_INFO) << "Broadcasting " << init_fragment_map_size_() << " Init Fragment(s) to all art subprocesses...";

		FragmentPtrs init_fragments;
		for (auto& fragment_id_pair : init_fragment_map_)
		{
			for (auto& ts_pair : fragment_id_pair.second)
			{
				init_fragments.emplace_back(std::make_unique<Fragment>(*ts_pair.second));
			}
		}

		broadcastFragments_(init_fragments);
		TLOG(TLVL_SENDINIT) << "Init Fragment sent";
		init_frags_sent_ = true;
	}
	else if (init_fragment_count_ > 0 && init_fragment_map_size_() == 0)
	{
		TLOG(TLVL_INFO) << "Cannot send Init Fragment(s) because I haven't yet received any! Set send_init_fragments to false or init_fragment_count to 0 if this process does not receive serialized art events to avoid potentially lengthy timeouts!";
	}
	else if (init_fragment_count_ > 0)
	{
		TLOG(TLVL_INFO) << "Cannot send Init Fragment(s) because I haven't yet received them (have " << init_fragment_map_size_() << " of " << init_fragment_count_ << ")!";
	}
	else
	{
		// Send an empty Init Fragment so that ArtdaqInput knows that this is a pure-Fragment input
		artdaq::FragmentPtrs begin_run_fragments_;
		begin_run_fragments_.emplace_back(new artdaq::Fragment());
		begin_run_fragments_.back()->setSystemType(artdaq::Fragment::InitFragmentType);
		broadcastFragments_(begin_run_fragments_);
		init_frags_sent_ = true;
	}
}

void artdaq::SharedMemoryEventManager::AddInitFragment(FragmentPtr& frag)
{
	std::unique_lock<std::mutex> lk(init_fragments_mutex_);

	auto fragId = frag->fragmentID();
	auto ts = frag->timestamp();

	init_fragment_map_[fragId][ts] = std::move(frag);
	TLOG(TLVL_ADDINITFRAGMENT) << "Received Init Fragment from rank " << fragId << ", art process id " << ts << ". Now have " << init_fragment_map_size_() << " of " << init_fragment_count_;

	// Don't send until all init fragments have been received
	if (init_fragment_map_size_() >= init_fragment_count_)
	{
		lk.unlock();
		send_init_frags_();
	}
}

size_t artdaq::SharedMemoryEventManager::init_fragment_map_size_() const
{
	size_t size = 0;

	for (auto& frag_id_pair : init_fragment_map_)
	{
		size += frag_id_pair.second.size();
	}

	return size;
}

void artdaq::SharedMemoryEventManager::UpdateArtConfiguration(fhicl::ParameterSet art_pset)
{
	TLOG(TLVL_UPDATEARTCONFIG) << "UpdateArtConfiguration BEGIN";
	if (art_pset != current_art_pset_ || !current_art_config_file_)
	{
		current_art_pset_ = art_pset;
		if (manual_art_)
			current_art_config_file_ = std::make_shared<art_config_file>(art_pset, GetKey(), GetBroadcastKey());
		else
			current_art_config_file_ = std::make_shared<art_config_file>(art_pset);
	}
	TLOG(TLVL_UPDATEARTCONFIG) << "UpdateArtConfiguration END";
}

std::string artdaq::SharedMemoryEventManager::buildStatisticsString_() const
{
	std::ostringstream oss;
	oss << app_name << " statistics:" << std::endl;

	artdaq::MonitoredQuantityPtr mqPtr =
	    artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(EVENTS_RELEASED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Event statistics: " << stats.recentSampleCount << " events released at " << stats.recentSampleRate
		    << " events/sec, effective data rate = "
		    << (stats.recentValueRate / 1024.0 / 1024.0)
		    << " MB/sec, monitor window = " << stats.recentDuration
		    << " sec, min::max event size = " << (stats.recentValueMin / 1024.0 / 1024.0)
		    << "::" << (stats.recentValueMax / 1024.0 / 1024.0) << " MB" << std::endl;
		if (stats.recentSampleRate > 0.0)
		{
			oss << "  Average time per event: ";
			oss << " elapsed time = " << (1.0 / stats.recentSampleRate) << " sec" << std::endl;
		}
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(FRAGMENTS_RECEIVED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Fragment statistics: " << stats.recentSampleCount << " fragments received at " << stats.recentSampleRate
		    << " fragments/sec, effective data rate = "
		    << (stats.recentValueRate / 1024.0 / 1024.0)
		    << " MB/sec, monitor window = " << stats.recentDuration
		    << " sec, min::max fragment size = " << (stats.recentValueMin / 1024.0 / 1024.0)
		    << "::" << (stats.recentValueMax / 1024.0 / 1024.0) << " MB" << std::endl;
	}

	oss << "  Event counts: Run -- " << run_event_count_ << " Total, " << run_incomplete_event_count_ << " Incomplete."
	    << "  Subrun -- " << subrun_event_count_ << " Total, " << subrun_incomplete_event_count_ << " Incomplete. "
	    << std::endl;
	//-----------------------------------------------------------------------------
	// P.Murat: add statistics on the SHM buffers
	// there are 4 different flags: 0:empty, 1:writing; 2:full 3:reading
	// want statistics on all of them
	//-----------------------------------------------------------------------------
	// auto = std::vector<std::pair<int, artdaq::SharedMemoryManager::BufferSemaphoreFlags>>
	artdaq::SharedMemoryEventManager* nc_this = (artdaq::SharedMemoryEventManager*)this;

	int bsize = nc_this->BufferSize();

	auto v = nc_this->GetBufferReport();

	int nbb[4] = {0, 0, 0, 0};

	int nbuff = v.size();
	for (int i = 0; i < nbuff; i++)
	{
		auto x = v[i];
		int flag = (int)x.second;
		nbb[flag]++;
	}

	oss << "  Shared Memory: "
	    << nbuff << " buffers of " << bsize << " B, "
	    << nbb[0] << " Empty, " << nbb[1] << " Writing, " << nbb[2] << " Full, " << nbb[3] << " reading"
	    << std::endl;

	return oss.str();
}
