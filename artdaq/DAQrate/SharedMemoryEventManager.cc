#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include <iomanip>
#include <fstream>

const std::string artdaq::SharedMemoryEventManager::EVENT_RATE_STAT_KEY("SharedMemoryEventRate");
const std::string artdaq::SharedMemoryEventManager::INCOMPLETE_EVENT_STAT_KEY("SharedMemoryIncompleteEvents");

artdaq::SharedMemoryEventManager::SharedMemoryEventManager(fhicl::ParameterSet pset, size_t num_fragments_per_event, run_id_t run,
														   size_t event_queue_depth, std::string art_fhicl)
	: SharedMemoryManager(pset.get<int>("shm_key", 0xBEE7),
						  pset.get<size_t>("event_queue_depth", event_queue_depth),
						  pset.get<size_t>("max_event_size_bytes"))
	, num_art_processes_(pset.get<size_t>("art_analyzer_count", 1))
	, num_fragments_per_event_(pset.get<size_t>("fragment_count", num_fragments_per_event))
	, queue_size_(pset.get<size_t>("event_queue_depth", event_queue_depth))
	, run_id_(run)
	, subrun_id_(0)
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
}

artdaq::SharedMemoryEventManager::~SharedMemoryEventManager()
{
	std::vector<int> ignored;
	endOfData(ignored);
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
		MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
			getMonitoredQuantity(EVENT_RATE_STAT_KEY);
		if (mqPtr.get() != 0)
		{
			mqPtr->addSample(BufferDataSize(buffer));
		}
		ReleaseBuffer(buffer);
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

void artdaq::SharedMemoryEventManager::RunArt()
{
	while (restart_art_)
	{
		art_process_return_codes_.push_back(system(("art -c " + config_file_name_).c_str()));
	}
}

void artdaq::SharedMemoryEventManager::ReconfigureArt(std::string art_fhicl)
{
	std::vector<int> ignored;
	endOfData(ignored);
	std::ofstream of(config_file_name_, std::ofstream::trunc);
	of << art_fhicl << std::endl;
	of.close();
	StartArt();
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
		SetBufferDestination(buffer, ii);
	}
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

void artdaq::SharedMemoryEventManager::StartArt()
{
	restart_art_ = true;
	for (size_t ii = 0; ii < num_art_processes_; ++ii)
	{
		art_processes_.emplace_back([=] {RunArt(); });
	}
}

bool
artdaq::SharedMemoryEventManager::endOfData(std::vector<int>& readerReturnValues)
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
		MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
			getMonitoredQuantity(EVENT_RATE_STAT_KEY);
		if (mqPtr.get() != 0)
		{
			mqPtr->addSample(BufferDataSize(buf));
		}
		ReleaseBuffer(buf);
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

void
artdaq::SharedMemoryEventManager::initStatistics_()
{
	MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
		getMonitoredQuantity(EVENT_RATE_STAT_KEY);
	if (mqPtr.get() == 0)
	{
		mqPtr.reset(new MonitoredQuantity(3.0, 300.0));
		StatisticsCollection::getInstance().
			addMonitoredQuantity(EVENT_RATE_STAT_KEY, mqPtr);
	}
	mqPtr->reset();

	mqPtr = StatisticsCollection::getInstance().
		getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
	if (mqPtr.get() == 0)
	{
		mqPtr.reset(new MonitoredQuantity(3.0, 300.0));
		StatisticsCollection::getInstance().
			addMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY, mqPtr);
	}
	mqPtr->reset();
}

void
artdaq::SharedMemoryEventManager::reportStatistics_()
{
	MonitoredQuantityPtr mqPtr = StatisticsCollection::getInstance().
		getMonitoredQuantity(EVENT_RATE_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		std::ostringstream oss;
		oss << EVENT_RATE_STAT_KEY << "_" << std::setfill('0') << std::setw(4) << run_id_
			<< "_" << std::setfill('0') << std::setw(4) << my_rank << ".txt";
		std::string filename = oss.str();
		std::ofstream outStream(filename.c_str());
		mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		outStream << "SharedMemoryEventManager rank " << my_rank << ": events processed = "
			<< stats.fullSampleCount << " at " << stats.fullSampleRate
			<< " events/sec, data rate = "
			<< (stats.fullValueRate * sizeof(RawDataType)
				/ 1024.0 / 1024.0) << " MB/sec, duration = "
			<< stats.fullDuration << " sec" << std::endl
			<< "    minimum event size = "
			<< (stats.fullValueMin * sizeof(RawDataType)
				/ 1024.0 / 1024.0)
			<< " MB, maximum event size = "
			<< (stats.fullValueMax * sizeof(RawDataType)
				/ 1024.0 / 1024.0)
			<< " MB" << std::endl;
		bool foundTheStart = false;
		for (int idx = 0; idx < (int)stats.recentBinnedDurations.size(); ++idx)
		{
			if (stats.recentBinnedDurations[idx] > 0.0)
			{
				foundTheStart = true;
			}
			if (foundTheStart)
			{
				outStream << "  " << std::fixed << std::setprecision(3)
					<< stats.recentBinnedEndTimes[idx]
					<< ": " << stats.recentBinnedSampleCounts[idx]
					<< " events at "
					<< (stats.recentBinnedSampleCounts[idx] /
						stats.recentBinnedDurations[idx])
					<< " events/sec, data rate = "
					<< (stats.recentBinnedValueSums[idx] *
						sizeof(RawDataType) / 1024.0 / 1024.0 /
						stats.recentBinnedDurations[idx])
					<< " MB/sec, bin size = "
					<< stats.recentBinnedDurations[idx]
					<< " sec" << std::endl;
			}
		}
		outStream.close();
	}

	mqPtr = StatisticsCollection::getInstance().
		getMonitoredQuantity(INCOMPLETE_EVENT_STAT_KEY);
	if (mqPtr.get() != 0)
	{
		std::ostringstream oss;
		oss << INCOMPLETE_EVENT_STAT_KEY << "_" << std::setfill('0')
			<< std::setw(4) << run_id_
			<< "_" << std::setfill('0') << std::setw(4) << my_rank << ".txt";
		std::string filename = oss.str();
		std::ofstream outStream(filename.c_str());
		mqPtr->waitUntilAccumulatorsHaveBeenFlushed(3.0);
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		outStream << "SharedMemoryEventManager rank " << my_rank << ": fragments processed = "
			<< stats.fullSampleCount << " at " << stats.fullSampleRate
			<< " fragments/sec, average incomplete event count = "
			<< stats.fullValueAverage << " duration = "
			<< stats.fullDuration << " sec" << std::endl
			<< "    minimum incomplete event count = "
			<< stats.fullValueMin << ", maximum incomplete event count = "
			<< stats.fullValueMax << std::endl;
		bool foundTheStart = false;
		for (int idx = 0; idx < (int)stats.recentBinnedDurations.size(); ++idx)
		{
			if (stats.recentBinnedDurations[idx] > 0.0)
			{
				foundTheStart = true;
			}
			if (foundTheStart && stats.recentBinnedSampleCounts[idx] > 0.0)
			{
				outStream << "  " << std::fixed << std::setprecision(3)
					<< stats.recentBinnedEndTimes[idx]
					<< ": " << stats.recentBinnedSampleCounts[idx]
					<< " fragments at "
					<< (stats.recentBinnedSampleCounts[idx] /
						stats.recentBinnedDurations[idx])
					<< " fragments/sec, average incomplete event count = "
					<< (stats.recentBinnedValueSums[idx] /
						stats.recentBinnedSampleCounts[idx])
					<< ", bin size = "
					<< stats.recentBinnedDurations[idx]
					<< " sec" << std::endl;
			}
		}
		outStream << "Incomplete count now = " << GetOpenEventCount() << std::endl;
		outStream.close();
	}
}

void
artdaq::SharedMemoryEventManager::sendMetrics()
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