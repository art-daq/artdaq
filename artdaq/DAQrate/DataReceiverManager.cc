#include <chrono>

#define TRACE_NAME "DataReceiverManager"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq/TransferPlugins/detail/HostMap.hh"
#include "cetlib_except/exception.h"
#include <iomanip>

artdaq::DataReceiverManager::DataReceiverManager(const fhicl::ParameterSet& pset, std::shared_ptr<SharedMemoryEventManager> shm)
	: stop_requested_(false)
	, stop_requested_time_(0)
	, source_threads_()
	, source_plugins_()
	, enabled_sources_()
	, running_sources_()
	, recv_frag_count_()
	, recv_frag_size_()
	, recv_seq_count_()
	, receive_timeout_(pset.get<size_t>("receive_timeout_usec", 100000))
	, stop_timeout_ms_(pset.get<size_t>("stop_timeout_ms", 1500))
	, shm_manager_(shm)
	, non_reliable_mode_enabled_(pset.get<bool>("non_reliable_mode", false))
	, non_reliable_mode_retry_count_(pset.get<size_t>("non_reliable_mode_retry_count", -1))
{
	TLOG(TLVL_DEBUG) << "Constructor";
	auto enabled_srcs = pset.get<std::vector<int>>("enabled_sources", std::vector<int>());
	auto enabled_srcs_empty = enabled_srcs.size() == 0;

	if (non_reliable_mode_enabled_)
	{
		TLOG(TLVL_WARNING) << "DataReceiverManager is configured to drop data after " << std::to_string(non_reliable_mode_retry_count_)
			<< " failed attempts to put data into the SharedMemoryEventManager! If this is unexpected, please check your configuration!";
	}

	if (enabled_srcs_empty)
	{
		TLOG(TLVL_INFO) << "enabled_sources not specified, assuming all sources enabled.";
	}
	else
	{
		for (auto& s : enabled_srcs)
		{
			enabled_sources_.insert(s);
		}
	}

	hostMap_t host_map = MakeHostMap(pset);
	auto srcs = pset.get<fhicl::ParameterSet>("sources", fhicl::ParameterSet());
	for (auto& s : srcs.get_pset_names())
	{
		auto src_pset = srcs.get<fhicl::ParameterSet>(s);
		host_map = MakeHostMap(src_pset, 0, host_map);
	}
	auto host_map_pset = MakeHostMapPset(host_map);
	fhicl::ParameterSet srcs_mod;
	for (auto& s : srcs.get_pset_names())
	{
		auto src_pset = srcs.get<fhicl::ParameterSet>(s);
		src_pset.erase("host_map");
		src_pset.put<std::vector<fhicl::ParameterSet>>("host_map", host_map_pset);
		srcs_mod.put<fhicl::ParameterSet>(s, src_pset);
	}

	for (auto& s : srcs_mod.get_pset_names())
	{
		try
		{
			auto transfer = std::unique_ptr<TransferInterface>(MakeTransferPlugin(srcs_mod, s,
				TransferInterface::Role::kReceive));
			auto source_rank = transfer->source_rank();
			if (enabled_srcs_empty) enabled_sources_.insert(source_rank);
			source_plugins_[source_rank] = std::move(transfer);
		}
		catch (cet::exception ex)
		{
			TLOG(TLVL_WARNING) << "cet::exception caught while setting up source " << s << ": " << ex.what();
		}
		catch (std::exception ex)
		{
			TLOG(TLVL_WARNING) << "std::exception caught while setting up source " << s << ": " << ex.what();
		}
		catch (...)
		{
			TLOG(TLVL_WARNING) << "Non-cet exception caught while setting up source " << s << ".";
		}
	}
	if (srcs.get_pset_names().size() == 0)
	{
		TLOG(TLVL_ERROR) << "No sources configured!";
	}
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
	TLOG(TLVL_TRACE) << "~DataReceiverManager: BEGIN";
	stop_threads();
	shm_manager_.reset();
	TLOG(TLVL_TRACE) << "Destructor END";
}


void artdaq::DataReceiverManager::start_threads()
{
	stop_requested_ = false;
	if (shm_manager_) shm_manager_->setRequestMode(artdaq::detail::RequestMessageMode::Normal);
	for (auto& source : source_plugins_)
	{
		auto& rank = source.first;
		if (enabled_sources_.count(rank))
		{
			running_sources_.insert(rank);
			boost::thread::attributes attrs;
			attrs.set_stack_size(4096 * 500); // 2000 KB
			source_threads_[rank] = boost::thread(attrs, boost::bind(&DataReceiverManager::runReceiver_, this, rank));
		}
	}
}

void artdaq::DataReceiverManager::stop_threads()
{
	TLOG(TLVL_TRACE) << "stop_threads: BEGIN: Setting stop_requested to true, frags=" << std::to_string(count()) << ", bytes=" << std::to_string(byteCount());

	stop_requested_time_ = TimeUtils::gettimeofday_us();
	stop_requested_ = true;

	TLOG(TLVL_TRACE) << "stop_threads: Joining all threads";
	for (auto& s : source_threads_)
	{
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	std::chrono::steady_clock::time_point start_time, after_header, before_body, eod_quiet_start;
	int ret;
	double delta_t, hdr_delta_t, store_delta_t, data_delta_t;
	detail::RawFragmentHeader header;
	size_t endOfDataCount = -1;
	auto sleep_time = receive_timeout_ / 100 > 100000 ? 100000 : receive_timeout_ / 100;
	auto max_retries = non_reliable_mode_retry_count_ * ceil(receive_timeout_ / sleep_time);

	while (!(stop_requested_ && TimeUtils::gettimeofday_us() - stop_requested_time_ > stop_timeout_ms_ * 1000) && enabled_sources_.count(source_rank))
	{
		TLOG(16) << "runReceiver_: Begin loop";
		if (stop_requested_) { receive_timeout_ = stop_timeout_ms_; }

		// Don't stop receiving until we haven't received anything for 1 second
		if (endOfDataCount <= recv_frag_count_.slotCount(source_rank) && TimeUtils::GetElapsedTimeMilliseconds(eod_quiet_start) > 1000)
		{
			TLOG(TLVL_DEBUG) << "runReceiver_: End of Data conditions met, ending runReceiver loop";
			running_sources_.erase(source_rank);
			return;
		}

		start_time = std::chrono::steady_clock::now();

		TLOG(16) << "runReceiver_: Calling receiveFragmentHeader";
		ret = source_plugins_[source_rank]->receiveFragmentHeader(header, receive_timeout_);
		TLOG(16) << "runReceiver_: Done with receiveFragmentHeader, ret=" << ret << " (should be " << source_rank << ")";
		if (ret != source_rank)
		{
			if (ret >= 0) {
				TLOG(TLVL_WARNING) << "Received Fragment from rank " << ret << ", but was expecting one from rank " << source_rank << "!";
			}
			continue; // Receive timeout or other oddness
		}

		after_header = std::chrono::steady_clock::now();
		eod_quiet_start = std::chrono::steady_clock::now();

		if (Fragment::isUserFragmentType(header.type) || header.type == Fragment::DataFragmentType || header.type == Fragment::EmptyFragmentType || header.type == Fragment::ContainerFragmentType) {
			TLOG(TLVL_TRACE) << "Received Fragment Header from rank " << source_rank << ".";
			RawDataType* loc = nullptr;
			size_t retries = 0;
			while (loc == nullptr)//&& TimeUtils::GetElapsedTimeMicroseconds(after_header)) < receive_timeout_) 
			{
				loc = shm_manager_->WriteFragmentHeader(header);
				if (loc == nullptr) usleep(sleep_time);
				if (stop_requested_) return;
				retries++;
				if (non_reliable_mode_enabled_ && retries > max_retries)
				{
					loc = shm_manager_->WriteFragmentHeader(header, true);
				}
			}
			if (loc == nullptr)
			{
				// Could not enqueue event!
				TLOG(TLVL_ERROR) << "runReceiver_: Could not get data location for event " << std::to_string(header.sequence_id);
				continue;
			}
			before_body = std::chrono::steady_clock::now();

			TLOG(16) << "runReceiver_: Calling receiveFragmentData";
			auto ret2 = source_plugins_[source_rank]->receiveFragmentData(loc, header.word_count - header.num_words());
			TLOG(16) << "runReceiver_: Done with receiveFragmentData, ret2=" << ret2 << " (should be " << source_rank << ")";

			if (ret != ret2) {
				TLOG(TLVL_ERROR) << "Unexpected return code from receiveFragmentData after receiveFragmentHeader! (Expected: " << ret << ", Got: " << ret2 << ")";
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader! (Expected: " << ret << ", Got: " << ret2 << ")";
			}

			shm_manager_->DoneWritingFragment(header);
			TLOG(TLVL_TRACE) << "Done receiving fragment with sequence ID " << std::to_string(header.sequence_id) << " from rank " << source_rank;

			recv_frag_count_.incSlot(source_rank);
			recv_frag_size_.incSlot(source_rank, header.word_count * sizeof(RawDataType));
			recv_seq_count_.setSlot(source_rank, header.sequence_id);
			if (endOfDataCount != static_cast<size_t>(-1))
			{
				TLOG(TLVL_DEBUG) << "Received fragment " << std::to_string(header.sequence_id) << " from rank " << source_rank
					<< " (" << std::to_string(recv_frag_count_.slotCount(source_rank)) << "/" << std::to_string(endOfDataCount) << ")";
			}

			if (metricMan)
			{//&& recv_frag_count_.slotCount(source_rank) % 100 == 0) {
				TLOG(6) << "runReceiver_: Sending receive stats";
				delta_t = TimeUtils::GetElapsedTime(start_time);
				hdr_delta_t = TimeUtils::GetElapsedTime(start_time, after_header);
				store_delta_t = TimeUtils::GetElapsedTime(after_header, before_body);
				data_delta_t = TimeUtils::GetElapsedTime(before_body);
				metricMan->sendMetric("Total Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Total Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(header.word_count * sizeof(RawDataType)), "B", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Total Receive Rate From Rank " + std::to_string(source_rank), header.word_count * sizeof(RawDataType) / delta_t, "B/s", 5, MetricMode::Average);

				metricMan->sendMetric("Header Receive Time From Rank " + std::to_string(source_rank), hdr_delta_t, "s", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Header Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(header.num_words() * sizeof(RawDataType)), "B", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Header Receive Rate From Rank " + std::to_string(source_rank), header.num_words() * sizeof(RawDataType) / hdr_delta_t, "B/s", 5, MetricMode::Average);

				metricMan->sendMetric("Shared Memory Wait Time From Rank " + std::to_string(source_rank), store_delta_t, "s", 5, MetricMode::Accumulate);

				metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), data_delta_t, "s", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>((header.word_count - header.num_words()) * sizeof(RawDataType)), "B", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), (header.word_count - header.num_words()) * sizeof(RawDataType) / data_delta_t, "B/s", 5, MetricMode::Average);
				metricMan->sendMetric("Data Receive Count From Rank " + std::to_string(source_rank), recv_frag_count_.slotCount(source_rank), "fragments", 3, MetricMode::LastPoint);
				TLOG(6) << "runReceiver_: Done sending receive stats";
			}
		}
		else if (header.type == Fragment::EndOfDataFragmentType || header.type == Fragment::InitFragmentType || header.type == Fragment::EndOfRunFragmentType || header.type == Fragment::EndOfSubrunFragmentType || header.type == Fragment::ShutdownFragmentType)
		{
			TLOG(TLVL_DEBUG) << "Received System Fragment from rank " << source_rank << " of type " << detail::RawFragmentHeader::SystemTypeToString(header.type) << ".";

			FragmentPtr frag(new Fragment(header.word_count - header.num_words()));
			memcpy(frag->headerAddress(), &header, header.num_words() * sizeof(RawDataType));
			auto ret3 = source_plugins_[source_rank]->receiveFragmentData(frag->headerAddress() + header.num_words(), header.word_count - header.num_words());
			if (ret3 != source_rank)
			{
				TLOG(TLVL_ERROR) << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving System Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving System Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";
			}

			switch (header.type)
			{
			case Fragment::EndOfDataFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				endOfDataCount = *(frag->dataBegin());
				TLOG(TLVL_DEBUG) << "EndOfData Fragment indicates that " << std::to_string(endOfDataCount) << " fragments are expected from rank " << source_rank
					<< " (recvd " << std::to_string(recv_frag_count_.slotCount(source_rank)) << ").";
				break;
			case Fragment::InitFragmentType:
				TLOG(TLVL_DEBUG) << "Received Init Fragment from rank " << source_rank << ".";
				shm_manager_->setRequestMode(detail::RequestMessageMode::Normal);
				shm_manager_->SetInitFragment(std::move(frag));
				break;
			case Fragment::EndOfRunFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				//shm_manager_->endRun();
				break;
			case Fragment::EndOfSubrunFragmentType:
				//shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				if (header.sequence_id != Fragment::InvalidSequenceID) shm_manager_->rolloverSubrun(header.sequence_id);
				else shm_manager_->rolloverSubrun(recv_seq_count_.slotCount(source_rank));
				break;
			case Fragment::ShutdownFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				break;
			}
		}

	}


	running_sources_.erase(source_rank);
}
