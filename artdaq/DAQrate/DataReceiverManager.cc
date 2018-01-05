#include <chrono>

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "cetlib_except/exception.h"
#include <iomanip>

artdaq::DataReceiverManager::DataReceiverManager(const fhicl::ParameterSet& pset, std::shared_ptr<SharedMemoryEventManager> shm)
	: stop_requested_(false)
	, stop_requested_time_(0)
	, source_threads_()
	, source_plugins_()
	, enabled_sources_()
	, recv_frag_count_()
	, recv_frag_size_()
	, recv_seq_count_()
	, receive_timeout_(pset.get<size_t>("receive_timeout_usec", 100000))
	, stop_timeout_ms_(pset.get<size_t>("stop_timeout_ms",3000))
	, shm_manager_(shm)
	, non_reliable_mode_enabled_(pset.get<bool>("non_reliable_mode", false))
	, non_reliable_mode_retry_count_(pset.get<size_t>("non_reliable_mode_retry_count", -1))
{
	TLOG_DEBUG("DataReceiverManager") << "Constructor" << TLOG_ENDL;
	auto enabled_srcs = pset.get<std::vector<int>>("enabled_sources", std::vector<int>());
	auto enabled_srcs_empty = enabled_srcs.size() == 0;

	if (non_reliable_mode_enabled_)
	{
		TLOG_WARNING("DataReceiverManager") << "DataReceiverManager is configured to drop data after " << std::to_string(non_reliable_mode_retry_count_)
			<< " failed attempts to put data into the SharedMemoryEventManager! If this is unexpected, please check your configuration!" << TLOG_ENDL;
	}

	if (enabled_srcs_empty)
	{
		TLOG_INFO("DataReceiverManager") << "enabled_sources not specified, assuming all sources enabled." << TLOG_ENDL;
	}
	else
	{
		for (auto& s : enabled_srcs)
		{
			enabled_sources_.insert(s);
		}
	}

	auto srcs = pset.get<fhicl::ParameterSet>("sources", fhicl::ParameterSet());
	for (auto& s : srcs.get_pset_names())
	{
		try
		{
			auto transfer = std::unique_ptr<TransferInterface>(MakeTransferPlugin(srcs, s,
																				  TransferInterface::Role::kReceive));
			auto source_rank = transfer->source_rank();
			if (enabled_srcs_empty) enabled_sources_.insert(source_rank);
			source_plugins_[source_rank] = std::move(transfer);
		}
		catch (cet::exception ex)
		{
			TLOG_WARNING("DataReceiverManager") << "cet::exception caught while setting up source " << s << ": " << ex.what() << TLOG_ENDL;
		}
		catch (std::exception ex)
		{
			TLOG_WARNING("DataReceiverManager") << "std::exception caught while setting up source " << s << ": " << ex.what() << TLOG_ENDL;
		}
		catch (...)
		{
			TLOG_WARNING("DataReceiverManager") << "Non-cet exception caught while setting up source " << s << "." << TLOG_ENDL;
		}
	}
	if (srcs.get_pset_names().size() == 0)
	{
		TLOG_ERROR("DataReceiverManager") << "No sources configured!" << TLOG_ENDL;
	}
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
	TLOG_TRACE("DataReceiverManager") << "~DataReceiverManager: BEGIN: Setting stop_requested to true, frags=" << std::to_string(count()) << ", bytes=" << std::to_string(byteCount()) << TLOG_ENDL;
	stop_requested_time_ = TimeUtils::gettimeofday_us();
	stop_requested_ = true;

	TLOG_TRACE("DataReceiverManager") << "~DataReceiverManager: Joining all threads" << TLOG_ENDL;
	for (auto& s : source_threads_)
	{
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
	shm_manager_.reset();
	TLOG_TRACE("DataReceiverManager") << "Destructor END" << TLOG_ENDL;
}


void artdaq::DataReceiverManager::start_threads()
{
	for (auto& source : source_plugins_)
	{
		auto& rank = source.first;
		if (enabled_sources_.count(rank))
		{
			running_sources_.insert(rank);
			source_threads_[rank] = std::thread(&DataReceiverManager::runReceiver_, this, rank);
		}
	}
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	std::chrono::steady_clock::time_point start_time, after_header, before_body;
	int ret;
	double delta_t, hdr_delta_t, store_delta_t, data_delta_t;
	detail::RawFragmentHeader header;
	size_t endOfDataCount = -1;
	auto sleep_time = receive_timeout_ / 100 > 100000 ? 100000 : receive_timeout_ / 100;
	auto max_retries = non_reliable_mode_retry_count_ * ceil(receive_timeout_ / sleep_time);

	while (!(stop_requested_ && TimeUtils::gettimeofday_us() - stop_requested_time_ > stop_timeout_ms_ * 1000) && enabled_sources_.count(source_rank))
	{
		TRACE(16, "DataReceiverManager::runReceiver_: Begin loop");
		if (stop_requested_) { receive_timeout_ = stop_timeout_ms_; }

		start_time = std::chrono::steady_clock::now();

		TRACE(16, "DataReceiverManager::runReceiver_: Calling receiveFragmentHeader");
		ret = source_plugins_[source_rank]->receiveFragmentHeader(header, receive_timeout_);
		TRACE(16, "DataReceiverManager::runReceiver_: Done with receiveFragmentHeader, ret=%d (should be %d)", ret, source_rank);
		if (ret != source_rank)
		{
			continue; // Receive timeout or other oddness
		}

		after_header = std::chrono::steady_clock::now();

		if (Fragment::isUserFragmentType(header.type) || header.type == Fragment::DataFragmentType || header.type == Fragment::EmptyFragmentType || header.type == Fragment::ContainerFragmentType) {
			TLOG_TRACE("DataReceiverManager") << "Received Fragment Header from rank " << source_rank << "." << TLOG_ENDL;
			RawDataType* loc = nullptr;
			size_t retries = 0;
			while (loc == nullptr )//&& TimeUtils::GetElapsedTimeMicroseconds(after_header)) < receive_timeout_) 
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
				TLOG_ERROR("DataReceiverManager") << "runReceiver_: Could not get data location for event " << std::to_string(header.sequence_id) << TLOG_ENDL;
				continue;
			}
			before_body = std::chrono::steady_clock::now();

			TRACE(16, "DataReceiverManager::runReceiver_: Calling receiveFragmentData");
			auto ret2 = source_plugins_[source_rank]->receiveFragmentData(loc, header.word_count - header.num_words());
			TRACE(16, "DataReceiverManager::runReceiver_: Done with receiveFragmentData, ret2=%d (should be %d)", ret2, source_rank);

			if (ret != ret2) {
				TLOG_ERROR("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader! (Expected: " << ret << ", Got: " << ret2 << ")" << TLOG_ENDL;
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader! (Expected: " << ret << ", Got: " << ret2 << ")";
			}

			shm_manager_->DoneWritingFragment(header);
			TLOG_TRACE("DataReceiverManager") << "Done receiving fragment with sequence ID " << std::to_string(header.sequence_id) << " from rank " << source_rank << TLOG_ENDL;

			recv_frag_count_.incSlot(source_rank);
			recv_frag_size_.incSlot(source_rank, header.word_count * sizeof(RawDataType));
			recv_seq_count_.setSlot(source_rank, header.sequence_id);
			if (endOfDataCount != static_cast<size_t>(-1))
			{
				TLOG_DEBUG("DataReceiverManager") << "Received fragment " << std::to_string(header.sequence_id) << " from rank " << source_rank
					<< " (" << std::to_string(recv_frag_count_.slotCount(source_rank)) << "/" << std::to_string(endOfDataCount) << ")" << TLOG_ENDL;
			}

			if (metricMan)
			{//&& recv_frag_count_.slotCount(source_rank) % 100 == 0) {
				TRACE(6, "DataReceiverManager::runReceiver_: Sending receive stats");
				delta_t = TimeUtils::GetElapsedTime(start_time);
				hdr_delta_t = TimeUtils::GetElapsedTime(start_time, after_header);
				store_delta_t = TimeUtils::GetElapsedTime(after_header, before_body);
				data_delta_t = TimeUtils::GetElapsedTime(before_body);
				metricMan->sendMetric("Total Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 1, MetricMode::Accumulate);
				metricMan->sendMetric("Total Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(header.word_count * sizeof(RawDataType)), "B", 1, MetricMode::Accumulate);
				metricMan->sendMetric("Total Receive Rate From Rank " + std::to_string(source_rank), header.word_count * sizeof(RawDataType) / delta_t, "B/s", 1, MetricMode::Average);

				metricMan->sendMetric("Header Receive Time From Rank " + std::to_string(source_rank), hdr_delta_t, "s", 1, MetricMode::Accumulate);
				metricMan->sendMetric("Header Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(header.num_words() * sizeof(RawDataType)), "B", 1, MetricMode::Accumulate);
				metricMan->sendMetric("Header Receive Rate From Rank " + std::to_string(source_rank), header.num_words() * sizeof(RawDataType) / hdr_delta_t, "B/s", 1, MetricMode::Average);

				metricMan->sendMetric("Shared Memory Wait Time From Rank " + std::to_string(source_rank), store_delta_t, "s", 1, MetricMode::Accumulate);

				metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), data_delta_t, "s", 1, MetricMode::Accumulate);
				metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>((header.word_count - header.num_words()) * sizeof(RawDataType)), "B", 1, MetricMode::Accumulate);
				metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), (header.word_count - header.num_words()) * sizeof(RawDataType) / data_delta_t, "B/s", 1, MetricMode::Average);
			metricMan->sendMetric("Data Receive Count From Rank " + std::to_string(source_rank), recv_frag_count_.slotCount(source_rank), "fragments", 3, MetricMode::Accumulate);
				TRACE(6, "DataReceiverManager::runReceiver_: Done sending receive stats");
			}
		}
		else if (header.type == Fragment::EndOfDataFragmentType || header.type == Fragment::InitFragmentType || header.type == Fragment::EndOfRunFragmentType || header.type == Fragment::EndOfSubrunFragmentType || header.type == Fragment::ShutdownFragmentType)
		{
			TLOG_DEBUG("DataReceiverManager") << "Received System Fragment from rank " << source_rank << " of type " << detail::RawFragmentHeader::SystemTypeToString(header.type) << "." << TLOG_ENDL;
			
			FragmentPtr frag(new Fragment(header.word_count - header.num_words()));
			memcpy(frag->headerAddress(), &header, header.num_words() * sizeof(RawDataType));
			auto ret3 = source_plugins_[source_rank]->receiveFragmentData(frag->headerAddress() + header.num_words(), header.word_count - header.num_words());
			if (ret3 != source_rank)
			{
				TLOG_ERROR("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving System Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")" << TLOG_ENDL;
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving System Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";
			}

			switch (header.type)
			{
			case Fragment::EndOfDataFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				endOfDataCount = *(frag->dataBegin());
				TLOG_DEBUG("DataReceiverManager") << "EndOfData Fragment indicates that " << std::to_string(endOfDataCount) << " fragments are expected from rank " << source_rank 
					<< " (recvd " << std::to_string(recv_frag_count_.slotCount(source_rank)) << ")." << TLOG_ENDL;
				break;
			case Fragment::InitFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::Normal);
				shm_manager_->SetInitFragment(std::move(frag));
				break;
			case Fragment::EndOfRunFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				break;
			case Fragment::EndOfSubrunFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				break;
			case Fragment::ShutdownFragmentType:
				shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
				break;
			}
		}

		if (endOfDataCount <= recv_frag_count_.slotCount(source_rank))
		{
			running_sources_.erase(source_rank);
			return;
		}
	}
}
