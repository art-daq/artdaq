#include <chrono>

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "cetlib_except/exception.h"
#include <iomanip>

artdaq::DataReceiverManager::DataReceiverManager(const fhicl::ParameterSet& pset, std::shared_ptr<SharedMemoryEventManager> shm)
	: stop_requested_(false)
	, source_threads_()
	, source_plugins_()
	, enabled_sources_()
	, recv_frag_count_()
	, recv_frag_size_()
	, recv_seq_count_()
	, receive_timeout_(pset.get<size_t>("receive_timeout_usec", 100000))
	, shm_manager_(shm)
{
	TLOG_DEBUG("DataReceiverManager") << "Constructor" << TLOG_ENDL;
	auto enabled_srcs = pset.get<std::vector<int>>("enabled_sources", std::vector<int>());
	auto enabled_srcs_empty = enabled_srcs.size() == 0;
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
	TLOG_DEBUG("DataReceiverManager") << "Destructor" << TLOG_ENDL;
	TRACE(5, "~DataReceiverManager: BEGIN: Setting stop_requested to true, frags=%zu, bytes=%zu", count(), byteCount());
	stop_requested_ = true;

	TRACE(5, "~DataReceiverManager: Joining all threads");
	for (auto& s : source_threads_)
	{
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
	TRACE(5, "~DataReceiverManager: DONE");
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

	while (!stop_requested_ && enabled_sources_.count(source_rank))
	{
		TRACE(16, "DataReceiverManager::runReceiver_: Begin loop");
		if (stop_requested_) return;

		start_time = std::chrono::steady_clock::now();

		TRACE(16, "DataReceiverManager::runReceiver_: Calling receiveFragmentHeader");
		ret = source_plugins_[source_rank]->receiveFragmentHeader(header, receive_timeout_);
		TRACE(16, "DataReceiverManager::runReceiver_: Done with receiveFragmentHeader, ret=%d (should be %d)", ret, source_rank);
		if (ret != source_rank) continue; // Receive timeout or other oddness

		after_header = std::chrono::steady_clock::now();

		if (Fragment::isUserFragmentType(header.type) || header.type == Fragment::DataFragmentType) {
			TLOG_TRACE("DataReceiverManager") << "Received Fragment Header from rank " << source_rank << "." << TLOG_ENDL;
			RawDataType* loc = nullptr;
			while (loc == nullptr) {
				loc = shm_manager_->WriteFragmentHeader(header);
				if (loc == nullptr) usleep(sleep_time);
				if (stop_requested_) return;
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
			recv_frag_count_.incSlot(source_rank);
			recv_frag_size_.incSlot(source_rank, header.word_count * sizeof(RawDataType));
			recv_seq_count_.setSlot(source_rank, header.sequence_id);

			if (metricMan)
			{//&& recv_frag_count_.slotCount(source_rank) % 100 == 0) {
				TRACE(6, "DataReceiverManager::runReceiver_: Sending receive stats");
				delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
				hdr_delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(after_header - start_time).count();
				store_delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(before_body - after_header).count();
				data_delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - before_body).count();
				metricMan->sendMetric("Total Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 1);
				metricMan->sendMetric("Total Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(header.word_count * sizeof(RawDataType)), "B", 1);
				metricMan->sendMetric("Total Receive Rate From Rank " + std::to_string(source_rank), header.word_count * sizeof(RawDataType) / delta_t, "B/s", 1);

				metricMan->sendMetric("Header Receive Time From Rank " + std::to_string(source_rank), hdr_delta_t, "s", 1);
				metricMan->sendMetric("Header Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(header.num_words() * sizeof(RawDataType)), "B", 1);
				metricMan->sendMetric("Header Receive Rate From Rank " + std::to_string(source_rank), header.num_words() * sizeof(RawDataType) / hdr_delta_t, "B/s", 1);

				metricMan->sendMetric("Shared Memory Wait Time From Rank " + std::to_string(source_rank), store_delta_t, "s", 1);

				metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), data_delta_t, "s", 1);
				metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>((header.word_count - header.num_words()) * sizeof(RawDataType)), "B", 1);
				metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), (header.word_count - header.num_words()) * sizeof(RawDataType) / data_delta_t, "B/s", 1);
				TRACE(6, "DataReceiverManager::runReceiver_: Done sending receive stats");
			}
		}
		else if (header.type == Fragment::EndOfDataFragmentType)
		{
			TLOG_DEBUG("DataReceiverManager") << "Received EndOfData Fragment from rank " << source_rank << "." << TLOG_ENDL;
			shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
			Fragment frag(header.word_count - header.num_words());
			auto ret3 = source_plugins_[source_rank]->receiveFragmentData(frag.headerAddress() + header.num_words(), 1);
			if (ret3 == source_rank)
			{
				endOfDataCount = *frag.dataBegin();
			}
			else
			{
				TLOG_ERROR("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving EndOfData Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")" << TLOG_ENDL;
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving EndOfData Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";
			}
		}
		else if (header.type == Fragment::InitFragmentType) {
			TLOG_DEBUG("DataReceiverManager") << "Received Init Fragment from rank " << source_rank << "." << TLOG_ENDL;
			FragmentPtr frag(new Fragment(header.word_count - header.num_words()));
			memcpy(frag->headerAddress(), &header, header.num_words() * sizeof(RawDataType));
			auto ret3 = source_plugins_[source_rank]->receiveFragmentData(frag->headerAddress() + header.num_words(), 1);
			if (ret3 == source_rank)
			{
				shm_manager_->SetInitFragment(std::move(frag));
			}
			else
			{
				TLOG_ERROR("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving EndOfData Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")" << TLOG_ENDL;
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving EndOfData Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";
			}
			
		}


		if (endOfDataCount <= recv_frag_count_.slotCount(source_rank))
		{
			running_sources_.erase(source_rank);
			return;
		}
	}
}