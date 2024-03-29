#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_DataReceiverManager").c_str()

#include "artdaq/DAQdata/HostMap.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"

#include <boost/bind.hpp>
#include <boost/exception/all.hpp>
#include <boost/thread.hpp>

#include <chrono>
#include <iomanip>
#include <thread>
#include <utility>

artdaq::DataReceiverManager::DataReceiverManager(const fhicl::ParameterSet& pset, std::shared_ptr<SharedMemoryEventManager> shm)
    : stop_requested_(false)
    , stop_requested_time_(0)
    , recv_frag_count_()
    , recv_frag_size_()
    , recv_seq_count_()
    , receive_timeout_(pset.get<size_t>("receive_timeout_usec", 100000))
    , stop_timeout_ms_(pset.get<size_t>("stop_timeout_ms", 1500))
    , shm_manager_(std::move(std::move(shm)))
    , non_reliable_mode_enabled_(pset.get<bool>("non_reliable_mode", false))
    , non_reliable_mode_retry_count_(pset.get<size_t>("non_reliable_mode_retry_count", -1))
{
	TLOG(TLVL_DEBUG + 32) << "Constructor";
	auto enabled_srcs = pset.get<std::vector<int>>("enabled_sources", std::vector<int>());
	auto enabled_srcs_empty = enabled_srcs.empty();

	if (non_reliable_mode_enabled_)
	{
		TLOG(TLVL_WARNING) << "DataReceiverManager is configured to drop data after " << non_reliable_mode_retry_count_
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
			enabled_sources_[s] = true;
		}
	}

	hostMap_t host_map = MakeHostMap(pset);
	auto tcp_receive_buffer_size = pset.get<size_t>("tcp_receive_buffer_size", 0);
	auto max_fragment_size_words = pset.get<size_t>("max_fragment_size_words", 0);

	auto srcs = pset.get<fhicl::ParameterSet>("sources", fhicl::ParameterSet());
	for (auto& s : srcs.get_pset_names())
	{
		auto src_pset = srcs.get<fhicl::ParameterSet>(s);
		host_map = MakeHostMap(src_pset, host_map);
	}
	auto host_map_pset = MakeHostMapPset(host_map);
	fhicl::ParameterSet srcs_mod;
	for (auto& s : srcs.get_pset_names())
	{
		auto src_pset = srcs.get<fhicl::ParameterSet>(s);
		src_pset.erase("host_map");
		src_pset.put<std::vector<fhicl::ParameterSet>>("host_map", host_map_pset);

		if (tcp_receive_buffer_size != 0 && !src_pset.has_key("tcp_receive_buffer_size"))
		{
			src_pset.put<size_t>("tcp_receive_buffer_size", tcp_receive_buffer_size);
		}
		if (max_fragment_size_words != 0 && !src_pset.has_key("max_fragment_size_words"))
		{
			src_pset.put<size_t>("max_fragment_size_words", max_fragment_size_words);
		}

		srcs_mod.put<fhicl::ParameterSet>(s, src_pset);
	}

	for (auto& s : srcs_mod.get_pset_names())
	{
		try
		{
			auto transfer = std::unique_ptr<TransferInterface>(MakeTransferPlugin(srcs_mod, s,
			                                                                      TransferInterface::Role::kReceive));
			auto source_rank = transfer->source_rank();
			if (enabled_srcs_empty)
			{
				enabled_sources_[source_rank] = true;
			}
			else if (enabled_sources_.count(source_rank) == 0u)
			{
				enabled_sources_[source_rank] = false;
			}
			running_sources_[source_rank] = false;
			source_plugins_[source_rank] = std::move(transfer);
		}
		catch (const cet::exception& ex)
		{
			TLOG(TLVL_WARNING) << "cet::exception caught while setting up source " << s << ": " << ex.what();
		}
		catch (const std::exception& ex)
		{
			TLOG(TLVL_WARNING) << "std::exception caught while setting up source " << s << ": " << ex.what();
		}
		catch (...)
		{
			TLOG(TLVL_WARNING) << "Non-cet exception caught while setting up source " << s << ".";
		}
	}
	if (srcs.get_pset_names().empty())
	{
		TLOG(TLVL_ERROR) << "No sources configured!";
	}
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
	TLOG(TLVL_DEBUG + 33) << "~DataReceiverManager: BEGIN";
	stop_threads();
	shm_manager_.reset();
	TLOG(TLVL_DEBUG + 33) << "Destructor END";
}

void artdaq::DataReceiverManager::start_threads()
{
	stop_requested_ = false;
	if (shm_manager_)
	{
		shm_manager_->setRequestMode(artdaq::detail::RequestMessageMode::Normal);
	}
	for (auto& source : source_plugins_)
	{
		auto& rank = source.first;
		if ((enabled_sources_.count(rank) != 0u) && enabled_sources_[rank].load())
		{
			recv_frag_count_.setSlot(rank, 0);
			recv_frag_size_.setSlot(rank, 0);
			recv_seq_count_.setSlot(rank, 0);

			running_sources_[rank] = true;
			boost::thread::attributes attrs;
			attrs.set_stack_size(4096 * 2000);  // 2000 KB
			try
			{
				source_threads_[rank] = boost::thread(attrs, boost::bind(&DataReceiverManager::runReceiver_, this, rank));
				char tname[16];                                                   // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
				snprintf(tname, sizeof(tname) - 1, "%d-%d RECV", rank, my_rank);  // NOLINT
				tname[sizeof(tname) - 1] = '\0';                                  // assure term. snprintf is not too evil :)
				auto handle = source_threads_[rank].native_handle();
				pthread_setname_np(handle, tname);
			}
			catch (const boost::exception& e)
			{
				TLOG(TLVL_ERROR) << "Caught boost::exception starting Receiver " << rank << " thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
				std::cerr << "Caught boost::exception starting Receiver " << rank << " thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
				exit(5);
			}
		}
	}
}

void artdaq::DataReceiverManager::stop_threads()
{
	TLOG(TLVL_DEBUG + 33) << "stop_threads: BEGIN: Setting stop_requested to true, frags=" << count() << ", bytes=" << byteCount();

	stop_requested_time_ = TimeUtils::gettimeofday_us();
	stop_requested_ = true;

	auto initial_count = running_sources().size();
	TLOG(TLVL_DEBUG + 33) << "stop_threads: Waiting for " << initial_count << " running receiver threads to stop";
	auto wait_start = std::chrono::steady_clock::now();
	auto last_report = std::chrono::steady_clock::now();
	while (!running_sources().empty() && TimeUtils::GetElapsedTime(wait_start) < 60.0)
	{
		usleep(10000);
		if (TimeUtils::GetElapsedTime(last_report) > 1.0)
		{
			TLOG(TLVL_DEBUG + 32) << "stop_threads: Waited " << TimeUtils::GetElapsedTime(wait_start) << " s for " << initial_count
			                      << " receiver threads to end (" << running_sources().size() << " remain)";
			last_report = std::chrono::steady_clock::now();
		}
	}
	if (!running_sources().empty())
	{
		TLOG(TLVL_WARNING) << "stop_threads: Timeout expired while waiting for all receiver threads to end. There are "
		                   << running_sources().size() << " threads remaining.";
	}

	TLOG(TLVL_DEBUG + 33) << "stop_threads: Joining " << source_threads_.size() << " receiver threads";
	for (auto& source_thread : source_threads_)
	{
		TLOG(TLVL_DEBUG + 33) << "stop_threads: Joining thread for source_rank " << source_thread.first;
		try
		{
			if (source_thread.second.joinable())
			{
				source_thread.second.join();
			}
			else
			{
				TLOG(TLVL_ERROR) << "stop_threads: Thread for source rank " << source_thread.first << " is not joinable!";
			}
		}
		catch (...)
		{
			// IGNORED
		}
	}
	source_threads_.clear();  // To prevent error messages from shutdown-after-stop

	TLOG(TLVL_DEBUG + 33) << "stop_threads: END";
}

std::set<int> artdaq::DataReceiverManager::enabled_sources() const
{
	std::set<int> output;
	for (auto& src : enabled_sources_)
	{
		if (src.second)
		{
			output.insert(src.first);
		}
	}
	return output;
}

std::set<int> artdaq::DataReceiverManager::running_sources() const
{
	std::set<int> output;
	for (auto& src : running_sources_)
	{
		if (src.second)
		{
			output.insert(src.first);
		}
	}
	return output;
}

void artdaq::DataReceiverManager::runReceiver_(int source_rank)
{
	std::chrono::steady_clock::time_point start_time, after_header, before_body, after_body, end_time = std::chrono::steady_clock::now();
	int ret;
	detail::RawFragmentHeader header;
	size_t endOfDataCount = -1;
	auto sleep_time = receive_timeout_ / 100 > 100000 ? 100000 : receive_timeout_ / 100;
	if (sleep_time < 5000)
	{
		sleep_time = 5000;
	}
	auto max_retries = non_reliable_mode_retry_count_ * ceil(receive_timeout_ / sleep_time);

	while (!(stop_requested_ && TimeUtils::gettimeofday_us() - stop_requested_time_ > stop_timeout_ms_ * 1000) && (enabled_sources_.count(source_rank) != 0u))
	{
		TLOG(TLVL_DEBUG + 35) << "runReceiver_: Begin loop stop_requested_=" << stop_requested_ << ", stop_timeout_ms_=" << stop_timeout_ms_ << ", enabled_sources_.count(source_rank)=" << enabled_sources_.count(source_rank) << ", now - stop_requested_time_=" << (TimeUtils::gettimeofday_us() - stop_requested_time_);
		std::this_thread::yield();

		// Don't stop receiving until we haven't received anything for 1 second
		if (endOfDataCount <= recv_frag_count_.slotCount(source_rank) && !source_plugins_[source_rank]->isRunning())
		{
			TLOG(TLVL_DEBUG + 32) << "runReceiver_: End of Data conditions met, ending runReceiver loop";
			break;
		}

		start_time = std::chrono::steady_clock::now();

		TLOG(TLVL_DEBUG + 35) << "runReceiver_: Calling receiveFragmentHeader tmo=" << receive_timeout_;
		ret = source_plugins_[source_rank]->receiveFragmentHeader(header, receive_timeout_);
		TLOG(TLVL_DEBUG + 35) << "runReceiver_: Done with receiveFragmentHeader, ret=" << ret << " (should be " << source_rank << ")";
		if (ret != source_rank)
		{
			if (ret >= 0)
			{
				TLOG(TLVL_WARNING) << "Received Fragment from rank " << ret << ", but was expecting one from rank " << source_rank << "!";
			}
			else if (ret == TransferInterface::DATA_END)
			{
				TLOG(TLVL_ERROR) << "Transfer Plugin returned DATA_END, ending receive loop!";
				break;
			}
			if (*running_sources().begin() == source_rank)  // Only do this for the first sender in the running_sources_ map
			{
				TLOG(TLVL_DEBUG + 34) << "Calling SMEM::CheckPendingBuffers from DRM receiver thread for " << source_rank << " to make sure that things aren't stuck";
				shm_manager_->CheckPendingBuffers();
			}

			usleep(sleep_time);
			continue;  // Receive timeout or other oddness
		}

		after_header = std::chrono::steady_clock::now();

		if (Fragment::isUserFragmentType(header.type) || header.type == Fragment::DataFragmentType || header.type == Fragment::EmptyFragmentType || header.type == Fragment::ContainerFragmentType)
		{
			TLOG(TLVL_DEBUG + 33) << "Received Fragment Header from rank " << source_rank << ", sequence ID " << header.sequence_id << ", timestamp " << header.timestamp;
			RawDataType* loc = nullptr;
			size_t retries = 0;
			auto latency_s = header.getLatency(true);
			auto latency = latency_s.tv_sec + (latency_s.tv_nsec / 1000000000.0);
			while (loc == nullptr)  //&& TimeUtils::GetElapsedTimeMicroseconds(after_header)) < receive_timeout_)
			{
				loc = shm_manager_->WriteFragmentHeader(header);

				// Break here and outside of the loop to go to the cleanup steps at the end of runReceiver_
				if (loc == nullptr && stop_requested_)
				{
					break;
				}

				if (loc == nullptr)
				{
					usleep(sleep_time);
				}
				retries++;
				if (non_reliable_mode_enabled_ && retries > max_retries)
				{
					loc = shm_manager_->WriteFragmentHeader(header, true);
				}
			}
			// Break here to go to cleanup at the end of runReceiver_
			if (loc == nullptr && stop_requested_)
			{
				break;
			}
			if (loc == nullptr)
			{
				// Could not enqueue event!
				TLOG(TLVL_ERROR) << "runReceiver_: Could not get data location for event " << header.sequence_id;
				continue;
			}
			before_body = std::chrono::steady_clock::now();

			TLOG(TLVL_DEBUG + 35) << "runReceiver_: Calling receiveFragmentData from rank " << source_rank << ", sequence ID " << header.sequence_id << ", timestamp " << header.timestamp;
			auto ret2 = source_plugins_[source_rank]->receiveFragmentData(loc, header.word_count - header.num_words());
			TLOG(TLVL_DEBUG + 35) << "runReceiver_: Done with receiveFragmentData, ret2=" << ret2 << " (should be " << source_rank << ")";

			if (ret != ret2)
			{
				TLOG(TLVL_ERROR) << "Unexpected return code from receiveFragmentData after receiveFragmentHeader! (Expected: " << ret << ", Got: " << ret2 << ")";
				TLOG(TLVL_ERROR) << "Error receiving data from rank " << source_rank << ", data has been lost! Event " << header.sequence_id << " will most likely be Incomplete!";

				// Mark the Fragment as invalid
				header.valid = false;
				header.complete = false;

				shm_manager_->DoneWritingFragment(header);
				// throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader! (Expected: " << ret << ", Got: " << ret2 << ")";
				continue;
			}

			shm_manager_->DoneWritingFragment(header);
			TLOG(TLVL_DEBUG + 33) << "Done receiving fragment with sequence ID " << header.sequence_id << " from rank " << source_rank;

			recv_frag_count_.incSlot(source_rank);
			recv_frag_size_.incSlot(source_rank, header.word_count * sizeof(RawDataType));
			recv_seq_count_.setSlot(source_rank, header.sequence_id);
			if (endOfDataCount != static_cast<size_t>(-1))
			{
				TLOG(TLVL_DEBUG + 32) << "Received fragment " << header.sequence_id << " from rank " << source_rank
				                      << " (" << recv_frag_count_.slotCount(source_rank) << "/" << endOfDataCount << ")";
			}

			after_body = std::chrono::steady_clock::now();

			auto hdr_delta_t = TimeUtils::GetElapsedTime(start_time, after_header);
			auto store_delta_t = TimeUtils::GetElapsedTime(after_header, before_body);
			auto data_delta_t = TimeUtils::GetElapsedTime(before_body, after_body);
			auto delta_t = TimeUtils::GetElapsedTime(start_time, after_body);
			auto dead_t = TimeUtils::GetElapsedTime(end_time, start_time);
			auto recv_wait_t = hdr_delta_t - latency;

			uint64_t data_size = header.word_count * sizeof(RawDataType);
			auto header_size = header.num_words() * sizeof(RawDataType);

			if (metricMan)
			{  //&& recv_frag_count_.slotCount(source_rank) % 100 == 0) {
				TLOG(TLVL_DEBUG + 34) << "runReceiver_: Sending receive stats for rank " << source_rank;
				metricMan->sendMetric("Total Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Total Receive Size From Rank " + std::to_string(source_rank), data_size, "B", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Total Receive Rate From Rank " + std::to_string(source_rank), data_size / delta_t, "B/s", 5, MetricMode::Average);

				metricMan->sendMetric("Header Receive Time From Rank " + std::to_string(source_rank), hdr_delta_t, "s", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Header Receive Size From Rank " + std::to_string(source_rank), header_size, "B", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Header Receive Rate From Rank " + std::to_string(source_rank), header_size / hdr_delta_t, "B/s", 5, MetricMode::Average);

				auto payloadSize = data_size - header_size;
				metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), data_delta_t, "s", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), payloadSize, "B", 5, MetricMode::Accumulate);
				metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), payloadSize / data_delta_t, "B/s", 5, MetricMode::Average);

				metricMan->sendMetric("Data Receive Count From Rank " + std::to_string(source_rank), recv_frag_count_.slotCount(source_rank), "fragments", 3, MetricMode::LastPoint);

				metricMan->sendMetric("Total Shared Memory Wait Time From Rank " + std::to_string(source_rank), store_delta_t, "s", 3, MetricMode::Accumulate);
				metricMan->sendMetric("Avg Shared Memory Wait Time From Rank " + std::to_string(source_rank), store_delta_t, "s", 3, MetricMode::Average);
				metricMan->sendMetric("Avg Fragment Wait Time From Rank " + std::to_string(source_rank), dead_t, "s", 3, MetricMode::Average);

				metricMan->sendMetric("Rank", std::to_string(my_rank), "", 3, MetricMode::LastPoint);
				metricMan->sendMetric("App Name", app_name, "", 3, MetricMode::LastPoint);
				metricMan->sendMetric("Fragment Latency at Receive From Rank " + std::to_string(source_rank), latency, "s", 4, MetricMode::Average | MetricMode::Maximum);
				metricMan->sendMetric("Header Receive Wait Time From Rank" + std::to_string(source_rank), recv_wait_t, "s", 4, MetricMode::Average | MetricMode::Maximum | MetricMode::Minimum);

				TLOG(TLVL_DEBUG + 34) << "runReceiver_: Done sending receive stats for rank " << source_rank;
			}

			end_time = std::chrono::steady_clock::now();
		}
		else if (header.type == Fragment::EndOfDataFragmentType || header.type == Fragment::InitFragmentType || header.type == Fragment::EndOfRunFragmentType || header.type == Fragment::EndOfSubrunFragmentType || header.type == Fragment::ShutdownFragmentType)
		{
			TLOG(TLVL_DEBUG + 32) << "Received System Fragment from rank " << source_rank << " of type " << detail::RawFragmentHeader::SystemTypeToString(header.type) << ".";

			FragmentPtr frag(new Fragment(header.word_count - header.num_words()));
			memcpy(frag->headerAddress(), &header, header.num_words() * sizeof(RawDataType));
			auto ret3 = source_plugins_[source_rank]->receiveFragmentData(frag->headerAddress() + header.num_words(), header.word_count - header.num_words());  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
			if (ret3 != source_rank)
			{
				TLOG(TLVL_ERROR) << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving System Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";
				throw cet::exception("DataReceiverManager") << "Unexpected return code from receiveFragmentData after receiveFragmentHeader while receiving System Fragment! (Expected: " << source_rank << ", Got: " << ret3 << ")";  // NOLINT(cert-err60-cpp)
			}

			switch (header.type)
			{
				case Fragment::EndOfDataFragmentType:
					shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
					if (endOfDataCount == static_cast<size_t>(-1))
					{
						endOfDataCount = *(frag->dataBegin());
					}
					else
					{
						endOfDataCount += *(frag->dataBegin());
					}
					TLOG(TLVL_DEBUG + 32) << "EndOfData Fragment indicates that " << endOfDataCount << " fragments are expected from rank " << source_rank
					                      << " (recvd " << recv_frag_count_.slotCount(source_rank) << ").";
					break;
				case Fragment::InitFragmentType:
					TLOG(TLVL_DEBUG + 32) << "Received Init Fragment from rank " << source_rank << ".";
					shm_manager_->setRequestMode(detail::RequestMessageMode::Normal);
					shm_manager_->AddInitFragment(frag);
					break;
				case Fragment::EndOfRunFragmentType:
					shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
					// shm_manager_->endRun();
					break;
				case Fragment::EndOfSubrunFragmentType:
					// shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
					TLOG(TLVL_DEBUG + 32) << "Received EndOfSubrun Fragment from rank " << source_rank
					                      << " with sequence_id " << header.sequence_id << ".";
					if (header.sequence_id != Fragment::InvalidSequenceID)
					{
						shm_manager_->rolloverSubrun(header.sequence_id, header.timestamp);
					}
					else
					{
						shm_manager_->rolloverSubrun(recv_seq_count_.slotCount(source_rank), header.timestamp);
					}
					break;
				case Fragment::ShutdownFragmentType:
					shm_manager_->setRequestMode(detail::RequestMessageMode::EndOfRun);
					break;
				default:
					break;
			}
		}
	}

	source_plugins_[source_rank]->flush_buffers();

	TLOG(TLVL_DEBUG + 32) << "runReceiver_ " << source_rank << " receive loop exited";
	running_sources_[source_rank] = false;
}
