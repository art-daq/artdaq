#include <chrono>

#include "proto/FragmentReceiverManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "cetlib_except/exception.h"

artdaq::FragmentReceiverManager::FragmentReceiverManager(const fhicl::ParameterSet& pset)
	: stop_requested_(false)
	, source_threads_()
	, source_plugins_()
	, enabled_sources_()
	, fragment_store_()
	, recv_frag_count_()
	, recv_frag_size_()
	, recv_seq_count_()
	, suppress_noisy_senders_(pset.get<bool>("auto_suppression_enabled", true))
	, suppression_threshold_(pset.get<size_t>("max_receive_difference", 50))
	, receive_timeout_(pset.get<size_t>("receive_timeout_usec", 100000))
{
	TLOG_DEBUG("FragmentReceiverManager") << "Constructor" << TLOG_ENDL;
	auto enabled_srcs = pset.get<std::vector<int>>("enabled_sources", std::vector<int>());
	auto enabled_srcs_empty = enabled_srcs.size() == 0;
	if (enabled_srcs_empty)
	{
		TLOG_INFO("FragmentReceiverManager") << "enabled_sources not specified, assuming all sources enabled." << TLOG_ENDL;
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
			fragment_store_[source_rank];
		}
		catch (cet::exception ex)
		{
			TLOG_WARNING("FragmentReceiverManager") << "cet::exception caught while setting up source " << s << ": " << ex.what() << TLOG_ENDL;
		}
		catch (std::exception ex)
		{
			TLOG_WARNING("FragmentReceiverManager") << "std::exception caught while setting up source " << s << ": " << ex.what() << TLOG_ENDL;
		}
		catch (...)
		{
			TLOG_WARNING("FragmentReceiverManager") << "Non-cet exception caught while setting up source " << s << "." << TLOG_ENDL;
		}
	}
	if (srcs.get_pset_names().size() == 0)
	{
		TLOG_ERROR("FragmentReceiverManager") << "No sources configured!" << TLOG_ENDL;
	}
}

artdaq::FragmentReceiverManager::~FragmentReceiverManager()
{
	TLOG_DEBUG("FragmentReceiverManager") << "Destructor" << TLOG_ENDL;
	TLOG_ARB(5, "FragmentReceiverManager") << "~FragmentReceiverManager: BEGIN: Setting stop_requested to true, frags=" << std::to_string(count()) << ", bytes=" << std::to_string(byteCount()) << TLOG_ENDL;
	stop_requested_ = true;

	TLOG_ARB(5, "FragmentReceiverManager") << "~FragmentReceiverManager: Notifying all threads" << TLOG_ENDL;
	output_cv_.notify_all();

	TLOG_ARB(5, "FragmentReceiverManager") << "~FragmentReceiverManager: Joining all threads" << TLOG_ENDL;
	for (auto& s : source_threads_)
	{
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
	TLOG_ARB(5, "FragmentReceiverManager") << "~FragmentReceiverManager: DONE" << TLOG_ENDL;
}

bool artdaq::FragmentReceiverManager::fragments_ready_() const
{
	return get_next_source_() != -1;
}

int artdaq::FragmentReceiverManager::get_next_source_() const
{
	//std::unique_lock<std::mutex> lck(fragment_store_mutex_);
	for (auto& it : fragment_store_)
	{
		if (!enabled_sources_.count(it.first)) continue;
		if (!it.second.empty()) return it.first;
	}
	return -1;
}

void artdaq::FragmentReceiverManager::start_threads()
{
	for (auto& source : source_plugins_)
	{
		auto& rank = source.first;
		if (enabled_sources_.count(rank))
		{
			source_threads_[rank] = std::thread(&FragmentReceiverManager::runReceiver_, this, rank);
		}
	}
}

artdaq::FragmentPtr artdaq::FragmentReceiverManager::recvFragment(int& rank, size_t timeout_usec)
{
	TLOG_ARB(5, "FragmentReceiverManager") <<"recvFragment entered tmo=" << std::to_string(timeout_usec) << " us" << TLOG_ENDL;

	if (timeout_usec == 0) timeout_usec = 1000000;

	auto ready = fragments_ready_();
	size_t waited = 0;
	auto wait_amount = timeout_usec / 1000 > 1000 ? timeout_usec / 1000 : 1000;
	TLOG_ARB(5, "FragmentReceiverManager") << "recvFragment fragment_ready_=" << ready << " before wait" << TLOG_ENDL;
	while (!ready && waited < timeout_usec)
	{
		{
			std::unique_lock<std::mutex> lck(input_cv_mutex_);
			input_cv_.wait_for(lck, std::chrono::microseconds(wait_amount));
		}
		waited += wait_amount;
		ready = fragments_ready_();
	}
	TLOG_ARB(5, "FragmentReceiverManager") << "recvFragment fragment_ready_=" << ready << " after waited=" << std::to_string( waited) << TLOG_ENDL;
	if (!ready)
	{
		TLOG_ARB(5, "FragmentReceiverManager")  << "recvFragment: No fragments ready, returning empty" << TLOG_ENDL;
		rank = TransferInterface::RECV_TIMEOUT;
		return std::unique_ptr<Fragment>{};
	}

	int current_source = get_next_source_();
	FragmentPtr current_fragment = fragment_store_[current_source].front();
	output_cv_.notify_all();
	rank = current_source;

	if (current_fragment != nullptr)
		TLOG_ARB(5, "FragmentReceiverManager") << "recvFragment: Done  rank="<< rank <<", fragment size="<<std::to_string(current_fragment->size()) << " words, seqId="  << std::to_string( current_fragment->sequenceID()) << TLOG_ENDL;
	return std::move(current_fragment);
}

void artdaq::FragmentReceiverManager::runReceiver_(int source_rank)
{
	while (!stop_requested_ && enabled_sources_.count(source_rank))
	{
		TLOG_ARB(16, "FragmentReceiverManager") << "runReceiver_ "<< source_rank << ": Begin loop" << TLOG_ENDL;
		auto is_suppressed = suppress_noisy_senders_ && recv_seq_count_.slotCount(source_rank) > suppression_threshold_ + recv_seq_count_.minCount();
		while (!stop_requested_ && is_suppressed)
		{
			TLOG_ARB(6, "FragmentReceiverManager") << "runReceiver_: Suppressing receiver rank " <<  source_rank << TLOG_ENDL;
			if (!is_suppressed) input_cv_.notify_all();
			else
			{
				std::unique_lock<std::mutex> lck(output_cv_mutex_);
				output_cv_.wait_for(lck, std::chrono::seconds(1));
			}
			is_suppressed = suppress_noisy_senders_ && recv_seq_count_.slotCount(source_rank) > suppression_threshold_ + recv_seq_count_.minCount();
		}
		if (stop_requested_) return;

		auto start_time = std::chrono::steady_clock::now();
		TLOG_ARB(16, "FragmentReceiverManager") << "runReceiver_: Calling receiveFragment" << TLOG_ENDL;
		auto fragment = std::unique_ptr<Fragment>(new Fragment());
#if 0
		auto ret = source_plugins_[source_rank]->receiveFragment(*fragment, receive_timeout_);
		TLOG_ARB(16, "FragmentReceiverManager") << "runReceiver_: Done with receiveFragment, ret=" << ret << " (should be " << source_rank << ")" << TLOG_ENDL;
		if (ret != source_rank) continue; // Receive timeout or other oddness
#else
		artdaq::detail::RawFragmentHeader hdr;
		auto ret1 = source_plugins_[source_rank]->receiveFragmentHeader(hdr, receive_timeout_);
		TLOG_ARB(16, "FragmentReceiverManager") << "runReceiver_: Done with receiveFragmentHeader, ret1=" << ret1 << " (should be " << source_rank << ")" << TLOG_ENDL;

		if (ret1 != source_rank) continue; // Receive timeout or other oddness

		fragment->resize(hdr.word_count - hdr.num_words());
		memcpy(fragment->headerAddress(), &hdr, hdr.num_words() * sizeof(artdaq::RawDataType));
		auto ret2 = source_plugins_[source_rank]->receiveFragmentData(fragment->headerAddress() + hdr.num_words(), hdr.word_count - hdr.num_words());
		if (ret2 != ret1)
		{
			TLOG_ERROR("FragmentReceiverManager") << "ReceiveFragmentHeader returned " << ret1 << ", but ReceiveFragmentData returned " << ret2 << TLOG_ENDL;
			continue;
		}
#endif


		if (fragment->type() == artdaq::Fragment::EndOfDataFragmentType)
		{
			fragment_store_[source_rank].SetEndOfData(*reinterpret_cast<size_t*>(fragment->dataBegin()));
		}
		else if(fragment->type() == artdaq::Fragment::DataFragmentType || fragment->type() == artdaq::Fragment::ContainerFragmentType || fragment->isUserFragmentType(fragment->type()))
		{
			recv_frag_count_.incSlot(source_rank);
			recv_frag_size_.incSlot(source_rank, fragment->size() * sizeof(RawDataType));
			recv_seq_count_.setSlot(source_rank, fragment->sequenceID());
		}
		else
		{
			continue;
		}



		if (metricMan)
		{//&& recv_frag_count_.slotCount(source_rank) % 100 == 0) {
			TLOG_ARB(6, "FragmentReceiverManager") << "runReceiver_: Sending receive stats" << TLOG_ENDL;
			auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
			metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 1);
			metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(fragment->size() * sizeof(RawDataType)), "B", 1);
			metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), fragment->size() * sizeof(RawDataType) / delta_t, "B/s", 1);
		}


		fragment_store_[source_rank].emplace_back(std::move(fragment));
		input_cv_.notify_all();

		if (fragment_store_[source_rank].GetEndOfData() <= recv_frag_count_.slotCount(source_rank))
		{
			return;
		}
	}
}
