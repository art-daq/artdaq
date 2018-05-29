#define TRACE_NAME (app_name + "_FragmentReceiverManager").c_str()
#include "artdaq/DAQdata/Globals.hh"

#include <chrono>

#include "artdaq/DAQrate/FragmentReceiverManager.hh"
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
	, last_source_(-1)
{
	TLOG(TLVL_DEBUG) << "Constructor" ;
	auto enabled_srcs = pset.get<std::vector<int>>("enabled_sources", std::vector<int>());
	auto enabled_srcs_empty = enabled_srcs.size() == 0;
	if (enabled_srcs_empty)
	{
		TLOG(TLVL_INFO) << "enabled_sources not specified, assuming all sources enabled." ;
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
			TLOG(TLVL_WARNING) << "cet::exception caught while setting up source " << s << ": " << ex.what() ;
		}
		catch (std::exception ex)
		{
			TLOG(TLVL_WARNING) << "std::exception caught while setting up source " << s << ": " << ex.what() ;
		}
		catch (...)
		{
			TLOG(TLVL_WARNING) << "Non-cet exception caught while setting up source " << s << "." ;
		}
	}
	if (srcs.get_pset_names().size() == 0)
	{
		TLOG(TLVL_ERROR) << "No sources configured!" ;
	}
}

artdaq::FragmentReceiverManager::~FragmentReceiverManager()
{
	TLOG(TLVL_DEBUG) << "Destructor" ;
	TLOG(5) << "~FragmentReceiverManager: BEGIN: Setting stop_requested to true, frags=" << std::to_string(count()) << ", bytes=" << std::to_string(byteCount()) ;
	stop_requested_ = true;

	TLOG(5) << "~FragmentReceiverManager: Notifying all threads" ;
	output_cv_.notify_all();

	TLOG(5) << "~FragmentReceiverManager: Joining all threads" ;
	for (auto& s : source_threads_)
	{
		auto& thread = s.second;
		if (thread.joinable()) thread.join();
	}
	TLOG(5) << "~FragmentReceiverManager: DONE" ;
}

bool artdaq::FragmentReceiverManager::fragments_ready_() const
{
	for (auto& it : fragment_store_)
	{
		if (!enabled_sources_.count(it.first)) continue;
		if (!it.second.empty()) { return true; }
	}
	return false;
}

int artdaq::FragmentReceiverManager::get_next_source_() const
{
	//std::unique_lock<std::mutex> lck(fragment_store_mutex_);
	std::set<int> ready_sources;
	for (auto& it : fragment_store_)
	{
		if (!enabled_sources_.count(it.first)) continue;
		if (!it.second.empty()) {
			ready_sources.insert(it.first);
		}
	}

	if (ready_sources.size()) {
		auto iter = ready_sources.find(last_source_);
		if (iter == ready_sources.end() || ++iter == ready_sources.end()) {
			TLOG(TLVL_DEBUG) << "get_next_source returning " << *ready_sources.begin();
			last_source_ = *ready_sources.begin();
			return *ready_sources.begin();
		}

		TLOG(TLVL_DEBUG) << "get_next_source returning " << *iter;
		last_source_ = *iter;
		return *iter;
	}	

	TLOG(TLVL_DEBUG) << "get_next_source returning -1";
	return -1;
}

void artdaq::FragmentReceiverManager::start_threads()
{
	for (auto& source : source_plugins_)
	{
		auto& rank = source.first;
		if (enabled_sources_.count(rank))
		{
			source_threads_[rank] = boost::thread(&FragmentReceiverManager::runReceiver_, this, rank);
		}
	}
}

artdaq::FragmentPtr artdaq::FragmentReceiverManager::recvFragment(int& rank, size_t timeout_usec)
{
	TLOG(5) <<"recvFragment entered tmo=" << std::to_string(timeout_usec) << " us" ;

	if (timeout_usec == 0) timeout_usec = 1000000;

	auto ready = fragments_ready_();
	size_t waited = 0;
	auto wait_amount = timeout_usec / 1000 > 1000 ? timeout_usec / 1000 : 1000;
	TLOG(5) << "recvFragment fragment_ready_=" << ready << " before wait" ;
	while (!ready && waited < timeout_usec)
	{
		{
			std::unique_lock<std::mutex> lck(input_cv_mutex_);
			input_cv_.wait_for(lck, std::chrono::microseconds(wait_amount));
		}
		waited += wait_amount;
		ready = fragments_ready_();
		if (running_sources_.size() == 0) break;
	}
	TLOG(5) << "recvFragment fragment_ready_=" << ready << " after waited=" << std::to_string( waited) ;
	if (!ready)
	{
		TLOG(5)  << "recvFragment: No fragments ready, returning empty" ;
		rank = TransferInterface::RECV_TIMEOUT;
		return std::unique_ptr<Fragment>{};
	}

	int current_source = get_next_source_();
	FragmentPtr current_fragment = fragment_store_[current_source].front();
	output_cv_.notify_all();
	rank = current_source;

	if (current_fragment != nullptr)
		TLOG(5) << "recvFragment: Done  rank="<< rank <<", fragment size="<<std::to_string(current_fragment->size()) << " words, seqId="  << std::to_string( current_fragment->sequenceID()) ;
	return current_fragment;
}

void artdaq::FragmentReceiverManager::runReceiver_(int source_rank)
{
	running_sources_.insert(source_rank);
	while (!stop_requested_ && enabled_sources_.count(source_rank))
	{
		TLOG(16) << "runReceiver_ "<< source_rank << ": Begin loop" ;
		auto is_suppressed = suppress_noisy_senders_ && recv_seq_count_.slotCount(source_rank) > suppression_threshold_ + recv_seq_count_.minCount();
		while (!stop_requested_ && is_suppressed)
		{
			TLOG(6) << "runReceiver_: Suppressing receiver rank " <<  source_rank ;
			if (!is_suppressed) input_cv_.notify_all();
			else
			{
				std::unique_lock<std::mutex> lck(output_cv_mutex_);
				output_cv_.wait_for(lck, std::chrono::seconds(1));
			}
			is_suppressed = suppress_noisy_senders_ && recv_seq_count_.slotCount(source_rank) > suppression_threshold_ + recv_seq_count_.minCount();
		}
		if (stop_requested_)
		{
			running_sources_.erase(source_rank);
			return;
		}

		if (fragment_store_[source_rank].GetEndOfData() <= recv_frag_count_.slotCount(source_rank) && !source_plugins_[source_rank]->isRunning())
		{
			TLOG(TLVL_DEBUG) << "runReceiver_: EndOfData conditions satisfied, ending receive loop";
			running_sources_.erase(source_rank);
			return;
		}

		auto start_time = std::chrono::steady_clock::now();
		TLOG(16) << "runReceiver_: Calling receiveFragment" ;
		auto fragment = std::unique_ptr<Fragment>(new Fragment());
#if 0
		auto ret = source_plugins_[source_rank]->receiveFragment(*fragment, receive_timeout_);
		TLOG(16) << "runReceiver_: Done with receiveFragment, ret=" << ret << " (should be " << source_rank << ")" ;
		if (ret != source_rank) continue; // Receive timeout or other oddness
#else
		artdaq::detail::RawFragmentHeader hdr;
		auto ret1 = source_plugins_[source_rank]->receiveFragmentHeader(hdr, receive_timeout_);
		TLOG(16) << "runReceiver_: Done with receiveFragmentHeader, ret1=" << ret1 << " (should be " << source_rank << ")" ;

		if (ret1 != source_rank) continue; // Receive timeout or other oddness

		fragment->resize(hdr.word_count - hdr.num_words());
		memcpy(fragment->headerAddress(), &hdr, hdr.num_words() * sizeof(artdaq::RawDataType));
		auto ret2 = source_plugins_[source_rank]->receiveFragmentData(fragment->headerAddress() + hdr.num_words(), hdr.word_count - hdr.num_words());
		if (ret2 != ret1)
		{
			TLOG(TLVL_ERROR) << "ReceiveFragmentHeader returned " << ret1 << ", but ReceiveFragmentData returned " << ret2 ;
			continue;
		}
#endif


		if (fragment->type() == artdaq::Fragment::EndOfDataFragmentType)
		{
			TLOG(TLVL_TRACE) << "runReceiver_: EndOfData Fragment received!";
			fragment_store_[source_rank].SetEndOfData(*reinterpret_cast<size_t*>(fragment->dataBegin()));
		}
		else if(fragment->type() == artdaq::Fragment::DataFragmentType || fragment->type() == artdaq::Fragment::ContainerFragmentType || fragment->isUserFragmentType(fragment->type()))
		{
			TLOG(TLVL_TRACE) << "runReceiver_: Data Fragment received!";
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
			TLOG(6) << "runReceiver_: Sending receive stats" ;
			auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
			metricMan->sendMetric("Data Receive Time From Rank " + std::to_string(source_rank), delta_t, "s", 1, MetricMode::Accumulate);
			metricMan->sendMetric("Data Receive Size From Rank " + std::to_string(source_rank), static_cast<unsigned long>(fragment->size() * sizeof(RawDataType)), "B", 1, MetricMode::Accumulate);
			metricMan->sendMetric("Data Receive Rate From Rank " + std::to_string(source_rank), fragment->size() * sizeof(RawDataType) / delta_t, "B/s", 1, MetricMode::Average);
		}


		fragment_store_[source_rank].emplace_back(std::move(fragment));
		TLOG(TLVL_TRACE) << "runReceiver_: There are now " << fragment_store_[source_rank].size() << " Fragments stored from this source";
		input_cv_.notify_all();

	}

	running_sources_.erase(source_rank);
}
