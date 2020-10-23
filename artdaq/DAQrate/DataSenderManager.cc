#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_DataSenderManager").c_str()
#include "artdaq/DAQdata/HostMap.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include "artdaq/DAQdata/TCPConnect.hh"
#include "canvas/Utilities/Exception.h"

artdaq::DataSenderManager::DataSenderManager(const fhicl::ParameterSet& pset)
    : sent_frag_count_()
    , broadcast_sends_(pset.get<bool>("broadcast_sends", false))
    , non_blocking_mode_(pset.get<bool>("nonblocking_sends", false))
    , send_timeout_us_(pset.get<size_t>("send_timeout_usec", 5000000))
    , send_retry_count_(pset.get<size_t>("send_retry_count", 2))
    , should_stop_(false)
    , highest_sequence_id_routed_(0)
{
	TLOG(TLVL_DEBUG) << "Received pset: " << pset.to_string();

	// Validate parameters
	if (send_timeout_us_ == 0)
	{
		send_timeout_us_ = std::numeric_limits<size_t>::max();
	}

	auto rmConfig = pset.get<fhicl::ParameterSet>("routing_table_config", fhicl::ParameterSet());
	table_receiver_.reset(new TableReceiver(rmConfig));

	hostMap_t host_map = MakeHostMap(pset);
	auto tcp_send_buffer_size = pset.get<size_t>("tcp_send_buffer_size", 0);
	auto max_fragment_size_words = pset.get<size_t>("max_fragment_size_words", 0);

	auto dests = pset.get<fhicl::ParameterSet>("destinations", fhicl::ParameterSet());
	for (auto& d : dests.get_pset_names())
	{
		auto dest_pset = dests.get<fhicl::ParameterSet>(d);
		host_map = MakeHostMap(dest_pset, host_map);
	}
	auto host_map_pset = MakeHostMapPset(host_map);
	fhicl::ParameterSet dests_mod;
	for (auto& d : dests.get_pset_names())
	{
		auto dest_pset = dests.get<fhicl::ParameterSet>(d);
		dest_pset.erase("host_map");
		dest_pset.put<std::vector<fhicl::ParameterSet>>("host_map", host_map_pset);

		if (tcp_send_buffer_size != 0 && !dest_pset.has_key("tcp_send_buffer_size"))
		{
			dest_pset.put<size_t>("tcp_send_buffer_size", tcp_send_buffer_size);
		}
		if (max_fragment_size_words != 0 && !dest_pset.has_key("max_fragment_size_words"))
		{
			dest_pset.put<size_t>("max_fragment_size_words", max_fragment_size_words);
		}

		dests_mod.put<fhicl::ParameterSet>(d, dest_pset);
	}

	for (auto& d : dests_mod.get_pset_names())
	{
		try
		{
			auto transfer = MakeTransferPlugin(dests_mod, d, TransferInterface::Role::kSend);
			auto destination_rank = transfer->destination_rank();
			destinations_.emplace(destination_rank, std::move(transfer));
		}
		catch (const std::invalid_argument&)
		{
			TLOG(TLVL_DEBUG) << "Invalid destination specification: " << d;
		}
		catch (const cet::exception& ex)
		{
			TLOG(TLVL_WARNING) << "Caught cet::exception: " << ex.what();
		}
		catch (...)
		{
			TLOG(TLVL_WARNING) << "Non-cet exception while setting up TransferPlugin: " << d << ".";
		}
	}
	if (destinations_.empty())
	{
		TLOG(TLVL_ERROR) << "No destinations specified!";
	}
	else
	{
		auto enabled_dests = pset.get<std::vector<size_t>>("enabled_destinations", std::vector<size_t>());
		if (enabled_dests.empty())
		{
			TLOG(TLVL_INFO) << "enabled_destinations not specified, assuming all destinations enabled.";
			for (auto& d : destinations_)
			{
				enabled_destinations_.insert(d.first);
			}
		}
		else
		{
			for (auto& d : enabled_dests)
			{
				enabled_destinations_.insert(d);
			}
		}
	}
}

artdaq::DataSenderManager::~DataSenderManager()
{
	TLOG(TLVL_DEBUG) << "Shutting down DataSenderManager BEGIN";
	should_stop_ = true;
	for (auto& dest : enabled_destinations_)
	{
		if (destinations_.count(dest) != 0u)
		{
			auto sts = destinations_[dest]->transfer_fragment_reliable_mode(std::move(*Fragment::eodFrag(sent_frag_count_.slotCount(dest))));
			if (sts != TransferInterface::CopyStatus::kSuccess)
			{
				TLOG(TLVL_ERROR) << "Error sending EOD Fragment to sender rank " << dest;
			}
			//  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
		}
	}
	TLOG(TLVL_DEBUG) << "Shutting down DataSenderManager END. Sent " << count() << " fragments.";
}

size_t artdaq::DataSenderManager::GetRoutingTableEntryCount() const
{
	return table_receiver_->GetRoutingTableEntryCount();
}

size_t artdaq::DataSenderManager::GetRemainingRoutingTableEntries() const
{
	return table_receiver_->GetRemainingRoutingTableEntries();
}

int artdaq::DataSenderManager::calcDest_(Fragment::sequence_id_t sequence_id) const
{
	if (enabled_destinations_.empty())
	{
		return TableReceiver::ROUTING_FAILED;  // No destinations configured.
	}

	if (table_receiver_->RoutingManagerEnabled())
	{
		TLOG(15) << "calcDest_ use_routing_manager check for routing info for seqID=" << sequence_id << " should_stop_=" << should_stop_;
		return table_receiver_->GetRoutingTableEntry(sequence_id);
	}
	if (enabled_destinations_.size() == 1)
	{
		return *enabled_destinations_.begin();  // Trivial case
	}
	auto index = sequence_id % enabled_destinations_.size();
	auto it = enabled_destinations_.begin();
	for (; index > 0; --index)
	{
		++it;
		if (it == enabled_destinations_.end())
		{
			it = enabled_destinations_.begin();
		}
	}
	return *it;
}

void artdaq::DataSenderManager::RemoveRoutingTableEntry(Fragment::sequence_id_t seq)
{
	TLOG(15) << "RemoveRoutingTableEntry: Removing sequence ID " << seq << " from routing table. Sent " << GetSentSequenceIDCount(seq) << " Fragments with this Sequence ID.";
	table_receiver_->RemoveRoutingTableEntry(seq);

	std::unique_lock<std::mutex> lck(sent_sequence_id_mutex_);
	if (sent_sequence_id_count_.find(seq) != sent_sequence_id_count_.end())
	{
		sent_sequence_id_count_.erase(sent_sequence_id_count_.find(seq));
	}
}

size_t artdaq::DataSenderManager::GetSentSequenceIDCount(Fragment::sequence_id_t seq)
{
	std::unique_lock<std::mutex> lck(sent_sequence_id_mutex_);
	if (sent_sequence_id_count_.count(seq) == 0u)
	{
		return 0;
	}
	return sent_sequence_id_count_[seq];
}

std::pair<int, artdaq::TransferInterface::CopyStatus> artdaq::DataSenderManager::sendFragment(Fragment&& frag)
{
	// Precondition: Fragment must be complete and consistent (including
	// header information).
	auto start_time = std::chrono::steady_clock::now();
	if (frag.type() == Fragment::EndOfDataFragmentType)
	{
		throw cet::exception("LogicError")  // NOLINT(cert-err60-cpp)
		    << "EOD fragments should not be sent on as received: "
		    << "use sendEODFrag() instead.";
	}
	size_t seqID = frag.sequenceID();
	size_t fragSize = frag.sizeBytes();
	auto latency_s = frag.getLatency(true);
	auto isSystemBroadcast = frag.type() == Fragment::EndOfRunFragmentType || frag.type() == Fragment::EndOfSubrunFragmentType || frag.type() == Fragment::InitFragmentType;

	double latency = latency_s.tv_sec + (latency_s.tv_nsec / 1000000000.0);
	TLOG(13) << "sendFragment start frag.fragmentHeader()=" << std::hex << static_cast<void*>(frag.headerBeginBytes()) << ", szB=" << std::dec << fragSize
	         << ", seqID=" << seqID << ", fragID=" << frag.fragmentID() << ", type=" << frag.typeString();
	int dest = TableReceiver::ROUTING_FAILED;
	auto outsts = TransferInterface::CopyStatus::kSuccess;
	if (broadcast_sends_ || isSystemBroadcast)
	{
		for (auto& bdest : enabled_destinations_)
		{
			TLOG(TLVL_TRACE) << "sendFragment: Sending fragment with seqId " << seqID << " to destination " << bdest << " (broadcast)";
			// Gross, we have to copy.
			auto sts = TransferInterface::CopyStatus::kTimeout;
			size_t retries = 0;  // Have NOT yet tried, so retries <= send_retry_count_ will have it RETRY send_retry_count_ times
			while (sts == TransferInterface::CopyStatus::kTimeout && retries <= send_retry_count_)
			{
				if (!non_blocking_mode_)
				{
					sts = destinations_[bdest]->transfer_fragment_reliable_mode(Fragment(frag));
				}
				else
				{
					sts = destinations_[bdest]->transfer_fragment_min_blocking_mode(frag, send_timeout_us_);
				}
				++retries;
			}
			if (sts != TransferInterface::CopyStatus::kSuccess)
			{
				outsts = sts;
			}
			sent_frag_count_.incSlot(bdest);
		}
	}
	else if (non_blocking_mode_)
	{
		dest = calcDest_(seqID);
		if (dest == TableReceiver::ROUTING_FAILED)
		{
			TLOG(TLVL_WARNING) << "Could not get destination for seqID " << seqID;
		}

		if (dest != TableReceiver::ROUTING_FAILED && (destinations_.count(dest) != 0u) && (enabled_destinations_.count(dest) != 0u))
		{
			TLOG(TLVL_TRACE) << "sendFragment: Sending fragment with seqId " << seqID << " to destination " << dest;
			TransferInterface::CopyStatus sts = TransferInterface::CopyStatus::kErrorNotRequiringException;
			auto lastWarnTime = std::chrono::steady_clock::now();
			size_t retries = 0;  // Have NOT yet tried, so retries <= send_retry_count_ will have it RETRY send_retry_count_ times
			while (sts != TransferInterface::CopyStatus::kSuccess && retries <= send_retry_count_)
			{
				sts = destinations_[dest]->transfer_fragment_min_blocking_mode(frag, send_timeout_us_);
				if (sts != TransferInterface::CopyStatus::kSuccess && TimeUtils::GetElapsedTime(lastWarnTime) >= 1)
				{
					TLOG(TLVL_WARNING) << "sendFragment: Sending fragment " << seqID << " to destination " << dest << " failed! Retrying...";
					lastWarnTime = std::chrono::steady_clock::now();
				}
				++retries;
			}
			if (sts != TransferInterface::CopyStatus::kSuccess)
			{
				outsts = sts;
			}
			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
		}
		else if (!should_stop_)
		{
			TLOG(TLVL_ERROR) << "(in non_blocking) calcDest returned invalid destination rank " << dest << "! This event has been lost: " << seqID
			                 << ". enabled_destinantions_.size()=" << enabled_destinations_.size();
		}
	}
	else
	{
		auto start = std::chrono::steady_clock::now();
		while (!should_stop_ && dest == TableReceiver::ROUTING_FAILED)
		{
			dest = calcDest_(seqID);
			if (dest == TableReceiver::ROUTING_FAILED)
			{
				TLOG(TLVL_WARNING) << "Could not get destination for seqID " << seqID << ", send number " << sent_frag_count_.count() << ", retrying. Waited " << TimeUtils::GetElapsedTime(start) << " s for routing information.";
				usleep(10000);
			}
		}
		if (dest != TableReceiver::ROUTING_FAILED && (destinations_.count(dest) != 0u) && (enabled_destinations_.count(dest) != 0u))
		{
			TLOG(TLVL_DEBUG + 2) << "DataSenderManager::sendFragment: Sending fragment with seqId " << seqID << " to destination " << dest;
			TransferInterface::CopyStatus sts = TransferInterface::CopyStatus::kErrorNotRequiringException;

			sts = destinations_[dest]->transfer_fragment_reliable_mode(std::move(frag));
			if (sts != TransferInterface::CopyStatus::kSuccess)
			{
				TLOG(TLVL_ERROR) << "sendFragment: Sending fragment " << seqID << " to destination "
				                 << dest << " failed! Data has been lost!";
			}

			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
			outsts = sts;
		}
		else if (!should_stop_)
		{
			TLOG(TLVL_ERROR) << "calcDest returned invalid destination rank " << dest << "! This event has been lost: " << seqID
			                 << ". enabled_destinantions_.size()=" << enabled_destinations_.size();
		}
	}

	if (!isSystemBroadcast)
	{
		std::unique_lock<std::mutex> lck(sent_sequence_id_mutex_);
		sent_sequence_id_count_[seqID]++;
	}

	auto delta_t = TimeUtils::GetElapsedTime(start_time);

	if (metricMan)
	{
		TLOG(TLVL_DEBUG + 2) << "sendFragment: sending metrics";
		metricMan->sendMetric("Data Send Time to Rank " + std::to_string(dest), delta_t, "s", 5, MetricMode::Accumulate);
		metricMan->sendMetric("Data Send Size to Rank " + std::to_string(dest), fragSize, "B", 5, MetricMode::Accumulate);
		metricMan->sendMetric("Data Send Rate to Rank " + std::to_string(dest), fragSize / delta_t, "B/s", 5, MetricMode::Average);
		metricMan->sendMetric("Data Send Count to Rank " + std::to_string(dest), sent_frag_count_.slotCount(dest), "fragments", 3, MetricMode::LastPoint);
		metricMan->sendMetric("Fragment Latency at Send", latency, "s", 4, MetricMode::Average | MetricMode::Maximum);
	}

	TLOG(TLVL_DEBUG + 2) << "sendFragment: Done sending fragment " << seqID << " to dest=" << dest;
	return std::make_pair(dest, outsts);
}  // artdaq::DataSenderManager::sendFragment
