#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq/DAQdata/Globals.hh"

#include <chrono>
#include "canvas/Utilities/Exception.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <poll.h>
#include <sys/socket.h>
#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq/DAQdata/TCPConnect.hh"

artdaq::DataSenderManager::DataSenderManager(const fhicl::ParameterSet& pset)
	: destinations_()
	, enabled_destinations_()
	, sent_frag_count_()
	, broadcast_sends_(pset.get<bool>("broadcast_sends", false))
	, non_blocking_mode_(pset.get<bool>("nonblocking_sends", false))
	, routing_master_mode_(detail::RoutingMasterMode::INVALID)
	, should_stop_(false)
	, ack_socket_(-1)
	, table_socket_(-1)
{
	TLOG_DEBUG("DataSenderManager") << "Received pset: " << pset.to_string() << TLOG_ENDL;
	auto rmConfig = pset.get<fhicl::ParameterSet>("routing_table_config", fhicl::ParameterSet());
	use_routing_master_ = rmConfig.get<bool>("use_routing_master", false);
	table_port_ = rmConfig.get<int>("table_update_port", 35556);
	table_address_ = rmConfig.get<std::string>("table_update_address", "227.128.12.28");
	ack_port_ = rmConfig.get<int>("table_acknowledge_port", 35557);
	ack_address_ = rmConfig.get<std::string>("routing_master_hostname", "localhost");
	routing_timeout_ms_ = (rmConfig.get<int>("routing_timeout_ms", 1000));


	auto dests = pset.get<fhicl::ParameterSet>("destinations", fhicl::ParameterSet());
	for (auto& d : dests.get_pset_names())
	{
		try
		{
		  auto transfer = MakeTransferPlugin(dests, d, TransferInterface::Role::kSend);
		  auto destination_rank = transfer->destination_rank();
		  destinations_.emplace( destination_rank, std::move(transfer));
		}
		catch (std::invalid_argument)
		{
			TRACE(3, "Invalid destination specification: " + d);
		}
		catch (cet::exception ex)
		{
			TLOG_WARNING("DataSenderManager") << "Caught cet::exception: " << ex.what() << TLOG_ENDL;
		}
		catch (...)
		{
			TLOG_WARNING("DataSenderManager") << "Non-cet exception while setting up TransferPlugin: " << d << "." << TLOG_ENDL;
		}
	}
	if (destinations_.size() == 0)
	{
		TLOG_ERROR("DataSenderManager") << "No destinations specified!" << TLOG_ENDL;
	}
	else
	{
		auto enabled_dests = pset.get<std::vector<size_t>>("enabled_destinations", std::vector<size_t>());
		if (enabled_dests.size() == 0)
		{
			TLOG_INFO("DataSenderManager") << "enabled_destinations not specified, assuming all destinations enabled." << TLOG_ENDL;
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
	if (use_routing_master_) startTableReceiverThread_();
}

artdaq::DataSenderManager::~DataSenderManager()
{
	TLOG_DEBUG("DataSenderManager") << "Shutting down DataSenderManager BEGIN" << TLOG_ENDL;
	should_stop_ = true;
	for (auto& dest : enabled_destinations_)
	{
		if (destinations_.count(dest))
		{
			destinations_[dest]->moveFragment(std::move(*Fragment::eodFrag(sent_frag_count_.slotCount(dest))));
			//  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
		}
	}
	if (routing_thread_.joinable()) routing_thread_.join();
	TLOG_DEBUG("DataSenderManager") << "Shutting down DataSenderManager END. Sent " << count() << " fragments." << TLOG_ENDL;
}


void artdaq::DataSenderManager::setupTableListener_()
{
	table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (!table_socket_)
	{
		TLOG_ERROR("DataSenderManager") << "Error creating socket for receiving table updates!" << TLOG_ENDL;
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		TLOG_ERROR("DataSenderManager") << " Unable to enable port reuse on request socket" << TLOG_ENDL;
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(table_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(table_socket_, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
	{
		TLOG_ERROR("DataSenderManager") << "Cannot bind request socket to port " << table_port_ << TLOG_ENDL;
		exit(1);
	}

	struct ip_mreq mreq;
	int sts = ResolveHost(table_address_.c_str(), mreq.imr_multiaddr);
	if (sts == -1)
	{
		TLOG_ERROR("DataSenderManager") << "Unable to resolve multicast address for table updates" << TLOG_ENDL;
		exit(1);
	}
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (setsockopt(table_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
	{
		TLOG_ERROR("DataSenderManager") << "Unable to join multicast group" << TLOG_ENDL;
		exit(1);
	}
}
void artdaq::DataSenderManager::startTableReceiverThread_()
{
	if (routing_thread_.joinable()) routing_thread_.join();
	TLOG_INFO("DataSenderManager") << "Starting Routing Thread" << TLOG_ENDL;
	routing_thread_ = std::thread(&DataSenderManager::receiveTableUpdatesLoop_, this);
}
void artdaq::DataSenderManager::receiveTableUpdatesLoop_()
{
	while (true)
	{
		if (should_stop_)
		{
			TLOG_DEBUG("DataSenderManager") << "receiveTableUpdatesLoop: should_stop is " << std::boolalpha << should_stop_ << ", stopping" << TLOG_ENDL;
			return;
		}

		TRACE(4, "DataSenderManager::receiveTableUpdatesLoop: Polling Request socket for new requests");
		if (table_socket_ == -1)
		{
			TLOG_DEBUG("DataSenderManager") << "Opening table listener socket" << TLOG_ENDL;
			setupTableListener_();
		}
		if (table_socket_ == -1)
		{
			TLOG_DEBUG("DataSenderManager") << "The listen socket was not opened successfully." << TLOG_ENDL;
			return;
		}
		if (ack_socket_ == -1)
		{
			ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
			auto sts = ResolveHost(ack_address_.c_str(), ack_port_, ack_addr_);
			if (sts == -1)
			{
				TLOG_ERROR("DataSenderManager") << "Unable to resolve routing_master_address" << TLOG_ENDL;
				exit(1);
			}
			TLOG_DEBUG("DataSenderManager") << "Ack socket is fd " << ack_socket_ << TLOG_ENDL;
		}

		struct pollfd fd;
		fd.fd = table_socket_;
		fd.events = POLLIN | POLLPRI;

		auto res = poll(&fd, 1, 1000);
		if (res > 0) {
			auto first = artdaq::Fragment::InvalidSequenceID;
			auto last = artdaq::Fragment::InvalidSequenceID;
			artdaq::detail::RoutingPacketHeader hdr;

			TLOG_DEBUG("DataSenderManager") << "Going to receive RoutingPacketHeader" << TLOG_ENDL;
			auto stss = recvfrom(table_socket_, &hdr, sizeof(artdaq::detail::RoutingPacketHeader), 0, NULL, NULL);
			TLOG_DEBUG("DataSenderManager") << "Received " << std::to_string(stss) << " bytes. (sizeof(RoutingPacketHeader) == " << std::to_string(sizeof(detail::RoutingPacketHeader)) << TLOG_ENDL;

			TLOG_DEBUG("DataSenderManager") << "Checking for valid header" << TLOG_ENDL;
			if (hdr.header == ROUTING_MAGIC) {
				if(routing_master_mode_ != detail::RoutingMasterMode::INVALID && routing_master_mode_ != hdr.mode)
				{
					TLOG_ERROR("DataSenderManager") << "Received table has different RoutingMasterMode than expected!" << TLOG_ENDL;
					exit(1);
				}
				routing_master_mode_ = hdr.mode;

				artdaq::detail::RoutingPacket buffer(hdr.nEntries);
				TLOG_DEBUG("DataSenderManager") << "Receiving data buffer" << TLOG_ENDL;
				auto sts = recv(table_socket_, &buffer[0], sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries, 0);
				assert(static_cast<size_t>(sts) == sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);
				TRACE(6, "Received a packet of %zu bytes", sts);

				first = buffer[0].sequence_id;
				last = buffer[buffer.size() - 1].sequence_id;

				if (first + hdr.nEntries - 1 != last)
				{
					TLOG_ERROR("DataSenderManager") << "Skipping this RoutingPacket because the first (" << first << ") and last (" << last << ") entries are inconsistent (sz=" << hdr.nEntries << ")!" << TLOG_ENDL;
					continue;
				}
				auto thisSeqID = first;

				if (routing_table_.count(last) == 0) {
					for (auto entry : buffer)
					{
						if (thisSeqID != entry.sequence_id)
						{
							TLOG_ERROR("DataSenderManager") << "Aborting processing of this RoutingPacket because I encountered an inconsistent entry (seqid=" << entry.sequence_id << ", expected=" << thisSeqID << ")!" << TLOG_ENDL;
							last = thisSeqID - 1;
							break;
						}
						thisSeqID++;
						if (routing_table_.count(entry.sequence_id))
						{
							if (routing_table_[entry.sequence_id] != entry.destination_rank)
							{
								TLOG_ERROR("DataSenderManager") << "Detected routing table corruption! Recevied update specifying that sequence ID " << entry.sequence_id
									<< " should go to rank " << entry.destination_rank << ", but I had already been told to send it to " << routing_table_[entry.sequence_id] << "!"
									<< " I will use the original value!" << TLOG_ENDL;
							}
							continue;
						}
						routing_table_[entry.sequence_id] = entry.destination_rank;
						TLOG_DEBUG("DataSenderManager") << "DataSenderManager " << std::to_string(my_rank) << ": received update: SeqID " << std::to_string(entry.sequence_id) << " -> Rank " << std::to_string(entry.destination_rank) << TLOG_ENDL;
					}
				}

				artdaq::detail::RoutingAckPacket ack;
				ack.rank = my_rank;
				ack.first_sequence_id = first;
				ack.last_sequence_id = last;

				TLOG_DEBUG("DataSenderManager") << "Sending RoutingAckPacket with first= " << std::to_string(first) << " and last= " << std::to_string(last) << " to " << ack_address_ << ", port " << ack_port_ << " (my_rank = " << my_rank << ")"<< TLOG_ENDL;
				TLOG_DEBUG("DataSenderManager") << "There are now " << routing_table_.size() << " entries in the Routing Table" << TLOG_ENDL;
				sendto(ack_socket_, &ack, sizeof(artdaq::detail::RoutingAckPacket), 0, (struct sockaddr *)&ack_addr_, sizeof(ack_addr_));
			}
		}
	}
}

size_t artdaq::DataSenderManager::GetRoutingTableEntryCount() const
{
	std::unique_lock<std::mutex> lck(routing_mutex_);
	return routing_table_.size();
}

int artdaq::DataSenderManager::calcDest_(Fragment::sequence_id_t sequence_id) const
{
	if (enabled_destinations_.size() == 0) return TransferInterface::RECV_TIMEOUT; // No destinations configured.
	if (enabled_destinations_.size() == 1) return *enabled_destinations_.begin(); // Trivial case

	if (use_routing_master_)
	{
		auto start = std::chrono::steady_clock::now();
		while (routing_timeout_ms_ <= 0 || std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() < routing_timeout_ms_)
		{
			std::unique_lock<std::mutex> lck(routing_mutex_);
			if (routing_master_mode_ == detail::RoutingMasterMode::RouteBySequenceID && routing_table_.count(sequence_id)) {
				routing_wait_time_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count());
				return routing_table_.at(sequence_id);
			}
			else if (routing_master_mode_ == detail::RoutingMasterMode::RouteBySendCount && routing_table_.count(sent_frag_count_.count()))
			{
				routing_wait_time_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count());
				return routing_table_.at(sent_frag_count_.count());
			}
			usleep(routing_timeout_ms_ * 10);
		}
		routing_wait_time_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count());
		if (routing_master_mode_ == detail::RoutingMasterMode::RouteBySequenceID) {
			TLOG_ERROR("DataSenderManager") << "Bad Omen: I don't have routing information for seqID " << std::to_string(sequence_id)
				<< " and the Routing Master did not send a table update in routing_timeout (" << std::to_string(routing_timeout_ms_) << ")!" << TLOG_ENDL;
		}
		else
		{
			TLOG_ERROR("DataSenderManager") << "Bad Omen: I don't have routing information for send number " << std::to_string(sent_frag_count_.count())
				<< " and the Routing Master did not send a table update in routing_timeout (" << std::to_string(routing_timeout_ms_) << ")!" << TLOG_ENDL;
		}
	}
	else {
		auto index = sequence_id % enabled_destinations_.size();
		auto it = enabled_destinations_.begin();
		for (; index > 0; --index)
		{
			++it;
			if (it == enabled_destinations_.end()) it = enabled_destinations_.begin();
		}
		return *it;
	}
	return TransferInterface::RECV_TIMEOUT;
}

int
artdaq::DataSenderManager::
sendFragment(Fragment&& frag)
{
	// Precondition: Fragment must be complete and consistent (including
	// header information).
	auto start_time = std::chrono::steady_clock::now();
	if (frag.type() == Fragment::EndOfDataFragmentType)
	{
		throw cet::exception("LogicError")
			<< "EOD fragments should not be sent on as received: "
			<< "use sendEODFrag() instead.";
	}
	size_t seqID = frag.sequenceID();
	size_t fragSize = frag.sizeBytes();
	TLOG_ARB(13, "DataSenderManager") << "sendFragment start frag.fragmentHeader()=" << std::hex << (void*)(frag.headerBeginBytes()) << ", szB=" << std::dec << std::to_string(fragSize) << ", seqID=" << std::to_string(seqID) << TLOG_ENDL;
	int dest = TransferInterface::RECV_TIMEOUT;
	if (broadcast_sends_ || frag.type() == Fragment::EndOfRunFragmentType || frag.type() == Fragment::EndOfSubrunFragmentType || frag.type() == Fragment::InitFragmentType)
	{
		for (auto& bdest : enabled_destinations_)
		{
			TRACE(5, "DataSenderManager::sendFragment: Sending fragment with seqId %zu to destination %d (broadcast)", seqID, bdest);
			// Gross, we have to copy.
			Fragment fragCopy(frag);
			auto sts = destinations_[bdest]->copyFragment(fragCopy);
			while (sts == TransferInterface::CopyStatus::kTimeout)
			{
				sts = destinations_[bdest]->copyFragment(fragCopy);
			}
			sent_frag_count_.incSlot(bdest);
		}
	}
	else if (non_blocking_mode_)
	{
		while (dest == TransferInterface::RECV_TIMEOUT) {
			dest = calcDest_(seqID);
			if (dest == TransferInterface::RECV_TIMEOUT)
			{
				TLOG_WARNING("DataSenderManager") << "Could not get destination for seqID " << std::to_string(seqID) << ", retrying." << TLOG_ENDL;
			}
		}
		if (destinations_.count(dest) && enabled_destinations_.count(dest))
		{
			TRACE(5, "DataSenderManager::sendFragment: Sending fragment with seqId %zu to destination %d", seqID, dest);
			TransferInterface::CopyStatus sts = TransferInterface::CopyStatus::kErrorNotRequiringException;
			auto lastWarnTime = std::chrono::steady_clock::now();
			while (sts != TransferInterface::CopyStatus::kSuccess)
			{
				sts = destinations_[dest]->copyFragment(frag);
				if (sts != TransferInterface::CopyStatus::kSuccess && std::chrono::duration_cast<artdaq::TimeUtils::seconds>(std::chrono::steady_clock::now() - lastWarnTime).count() >= 1)
				{
					TLOG_ERROR("DataSenderManager") << "sendFragment: Sending fragment " << seqID << " to destination " << dest << " failed! Retrying..." << TLOG_ENDL;
					lastWarnTime = std::chrono::steady_clock::now();
				}
			}
			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
		}
		else
		{
			TLOG_WARNING("DataSenderManager") << "calcDest returned invalid destination rank " << dest << "! This event has been lost: " << seqID << TLOG_ENDL;
		}
	}
	else
	{
		while (dest == TransferInterface::RECV_TIMEOUT) {
			dest = calcDest_(seqID);
			if (dest == TransferInterface::RECV_TIMEOUT)
			{
				TLOG_WARNING("DataSenderManager") << "Could not get destination for seqID " << std::to_string(seqID) << ", send number " << sent_frag_count_.count() << ", retrying." << TLOG_ENDL;
			}
		}
		if (destinations_.count(dest) && enabled_destinations_.count(dest))
		{
			TRACE(5, "DataSenderManager::sendFragment: Sending fragment with seqId %zu to destination %d", seqID, dest);
			TransferInterface::CopyStatus sts = TransferInterface::CopyStatus::kErrorNotRequiringException;
			auto lastWarnTime = std::chrono::steady_clock::now();
			while (sts != TransferInterface::CopyStatus::kSuccess)
			{
				sts = destinations_[dest]->moveFragment(std::move(frag));
				if (sts != TransferInterface::CopyStatus::kSuccess && std::chrono::duration_cast<artdaq::TimeUtils::seconds>(std::chrono::steady_clock::now() - lastWarnTime).count() >= 1)
				{
					TLOG_ERROR("DataSenderManager") << "sendFragment: Sending fragment " << seqID << " to destination " << dest << " failed! Retrying..." << TLOG_ENDL;
					lastWarnTime = std::chrono::steady_clock::now();
				}
			}
			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
		}
		else
		{
			TLOG_WARNING("DataSenderManager") << "calcDest returned invalid destination rank " << dest << "! This event has been lost: " << seqID << TLOG_ENDL;
		}
	}
	if (routing_master_mode_ == detail::RoutingMasterMode::RouteBySequenceID && routing_table_.find(seqID - 1) != routing_table_.end())
	{
		std::unique_lock<std::mutex> lck(routing_mutex_);
		routing_table_.erase(routing_table_.begin(), routing_table_.find(seqID - 1));
	}
	else if(routing_master_mode_ == detail::RoutingMasterMode::RouteBySendCount)
	{
		std::unique_lock<std::mutex> lck(routing_mutex_);
		routing_table_.erase(routing_table_.begin(), routing_table_.find(sent_frag_count_.count()));
	}
	if (metricMan)
	{//&& sent_frag_count_.slotCount(dest) % 100 == 0) {
		auto delta_t = std::chrono::duration_cast<artdaq::TimeUtils::seconds>(std::chrono::steady_clock::now() - start_time).count();
		metricMan->sendMetric("Data Send Time to Rank " + std::to_string(dest), delta_t, "s", 3, MetricMode::Accumulate);
		metricMan->sendMetric("Data Send Size to Rank " + std::to_string(dest), fragSize, "B", 3, MetricMode::Accumulate);
		metricMan->sendMetric("Data Send Rate to Rank " + std::to_string(dest), fragSize / delta_t, "B/s", 3, MetricMode::Average);
		metricMan->sendMetric("Data Send Count to Rank " + std::to_string(dest), sent_frag_count_.slotCount(dest), "fragments", 3, MetricMode::LastPoint);
		if (use_routing_master_) {
			metricMan->sendMetric("Routing Table Size", routing_table_.size(), "events", 1, MetricMode::LastPoint);
			if (routing_wait_time_ > 0)
			{
				size_t wttemp = routing_wait_time_;
				routing_wait_time_ = 0;
				metricMan->sendMetric("Routing Wait Time", wttemp / 1000000000, "s", 1, MetricMode::Average);
			}
		}
	}
	TRACE(5, "DataSenderManager::sendFragment: Done sending fragment %zu", seqID);
	return dest;
}
