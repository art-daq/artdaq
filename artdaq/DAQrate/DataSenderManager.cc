#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"
#include "artdaq/DAQdata/Globals.hh"

#include <chrono>
#include <canvas/Utilities/Exception.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "artdaq/Application/Routing/RoutingPacket.hh"
#include <artdaq/DAQdata/TCPConnect.hh>

artdaq::DataSenderManager::DataSenderManager(fhicl::ParameterSet pset)
	: destinations_()
	, enabled_destinations_()
	, sent_frag_count_()
	, broadcast_sends_(pset.get<bool>("broadcast_sends", false))
	, use_routing_master_(pset.get<bool>("use_routing_master", false))
	, should_stop_(false)
	, table_port_(pset.get<int>("table_update_port", 35556))
	, table_address_(pset.get<std::string>("table_update_address", "227.128.12.28"))
	, ack_port_(pset.get<int>("table_acknowledge_port", 35557))
	, ack_address_(pset.get<std::string>("routing_master_hostname", "localhost"))
	, ack_socket_(-1)
	, table_socket_(-1)
	, table_epoll_fd_(-1)
	, routing_timeout_ms_(pset.get<int>("routing_timeout_ms", 1000))
{
	mf::LogDebug("DataSenderManager") << "Received pset: " << pset.to_string();
	auto dests = pset.get<fhicl::ParameterSet>("destinations", fhicl::ParameterSet());
	for (auto& d : dests.get_pset_names())
	{
		try
		{
			auto dd = std::stoi(d.substr(1));
			destinations_.emplace(dd, MakeTransferPlugin(dests, d, TransferInterface::Role::kSend));
		}
		catch (std::invalid_argument)
		{
			TRACE(3, "Invalid destination specification: " + d);
		}
		catch (cet::exception ex)
		{
			mf::LogWarning("DataSenderManager") << "Caught cet::exception: " << ex.what();
		}
		catch (...)
		{
			mf::LogWarning("DataSenderManager") << "Non-cet exception while setting up TransferPlugin: " << d << ".";
		}
	}
	if (destinations_.size() == 0)
	{
		mf::LogError("DataSenderManager") << "No destinations specified!";
	}
	else
	{
		auto enabled_dests = pset.get<std::vector<size_t>>("enabled_destinations", std::vector<size_t>());
		if (enabled_dests.size() == 0)
		{
			mf::LogInfo("DataSenderManager") << "enabled_destinations not specified, assuming all destinations enabled.";
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
	for (auto& dest : enabled_destinations_)
	{
		if (destinations_.count(dest))
		{
			destinations_[dest]->moveFragment(std::move(*Fragment::eodFrag(sent_frag_count_.slotCount(dest))));
			//  sendFragTo(std::move(*Fragment::eodFrag(nFragments)), dest, true);
		}
	}
	should_stop_ = true;
	if (routing_thread_.joinable()) routing_thread_.join();
	mf::LogDebug("DataSenderManager") << "Shutting down DataSenderManager. Sent " << count() << " fragments.";
}


void artdaq::DataSenderManager::setupTableListener()
{
	table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (!table_socket_)
	{
		mf::LogError("DataSenderManager") << "Error creating socket for receiving data requests!";
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		mf::LogError("DataSenderManager") << " Unable to enable port reuse on request socket";
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(table_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(table_socket_, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
	{
		mf::LogError("DataSenderManager") << "Cannot bind request socket to port " << table_port_;
		exit(1);
	}

	struct ip_mreq mreq;
	int sts = ResolveHost(table_address_.c_str(), mreq.imr_multiaddr);
	if(sts == -1)
	{
		mf::LogError("DataSenderManager") << "Unable to resolve multicast address for table updates";
		exit(1);
	}
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (setsockopt(table_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
	{
		mf::LogError("DataSenderManager") << "Unable to join multicast group";
		exit(1);
	}
}
void artdaq::DataSenderManager::startTableReceiverThread()
{
	if (routing_thread_.joinable()) routing_thread_.join();
	mf::LogInfo("DataSenderManager") << "Starting Routing Thread" << std::endl;
	routing_thread_ = std::thread(&DataSenderManager::receiveTableUpdatesLoop, this);
}
void artdaq::DataSenderManager::receiveTableUpdatesLoop()
{
	while (true)
	{
		if (should_stop_)
		{
			mf::LogDebug("DataSenderManager") << "receiveTableUpdatesLoop: should_stop is " << std::boolalpha << should_stop_;
			return;
		}

		TRACE(4, "DataSenderManager::receiveTableUpdatesLoop: Polling Request socket for new requests");
		if (table_socket_ == -1)
		{
			mf::LogDebug("DataSenderManager") << "Opening token listener socket";
			setupTableListener();
		}
		if (table_socket_ == -1 || table_epoll_fd_ == -1)
		{
			mf::LogDebug("DataSenderManager") << "One of the listen sockets was not opened successfully.";
			return;
		}

		auto first = artdaq::Fragment::InvalidSequenceID;
		auto last = artdaq::Fragment::InvalidSequenceID;
		detail::RoutingPacketHeader hdr;
		recv(table_socket_, &hdr, sizeof(detail::RoutingPacketHeader), 0);
		if (hdr.header == ROUTING_MAGIC) {
			detail::RoutingPacket buffer(hdr.nEntries);
			recv(table_socket_, &buffer[0], sizeof(detail::RoutingPacketEntry) * hdr.nEntries, 0);
			first = buffer[0].sequence_id;
			last = buffer[buffer.size() - 1].sequence_id;

			{
				std::unique_lock<std::mutex> lck(routing_mutex_);
				for (auto entry : buffer)
				{
					if (routing_table_.count(entry.sequence_id)) continue;
					routing_table_[entry.sequence_id] = entry.destination_rank;
				}
			}

			detail::RoutingAckPacket ack;
			ack.rank = my_rank;
			ack.first_sequence_id = first;
			ack.last_sequence_id = last;

			if (ack_socket_ == -1)
			{
				ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
				int sts = ResolveHost(ack_address_.c_str(), ack_port_, ack_addr_);
				if(sts == -1)
				{
					mf::LogError("DataSenderManager") << "Unable to resolve routing_master_address";
					exit(1);
				}
			}

			mf::LogDebug("DataSenderManager") << "Sending RoutingAckPacket to " << ack_address_ << std::endl;
			sendto(ack_socket_, &ack, sizeof(detail::RoutingAckPacket), 0, (struct sockaddr *)&ack_addr_, sizeof(ack_addr_));
		}
	}
}


int artdaq::DataSenderManager::calcDest(Fragment::sequence_id_t sequence_id) const
{
	if (enabled_destinations_.size() == 0) return TransferInterface::RECV_TIMEOUT; // No destinations configured.
	if (use_routing_master_)
	{
		auto start = std::chrono::steady_clock::now();
		while (routing_timeout_ms_ <= 0 || std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() < routing_timeout_ms_)
		{
			std::unique_lock<std::mutex> lck(routing_mutex_);
			if (routing_table_.count(sequence_id)) {
				return routing_table_.at(sequence_id);
			}
			usleep(routing_timeout_ms_ * 10);
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
	TRACE(13, "sendFragment start frag.fragmentHeader()=%p, szB=%zu", (void*)(frag.headerBeginBytes()), fragSize);
	int dest = -1;
	if (broadcast_sends_)
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
	else
	{
		dest = calcDest(seqID);
		if (destinations_.count(dest) && enabled_destinations_.count(dest))
		{
			TRACE(5, "DataSenderManager::sendFragment: Sending fragment with seqId %zu to destination %d", seqID, dest);
			TransferInterface::CopyStatus sts = TransferInterface::CopyStatus::kErrorNotRequiringException;
			auto lastWarnTime = std::chrono::steady_clock::now();
			while (sts != TransferInterface::CopyStatus::kSuccess)
			{
				sts = destinations_[dest]->moveFragment(std::move(frag));
				if (sts != TransferInterface::CopyStatus::kSuccess && std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - lastWarnTime).count() >= 1)
				{
					mf::LogError("DataSenderManager") << "sendFragment: Sending fragment " << seqID << " to destination " << dest << " failed! Retrying...";
					lastWarnTime = std::chrono::steady_clock::now();
				}
			}
			//sendFragTo(std::move(frag), dest);
			sent_frag_count_.incSlot(dest);
		}
	}
	if (routing_table_.find(seqID - 1) != routing_table_.end())
	{
		std::unique_lock<std::mutex> lck(routing_mutex_);
		routing_table_.erase(routing_table_.begin(), routing_table_.find(seqID - 1));
	}
	if (metricMan)
	{//&& sent_frag_count_.slotCount(dest) % 100 == 0) {
		auto delta_t = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time).count();
		metricMan->sendMetric("Data Send Time to Rank " + std::to_string(dest), delta_t, "s", 1);
		metricMan->sendMetric("Data Send Size to Rank " + std::to_string(dest), fragSize, "B", 1);
		metricMan->sendMetric("Data Send Rate to Rank " + std::to_string(dest), fragSize / delta_t, "B/s", 1);
	}
	TRACE(5, "DataSenderManager::sendFragment: Done sending fragment %zu", seqID);
	return dest;
}
