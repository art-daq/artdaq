#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_TableReceiver").c_str()
#include "artdaq/DAQrate/detail/TableReceiver.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq/TransferPlugins/detail/HostMap.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include "artdaq/DAQdata/TCPConnect.hh"
#include "canvas/Utilities/Exception.h"

artdaq::TableReceiver::TableReceiver(const fhicl::ParameterSet& pset)
    : use_routing_manager_(pset.get<bool>("use_routing_manager", false))
    , routing_manager_mode_(detail::RoutingManagerMode::INVALID)
    , should_stop_(false)
    , table_port_(pset.get<int>("table_update_port", 35556))
    , table_address_(pset.get<std::string>("table_update_address", "227.128.12.28"))
    , table_multicast_interface_(pset.get<std::string>("table_update_multicast_interface", "localhost"))
    , ack_port_(pset.get<int>("table_acknowledge_port", 35557))
    , ack_address_(pset.get<std::string>("routing_manager_hostname", "localhost"))
    , ack_socket_(-1)
    , table_socket_(-1)
    , routing_table_last_(0)
    , routing_table_max_size_(pset.get<size_t>("routing_table_max_size", 1000))
    , routing_timeout_ms_((pset.get<size_t>("routing_timeout_ms", 1000)))
    , highest_sequence_id_routed_(0)
{
	TLOG(TLVL_DEBUG) << "Received pset: " << pset.to_string();

	if (use_routing_manager_)
	{
		startTableReceiverThread_();
	}
}

artdaq::TableReceiver::~TableReceiver()
{
	TLOG(TLVL_DEBUG) << "Shutting down TableReceiver BEGIN";
	should_stop_ = true;
	try
	{
		if (routing_thread_.joinable())
		{
			routing_thread_.join();
		}
	}
	catch (...)
	{  // IGNORED
	}
	TLOG(TLVL_DEBUG) << "Shutting down TableReceiver END.";
}

artdaq::TableReceiver::RoutingTable artdaq::TableReceiver::GetRoutingTable() const
{
	std::unique_lock<std::mutex> lk(routing_mutex_);
	RoutingTable routing_table_copy(routing_table_);
	return routing_table_copy;
}

artdaq::TableReceiver::RoutingTable artdaq::TableReceiver::GetAndClearRoutingTable()
{
	std::unique_lock<std::mutex> lk(routing_mutex_);
	RoutingTable routing_table_copy(routing_table_);
	routing_table_.clear();
	return routing_table_copy;
}

int artdaq::TableReceiver::GetRoutingTableEntry(artdaq::Fragment::sequence_id_t seqID) const
{
	auto routing_timeout_ms = routing_timeout_ms_;
	if (routing_timeout_ms == 0) {
		routing_timeout_ms = 3600 * 1000;
	}
	auto condition_wait = routing_timeout_ms > 10 ? std::chrono::milliseconds(10) : std::chrono::milliseconds(routing_timeout_ms);
	auto start_time = std::chrono::steady_clock::now();
	while (!should_stop_ && TimeUtils::GetElapsedTimeMilliseconds(start_time) < routing_timeout_ms)
	{
		std::unique_lock<std::mutex> lk(routing_mutex_);
		routing_cv_.wait_for(lk, condition_wait, [&]() { return routing_table_.count(seqID); });
		if (routing_table_.count(seqID))
		{
			routing_wait_time_.fetch_add(TimeUtils::GetElapsedTimeMicroseconds(start_time));
			return routing_table_.at(seqID);
		}
	}
	if (routing_manager_mode_ == detail::RoutingManagerMode::RouteBySequenceID)
	{
		TLOG(TLVL_WARNING) << "Bad Omen: I don't have routing information for seqID " << seqID
		                   << " and the Routing Manager did not send a table update containing it in routing_timeout_ms window (" << routing_timeout_ms_ << " ms)!";
	}
	else
	{
		TLOG(TLVL_WARNING) << "Bad Omen: I don't have routing information for send number " << seqID
		                   << " and the Routing Manager did not send a table update containing it in routing_timeoutms window (" << routing_timeout_ms_ << " ms)!";
	}
	routing_wait_time_.fetch_add(TimeUtils::GetElapsedTimeMicroseconds(start_time));
	return ROUTING_FAILED;
}

void artdaq::TableReceiver::setupTableListener_()
{
	int sts;
	table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (table_socket_ < 0)
	{
		TLOG(TLVL_ERROR) << "Error creating socket for receiving table updates!";
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		TLOG(TLVL_ERROR) << " Unable to enable port reuse on request socket";
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(table_port_);
	//si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	struct in_addr in_addr_s;
	sts = inet_aton(table_address_.c_str(), &in_addr_s);
	if (sts == 0)
	{
		TLOG(TLVL_ERROR) << "inet_aton says table_address " << table_address_ << " is invalid";
	}
	si_me_request.sin_addr.s_addr = in_addr_s.s_addr;
	if (bind(table_socket_, reinterpret_cast<struct sockaddr*>(&si_me_request), sizeof(si_me_request)) == -1)  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	{
		TLOG(TLVL_ERROR) << "Cannot bind request socket to port " << table_port_;
		exit(1);
	}

	struct ip_mreq mreq;
	sts = ResolveHost(table_address_.c_str(), mreq.imr_multiaddr);
	if (sts == -1)
	{
		TLOG(TLVL_ERROR) << "Unable to resolve multicast address for table updates";
		exit(1);
	}
	sts = GetInterfaceForNetwork(table_multicast_interface_.c_str(), mreq.imr_interface);
	if (sts == -1)
	{
		TLOG(TLVL_ERROR) << "Unable to determine the multicast interface for table updates using " << table_multicast_interface_;
		exit(1);
	}
	char addr_str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(mreq.imr_interface), addr_str, INET_ADDRSTRLEN);
	TLOG(TLVL_INFO) << "Successfully determined the multicast network interface for " << table_multicast_interface_ << ": " << addr_str << " (TableReceiver receiving routing table updates)";
	if (setsockopt(table_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
	{
		TLOG(TLVL_ERROR) << "Unable to join multicast group";
		exit(1);
	}
}
void artdaq::TableReceiver::startTableReceiverThread_()
{
	if (routing_thread_.joinable())
	{
		routing_thread_.join();
	}
	TLOG(TLVL_INFO) << "Starting Routing Thread";
	try
	{
		routing_thread_ = boost::thread(&TableReceiver::receiveTableUpdatesLoop_, this);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Routing Table Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Routing Table Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}
void artdaq::TableReceiver::receiveTableUpdatesLoop_()
{
	while (true)
	{
		if (should_stop_)
		{
			TLOG(TLVL_DEBUG) << __func__ << ": should_stop is " << std::boolalpha << should_stop_ << ", stopping";
			artdaq::detail::RoutingAckPacket endOfDataAck = detail::RoutingAckPacket::makeEndOfDataRoutingAckPacket(my_rank);

			TLOG(TLVL_DEBUG) << __func__ << ": Sending RoutingAckPacket with end of run markers to " << ack_address_ << ", port " << ack_port_ << " (my_rank = " << my_rank << ")";
			sendto(ack_socket_, &endOfDataAck, sizeof(artdaq::detail::RoutingAckPacket), 0, reinterpret_cast<struct sockaddr*>(&ack_addr_), sizeof(ack_addr_));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
			return;
		}

		TLOG(TLVL_TRACE) << __func__ << ": Polling table socket for new routes (interface,address,port = "
		                 << table_multicast_interface_ << "," << table_address_ << "," << table_port_ << ")";
		if (table_socket_ == -1)
		{
			TLOG(TLVL_DEBUG) << __func__ << ": Opening table listener socket";
			setupTableListener_();
		}
		if (table_socket_ == -1)
		{
			TLOG(TLVL_DEBUG) << __func__ << ": The listen socket was not opened successfully.";
			return;
		}
		if (ack_socket_ == -1)
		{
			ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
			auto sts = ResolveHost(ack_address_.c_str(), ack_port_, ack_addr_);
			if (sts == -1)
			{
				TLOG(TLVL_ERROR) << __func__ << ": Unable to resolve routing_manager_address";
				exit(1);
			}
			TLOG(TLVL_DEBUG) << __func__ << ": Ack socket is fd " << ack_socket_;
			char addr_str[INET_ADDRSTRLEN];
			inet_ntop(AF_INET, &(ack_addr_.sin_addr), addr_str, INET_ADDRSTRLEN);
			TLOG(TLVL_INFO) << "Successfully determined the network interface for " << ack_address_ << ": " << addr_str << " (TableReceiver sending table update acknowledgements)";
		}

		struct pollfd fd;
		fd.fd = table_socket_;
		fd.events = POLLIN | POLLPRI;

		auto res = poll(&fd, 1, 1000);
		if (res > 0)
		{
			auto first = artdaq::Fragment::InvalidSequenceID;
			auto last = artdaq::Fragment::InvalidSequenceID;
			std::vector<uint8_t> buf(MAX_ROUTING_TABLE_SIZE);
			artdaq::detail::RoutingPacketHeader hdr;

			TLOG(TLVL_DEBUG) << __func__ << ": Going to receive RoutingPacketHeader";
			struct sockaddr_in from;
			socklen_t len = sizeof(from);
			auto stss = recvfrom(table_socket_, &buf[0], MAX_ROUTING_TABLE_SIZE, 0, reinterpret_cast<struct sockaddr*>(&from), &len);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
			TLOG(TLVL_DEBUG) << __func__ << ": Received " << stss << " bytes from " << inet_ntoa(from.sin_addr) << ":" << from.sin_port;

			if (stss > static_cast<ssize_t>(sizeof(hdr)))
			{
				memcpy(&hdr, &buf[0], sizeof(artdaq::detail::RoutingPacketHeader));
			}
			else
			{
				TLOG(TLVL_TRACE) << __func__ << ": Incorrect size received. Discarding.";
				continue;
			}

			TLOG(TLVL_DEBUG) << "receiveTableUpdatesLoop_: Checking for valid header with nEntries=" << hdr.nEntries << "header=" << std::hex << hdr.header;
			if (hdr.header != ROUTING_MAGIC)
			{
				TLOG(TLVL_TRACE) << __func__ << ": non-RoutingPacket received. No ROUTING_MAGIC. size(bytes)=" << stss;
			}
			else
			{
				if (routing_manager_mode_ != detail::RoutingManagerMode::INVALID && routing_manager_mode_ != hdr.mode)
				{
					TLOG(TLVL_ERROR) << __func__ << ": Received table has different RoutingManagerMode than expected!";
					exit(1);
				}
				routing_manager_mode_ = hdr.mode;

				artdaq::detail::RoutingPacket buffer(hdr.nEntries);
				assert(static_cast<size_t>(stss) == sizeof(artdaq::detail::RoutingPacketHeader) + sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);
				memcpy(&buffer[0], &buf[sizeof(artdaq::detail::RoutingPacketHeader)], sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);

				first = buffer.front().sequence_id;
				last = buffer.back().sequence_id;

				if (first + hdr.nEntries - 1 != last)
				{
					TLOG(TLVL_ERROR) << __func__ << ": Skipping this RoutingPacket because the first (" << first << ") and last (" << last << ") entries are inconsistent (sz=" << hdr.nEntries << ")!";
					continue;
				}
				auto thisSeqID = first;

				{
					std::unique_lock<std::mutex> lck(routing_mutex_);
					if (routing_table_.count(last) == 0)
					{
						for (auto entry : buffer)
						{
							if (thisSeqID != entry.sequence_id)
							{
								TLOG(TLVL_ERROR) << __func__ << ": Aborting processing of this RoutingPacket because I encountered an inconsistent entry (seqid=" << entry.sequence_id << ", expected=" << thisSeqID << ")!";
								last = thisSeqID - 1;
								break;
							}
							thisSeqID++;
							if (routing_table_.count(entry.sequence_id) != 0u)
							{
								if (routing_table_[entry.sequence_id] != entry.destination_rank)
								{
									TLOG(TLVL_ERROR) << __func__ << ": Detected routing table corruption! Recevied update specifying that sequence ID " << entry.sequence_id
									                 << " should go to rank " << entry.destination_rank << ", but I had already been told to send it to " << routing_table_[entry.sequence_id] << "!"
									                 << " I will use the original value!";
								}
								continue;
							}
							if (entry.sequence_id < routing_table_last_)
							{
								continue;
							}
							routing_table_[entry.sequence_id] = entry.destination_rank;
							TLOG(TLVL_DEBUG) << __func__ << ": (my_rank=" << my_rank << ") received update: SeqID " << entry.sequence_id
							                 << " -> Rank " << entry.destination_rank;
						}
					}

					TLOG(TLVL_DEBUG) << __func__ << ": There are now " << routing_table_.size() << " entries in the Routing Table";
					if (!routing_table_.empty())
					{
						TLOG(TLVL_DEBUG) << __func__ << ": Last routing table entry is seqID=" << routing_table_.rbegin()->first;
					}

					auto counter = 0;
					for (auto& entry : routing_table_)
					{
						TLOG(45) << "Routing Table Entry" << counter << ": " << entry.first << " -> " << entry.second;
						counter++;
					}
				}
				routing_cv_.notify_all();

				artdaq::detail::RoutingAckPacket ack;
				ack.rank = my_rank;
				ack.first_sequence_id = first;
				ack.last_sequence_id = last;

				if (last > routing_table_last_)
				{
					routing_table_last_ = last;
				}

				if (my_rank < static_cast<int>(8 * sizeof(hdr.already_acknowledged_ranks)) && hdr.already_acknowledged_ranks.test(my_rank))
				{
					TLOG(TLVL_DEBUG) << __func__ << ": Skipping RoutingAckPacket since this Routing Table Update has already been acknowledged (my_rank = " << my_rank << ")";
				}
				else
				{
					TLOG(TLVL_DEBUG) << __func__ << ": Sending RoutingAckPacket with first= " << first << " and last= " << last << " to " << ack_address_ << ", port " << ack_port_ << " (my_rank = " << my_rank << ")";
					sendto(ack_socket_, &ack, sizeof(artdaq::detail::RoutingAckPacket), 0, reinterpret_cast<struct sockaddr*>(&ack_addr_), sizeof(ack_addr_));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
				}
			}
		}

		SendMetrics();
	}
}

size_t artdaq::TableReceiver::GetRoutingTableEntryCount() const
{
	std::unique_lock<std::mutex> lck(routing_mutex_);
	return routing_table_.size();
}

size_t artdaq::TableReceiver::GetRemainingRoutingTableEntries() const
{
	std::unique_lock<std::mutex> lck(routing_mutex_);
	// Find the distance from the next highest sequence ID to the end of the list
	size_t dist = std::distance(routing_table_.upper_bound(highest_sequence_id_routed_), routing_table_.end());
	return dist;  // If dist == 1, there is one entry left.
}

void artdaq::TableReceiver::RemoveRoutingTableEntry(Fragment::sequence_id_t seq)
{
	TLOG(15) << "RemoveRoutingTableEntry: Removing sequence ID " << seq << " from routing table.";
	std::unique_lock<std::mutex> lck(routing_mutex_);
	//	while (routing_table_.size() > routing_table_max_size_)
	//	{
	//		routing_table_.erase(routing_table_.begin());
	//	}
	if (routing_table_.find(seq) != routing_table_.end())
	{
		routing_table_.erase(routing_table_.find(seq));
	}
}

void artdaq::TableReceiver::SendMetrics() const
{
	if (metricMan)
	{
		TLOG(5) << "sending metrics";
		if (use_routing_manager_)
		{
			metricMan->sendMetric("Routing Table Size", GetRoutingTableEntryCount(), "events", 2, MetricMode::LastPoint);
			if (routing_wait_time_ > 0)
			{
				metricMan->sendMetric("Routing Wait Time", static_cast<double>(routing_wait_time_.load()) / 1000000, "s", 2, MetricMode::Average);
				routing_wait_time_ = 0;
			}
		}
	}
}
