#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_TableReceiver").c_str()
#include "artdaq/DAQrate/detail/TableReceiver.hh"

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
    , route_on_request_(pset.get<bool>("route_on_request_mode", false))
    , should_stop_(false)
    , table_port_(pset.get<int>("table_update_port", 35556))
    , table_address_(pset.get<std::string>("routing_manager_hostname", "localhost"))
    , table_socket_(-1)
    , routing_table_last_(0)
    , routing_table_max_size_(pset.get<size_t>("routing_table_max_size", 1000))
    , routing_timeout_ms_((pset.get<size_t>("routing_timeout_ms", 1000)))
    , highest_sequence_id_routed_(0)
{
	TLOG(TLVL_DEBUG) << "Received pset: " << pset.to_string();

	if (use_routing_manager_ && !route_on_request_)
	{
		startTableReceiverThread_();
	}
}

artdaq::TableReceiver::~TableReceiver()
{
	TLOG(TLVL_DEBUG) << "Shutting down TableReceiver BEGIN";
	should_stop_ = true;
	disconnectFromRoutingManager_();

	if (routing_thread_ != nullptr)
	{
		try
		{
			if (routing_thread_->joinable())
			{
				routing_thread_->join();
			}
		}
		catch (...)
		{  // IGNORED
		}
	}
	TLOG(TLVL_DEBUG) << "Shutting down TableReceiver END.";
}

artdaq::TableReceiver::RoutingTable artdaq::TableReceiver::GetRoutingTable() const
{
	std::lock_guard<std::mutex> lk(routing_mutex_);
	RoutingTable routing_table_copy(routing_table_);
	return routing_table_copy;
}

artdaq::TableReceiver::RoutingTable artdaq::TableReceiver::GetAndClearRoutingTable()
{
	std::lock_guard<std::mutex> lk(routing_mutex_);
	RoutingTable routing_table_copy(routing_table_);
	routing_table_.clear();
	return routing_table_copy;
}

int artdaq::TableReceiver::GetRoutingTableEntry(artdaq::Fragment::sequence_id_t seqID)
{
	if (use_routing_manager_)
	{
		if (route_on_request_)
		{
			return sendTableUpdateRequest_(seqID);
		}
		else
		{
			auto routing_timeout_ms = routing_timeout_ms_;
			if (routing_timeout_ms == 0)
			{
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
			TLOG(TLVL_WARNING) << "Bad Omen: Timeout receiving routing information for " << seqID
			                   << " in routing_timeout_ms (" << routing_timeout_ms_ << " ms)!";

			routing_wait_time_.fetch_add(TimeUtils::GetElapsedTimeMicroseconds(start_time));
		}
	}
	return ROUTING_FAILED;
}

void artdaq::TableReceiver::connectToRoutingManager_()
{
	auto start_time = std::chrono::steady_clock::now();
	while (table_socket_ < 0 && TimeUtils::GetElapsedTime(start_time) < 30)
	{
		table_socket_ = TCPConnect(table_address_.c_str(), table_port_);
		if (table_socket_ < 0)
		{
			TLOG(TLVL_TRACE) << "Waited " << TimeUtils::GetElapsedTime(start_time) << " s for Routing Manager to open table listen socket";
			usleep(100000);
		}
	}
	if (table_socket_ < 0)
	{
		TLOG(TLVL_ERROR) << "Error creating socket for receiving table updates!";
		exit(1);
	}

	detail::RoutingRequest startHdr(my_rank);
	write(table_socket_, &startHdr, sizeof(startHdr));
}

void artdaq::TableReceiver::disconnectFromRoutingManager_()
{
	detail::RoutingRequest endHdr(my_rank, detail::RoutingRequest::RequestMode::Disconnect);
	write(table_socket_, &endHdr, sizeof(endHdr));
	close(table_socket_);
	table_socket_ = -1;
}

void artdaq::TableReceiver::startTableReceiverThread_()
{
	if (routing_thread_ != nullptr && routing_thread_->joinable())
	{
		routing_thread_->join();
	}
	TLOG(TLVL_INFO) << "Starting Routing Thread";
	try
	{
		routing_thread_.reset(new boost::thread(&TableReceiver::receiveTableUpdatesLoop_, this));
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Routing Table Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Routing Table Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}

bool artdaq::TableReceiver::receiveTableUpdate_()
{
	TLOG(TLVL_TRACE) << __func__ << ": Polling table socket for new routes (address:port = " << table_address_ << ":" << table_port_ << ")";
	if (table_socket_ == -1)
	{
		TLOG(TLVL_DEBUG) << __func__ << ": Opening table socket";
		connectToRoutingManager_();
	}
	if (table_socket_ == -1)
	{
		TLOG(TLVL_DEBUG) << __func__ << ": The table socket was not opened successfully.";
		return false;
	}

	struct pollfd fd;
	fd.fd = table_socket_;
	fd.events = POLLIN | POLLPRI;

	auto res = poll(&fd, 1, 1000);
	if (res > 0)
	{
		if (fd.revents & (POLLIN | POLLPRI))
		{
			TLOG(TLVL_DEBUG) << __func__ << ": Going to receive RoutingPacketHeader";
			artdaq::detail::RoutingPacketHeader hdr;
			ssize_t stss = recv(table_socket_, &hdr, sizeof(hdr), MSG_WAITALL);
			if (stss != sizeof(hdr))
			{
				TLOG(TLVL_ERROR) << "Error reading Table Header from Table socket, errno=" << errno << " (" << strerror(errno) << ")";
				disconnectFromRoutingManager_();
				return false;
			}

			TLOG(TLVL_DEBUG) << "receiveTableUpdatesLoop_: Checking for valid header with nEntries=" << hdr.nEntries << " header=" << std::hex << hdr.header;
			if (hdr.header != ROUTING_MAGIC)
			{
				TLOG(TLVL_TRACE) << __func__ << ": non-RoutingPacket received. No ROUTING_MAGIC.";
				return false;
			}
			if (hdr.nEntries == 0)
			{
				TLOG(TLVL_TRACE) << __func__ << ": Empty Routing Table update received.";
				return false;
			}

			artdaq::detail::RoutingPacket buffer(hdr.nEntries);
			size_t sts = 0;
			size_t total = sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries;
			while (sts < total)
			{
				stss = read(table_socket_, reinterpret_cast<char*>(&buffer[0]) + sts, total - sts);
				sts += stss;
				TLOG(TLVL_DEBUG) << "Read " << stss << " bytes, total " << sts << " / " << total;
				if (stss < 0)
				{
					TLOG(TLVL_ERROR) << "Error reading Table Data from Table socket, errno=" << errno << " (" << strerror(errno) << ")";
					disconnectFromRoutingManager_();
					return false;
				}
			}

			auto first = buffer.front().sequence_id;
			auto last = buffer.back().sequence_id;

			if (first + hdr.nEntries - 1 != last)
			{
				TLOG(TLVL_ERROR) << __func__ << ": Skipping this RoutingPacket because the first (" << first << ") and last (" << last << ") entries are inconsistent (sz=" << hdr.nEntries << ")!";
				return false;
			}

			auto thisSeqID = first;

			{
				std::lock_guard<std::mutex> lck(routing_mutex_);
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

			SendMetrics();
			return true;
		}
		else
		{
			TLOG(TLVL_DEBUG) << "Poll indicates socket closure. Disconnecting from Routing Manager";
			disconnectFromRoutingManager_();
			return false;
		}
	}
	return false;
}

void artdaq::TableReceiver::receiveTableUpdatesLoop_()
{
	while (true)
	{
		if (should_stop_)
		{
			TLOG(TLVL_DEBUG) << __func__ << ": should_stop is " << std::boolalpha << should_stop_ << ", stopping";
			disconnectFromRoutingManager_();
			return;
		}

		receiveTableUpdate_();
	}
}

int artdaq::TableReceiver::sendTableUpdateRequest_(Fragment::sequence_id_t seq)
{
	TLOG(TLVL_TRACE) << "sendTableUpdateRequest_ BEGIN";
	{
		std::lock_guard<std::mutex> lck(routing_mutex_);
		if (routing_table_.count(seq))
		{
			TLOG(TLVL_TRACE) << "sendTableUpdateRequest_ END: " << routing_table_.at(seq);
			return routing_table_.at(seq);
		}
	}
	if (table_socket_ == -1)
	{
		connectToRoutingManager_();
	}

	auto start_time = std::chrono::steady_clock::now();
	while (TimeUtils::GetElapsedTimeMilliseconds(start_time) < routing_timeout_ms_)
	{
		TLOG(TLVL_DEBUG) << "sendTableUpdateRequest_: Sending table update request for " << my_rank << ", sequence ID " << seq;
		detail::RoutingRequest pkt(my_rank, seq);
		write(table_socket_, &pkt, sizeof(pkt));

		receiveTableUpdate_();
		{
			std::lock_guard<std::mutex> lck(routing_mutex_);
			if (routing_table_.count(seq))
			{
				TLOG(TLVL_TRACE) << "sendTableUpdateRequest_ END: " << routing_table_.at(seq);
				return routing_table_.at(seq);
			}
		}
	}
	TLOG(TLVL_TRACE) << "sendTableUpdateRequest_ END: Routing Failed";
	return ROUTING_FAILED;
}

size_t artdaq::TableReceiver::GetRoutingTableEntryCount() const
{
	std::lock_guard<std::mutex> lck(routing_mutex_);
	return routing_table_.size();
}

size_t artdaq::TableReceiver::GetRemainingRoutingTableEntries() const
{
	std::lock_guard<std::mutex> lck(routing_mutex_);
	// Find the distance from the next highest sequence ID to the end of the list
	size_t dist = std::distance(routing_table_.upper_bound(highest_sequence_id_routed_), routing_table_.end());
	return dist;  // If dist == 1, there is one entry left.
}

void artdaq::TableReceiver::RemoveRoutingTableEntry(Fragment::sequence_id_t seq)
{
	TLOG(15) << "RemoveRoutingTableEntry: Removing sequence ID " << seq << " from routing table.";
	std::lock_guard<std::mutex> lck(routing_mutex_);
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
