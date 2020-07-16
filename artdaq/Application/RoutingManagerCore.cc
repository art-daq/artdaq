#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/un.h>
#include <algorithm>
#include <memory>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"

#include "artdaq/DAQdata/Globals.hh"                          // include these 2 first to get tracemf.h -
#define TRACE_NAME (app_name + "_RoutingManagerCore").c_str()  // before trace.h
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/Application/RoutingManagerCore.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQdata/TCP_listen_fd.hh"
#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"

const std::string artdaq::RoutingManagerCore::
    TABLE_UPDATES_STAT_KEY("RoutingManagerCoreTableUpdates");
const std::string artdaq::RoutingManagerCore::
    TOKENS_RECEIVED_STAT_KEY("RoutingManagerCoreTokensReceived");

artdaq::RoutingManagerCore::RoutingManagerCore()
    : shutdown_requested_(false)
    , stop_requested_(true)
    , pause_requested_(false)
    , statsHelperPtr_(new artdaq::StatisticsHelper())
{
	TLOG(TLVL_DEBUG) << "Constructor";
	statsHelperPtr_->addMonitoredQuantityName(TABLE_UPDATES_STAT_KEY);
	statsHelperPtr_->addMonitoredQuantityName(TOKENS_RECEIVED_STAT_KEY);
}

artdaq::RoutingManagerCore::~RoutingManagerCore()
{
	TLOG(TLVL_DEBUG) << "Destructor";
	artdaq::StatisticsCollection::getInstance().requestStop();
	token_receiver_->stopTokenReception(true);
}

bool artdaq::RoutingManagerCore::initialize(fhicl::ParameterSet const& pset, uint64_t /*unused*/, uint64_t /*unused*/)
{
	TLOG(TLVL_DEBUG) << "initialize method called with "
	                 << "ParameterSet = \"" << pset.to_string()
	                 << "\".";

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the DAQ parameters in the initialization "
		    << "ParameterSet: \"" + pset.to_string() + "\".";
		return false;
	}

	if (daq_pset.has_key("rank"))
	{
		if (my_rank >= 0 && daq_pset.get<int>("rank") != my_rank)
		{
			TLOG(TLVL_WARNING) << "Routing Manager rank specified at startup is different than rank specified at configure! Using rank received at configure!";
		}
		my_rank = daq_pset.get<int>("rank");
	}
	if (my_rank == -1)
	{
		TLOG(TLVL_ERROR) << "Routing Manager rank not specified at startup or in configuration! Aborting";
		exit(1);
	}

	try
	{
		policy_pset_ = daq_pset.get<fhicl::ParameterSet>("policy");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the policy parameters in the DAQ initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
		return false;
	}

	try
	{
		token_receiver_pset_ = daq_pset.get<fhicl::ParameterSet>("token_receiver");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the token_receiver parameters in the DAQ initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
		return false;
	}

	// pull out the Metric part of the ParameterSet
	fhicl::ParameterSet metric_pset;
	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...)
	{}  // OK if there's no metrics table defined in the FHiCL

	if (metric_pset.is_empty())
	{
		TLOG(TLVL_INFO) << "No metric plugins appear to be defined";
	}
	try
	{
		metricMan->initialize(metric_pset, app_name);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
		                 "Error loading metrics in RoutingManagerCore::initialize()");
	}

	// create the requested RoutingPolicy
	auto policy_plugin_spec = policy_pset_.get<std::string>("policy", "");
	if (policy_plugin_spec.length() == 0)
	{
		TLOG(TLVL_ERROR)
		    << "No fragment generator (parameter name = \"policy\") was "
		    << "specified in the policy ParameterSet.  The "
		    << "DAQ initialization PSet was \"" << daq_pset.to_string() << "\".";
		return false;
	}
	try
	{
		policy_ = artdaq::makeRoutingManagerPolicy(policy_plugin_spec, policy_pset_);
	}
	catch (...)
	{
		std::stringstream exception_string;
		exception_string << "Exception thrown during initialization of policy of type \""
		                 << policy_plugin_spec << "\"";

		ExceptionHandler(ExceptionHandlerRethrow::no, exception_string.str());

		TLOG(TLVL_DEBUG) << "FHiCL parameter set used to initialize the policy which threw an exception: " << policy_pset_.to_string();

		return false;
	}

	rt_priority_ = daq_pset.get<int>("rt_priority", 0);
	sender_ranks_ = daq_pset.get<std::vector<int>>("sender_ranks");

	receive_ack_events_ = std::vector<epoll_event>(sender_ranks_.size());

	auto mode = daq_pset.get<bool>("senders_send_by_send_count", false);
	routing_mode_ = mode ? detail::RoutingManagerMode::RouteBySendCount : detail::RoutingManagerMode::RouteBySequenceID;
	max_table_update_interval_ms_ = daq_pset.get<size_t>("table_update_interval_ms", 1000);
	current_table_interval_ms_ = max_table_update_interval_ms_;
	max_ack_cycle_count_ = daq_pset.get<size_t>("table_ack_retry_count", 5);
	send_tables_port_ = daq_pset.get<int>("table_update_port", 35556);
	receive_acks_port_ = daq_pset.get<int>("table_acknowledge_port", 35557);
	send_tables_address_ = daq_pset.get<std::string>("table_update_address", "227.128.12.28");
	multicast_out_hostname_ = daq_pset.get<std::string>("routing_manager_hostname", "localhost");

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelperPtr_->createCollectors(daq_pset, 100, 30.0, 60.0, TABLE_UPDATES_STAT_KEY);

	// create the requested TokenReceiver
	token_receiver_ = std::make_unique<TokenReceiver>(token_receiver_pset_, policy_, routing_mode_, sender_ranks_.size(), max_table_update_interval_ms_);
	token_receiver_->setStatsHelper(statsHelperPtr_, TOKENS_RECEIVED_STAT_KEY);
	token_receiver_->startTokenReception();
	token_receiver_->pauseTokenReception();

	shutdown_requested_.store(false);
	return true;
}

bool artdaq::RoutingManagerCore::start(art::RunID id, uint64_t /*unused*/, uint64_t /*unused*/)
{
	run_id_ = id;
	for (auto& rank : sender_ranks_)
	{
		if (active_ranks_.count(rank) == 0u)
		{
			active_ranks_.insert(rank);
		}
	}
	stop_requested_.store(false);
	pause_requested_.store(false);

	statsHelperPtr_->resetStatistics();

	metricMan->do_start();
	table_update_count_ = 0;
	token_receiver_->setRunNumber(run_id_.run());
	token_receiver_->resumeTokenReception();

	TLOG(TLVL_INFO) << "Started run " << run_id_.run();
	return true;
}

bool artdaq::RoutingManagerCore::stop(uint64_t /*unused*/, uint64_t /*unused*/)
{
	TLOG(TLVL_INFO) << "Stopping run " << run_id_.run()
	                << " after " << table_update_count_ << " table updates."
	                << " and " << token_receiver_->getReceivedTokenCount() << " received tokens.";
	stop_requested_.store(true);
	token_receiver_->pauseTokenReception();
	run_id_ = art::RunID::flushRun();
	return true;
}

bool artdaq::RoutingManagerCore::pause(uint64_t /*unused*/, uint64_t /*unused*/)
{
	TLOG(TLVL_INFO) << "Pausing run " << run_id_.run()
	                << " after " << table_update_count_ << " table updates."
	                << " and " << token_receiver_->getReceivedTokenCount() << " received tokens.";
	pause_requested_.store(true);
	return true;
}

bool artdaq::RoutingManagerCore::resume(uint64_t /*unused*/, uint64_t /*unused*/)
{
	TLOG(TLVL_DEBUG) << "Resuming run " << run_id_.run();
	pause_requested_.store(false);
	metricMan->do_start();
	return true;
}

bool artdaq::RoutingManagerCore::shutdown(uint64_t /*unused*/)
{
	shutdown_requested_.store(true);
	token_receiver_->stopTokenReception();
	policy_.reset();
	metricMan->shutdown();
	return true;
}

bool artdaq::RoutingManagerCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	TLOG(TLVL_INFO) << "soft_initialize method called with "
	                << "ParameterSet = \"" << pset.to_string()
	                << "\".";
	return initialize(pset, e, f);
}

bool artdaq::RoutingManagerCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	TLOG(TLVL_INFO) << "reinitialize method called with "
	                << "ParameterSet = \"" << pset.to_string()
	                << "\".";
	return initialize(pset, e, f);
}

void artdaq::RoutingManagerCore::process_event_table()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param) != 0)
		{
			TLOG(TLVL_WARNING) << "setting realtime priority failed";
		}
#pragma GCC diagnostic pop
	}

	// try-catch block here?

	// how to turn RT PRI off?
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
		if (status != 0)
		{
			TLOG(TLVL_ERROR)
			    << "Failed to set realtime priority to " << rt_priority_
			    << ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	//MPI_Barrier(local_group_comm_);

	TLOG(TLVL_DEBUG) << "Sending initial table.";
	auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
	auto nextSendTime = startTime;
	double delta_time;
	while (!stop_requested_ && !pause_requested_)
	{
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		if (startTime >= nextSendTime)
		{
			auto table = policy_->GetCurrentTable();
			if (!table.empty())
			{
				send_event_table(table);
				++table_update_count_;
				delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
				statsHelperPtr_->addSample(TABLE_UPDATES_STAT_KEY, delta_time);
				TLOG(16) << "process_fragments TABLE_UPDATES_STAT_KEY=" << delta_time;

				bool readyToReport = statsHelperPtr_->readyToReport();
				if (readyToReport)
				{
					std::string statString = buildStatisticsString_();
					TLOG(TLVL_INFO) << statString;
					sendMetrics_();
				}
			}
			else
			{
				TLOG(TLVL_TRACE) << "No tokens received in this update interval (" << current_table_interval_ms_ << " ms)! This most likely means that the receivers are not keeping up!";
			}
			auto max_tokens = policy_->GetMaxNumberOfTokens();
			if (max_tokens > 0)
			{
				auto frac = table.size() / static_cast<double>(max_tokens);
				if (frac > 0.75)
				{
					current_table_interval_ms_ = 9 * current_table_interval_ms_ / 10;
				}
				if (frac < 0.5)
				{
					current_table_interval_ms_ = 11 * current_table_interval_ms_ / 10;
				}
				if (current_table_interval_ms_ > max_table_update_interval_ms_)
				{
					current_table_interval_ms_ = max_table_update_interval_ms_;
				}
				if (current_table_interval_ms_ < 1)
				{
					current_table_interval_ms_ = 1;
				}
			}
			nextSendTime = startTime + current_table_interval_ms_ / 1000.0;
			TLOG(TLVL_TRACE) << "current_table_interval_ms is now " << current_table_interval_ms_;
		}
		else
		{
			usleep(current_table_interval_ms_ * 10);  // 1/100 of the table update interval
		}
	}

	TLOG(TLVL_DEBUG) << "stop_requested_ is " << stop_requested_ << ", pause_requested_ is " << pause_requested_ << ", exiting process_event_table loop";
	policy_->Reset();
	metricMan->do_stop();
}

void artdaq::RoutingManagerCore::send_event_table(detail::RoutingPacket packet)
{
	// Reconnect table socket, if necessary
	if (table_socket_ == -1)
	{
		table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (table_socket_ < 0)
		{
			TLOG(TLVL_ERROR) << "I failed to create the socket for sending Data Requests! Errno: " << errno;
			exit(1);
		}
		auto sts = ResolveHost(send_tables_address_.c_str(), send_tables_port_, send_tables_addr_);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Unable to resolve table_update_address";
			exit(1);
		}

		auto yes = 1;
		if (multicast_out_hostname_ != "localhost")
		{
			TLOG(TLVL_DEBUG) << "Making sure that multicast sending uses the correct interface for hostname " << multicast_out_hostname_;
			struct in_addr addr;
			sts = GetInterfaceForNetwork(multicast_out_hostname_.c_str(), addr);
			if (sts == -1)
			{
				throw art::Exception(art::errors::Configuration) << "RoutingManagerCore: Unable to determine the multicast interface address from the routing_manager_address parameter value of " << multicast_out_hostname_ << std::endl;  // NOLINT(cert-err60-cpp)
				exit(1);
			}
			char addr_str[INET_ADDRSTRLEN];
			inet_ntop(AF_INET, &(addr), addr_str, INET_ADDRSTRLEN);
			TLOG(TLVL_INFO) << "Successfully determined the multicast interface address for " << multicast_out_hostname_ << ": " << addr_str << " (RoutingManager sending table updates to BoardReaders)";

			if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
			{
				TLOG(TLVL_ERROR) << "RoutingManagerCore: Unable to enable port reuse on table update socket";
				throw art::Exception(art::errors::Configuration) << "RoutingManagerCore: Unable to enable port reuse on table update socket" << std::endl;  // NOLINT(cert-err60-cpp)
				exit(1);
			}

			if (setsockopt(table_socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &yes, sizeof(yes)) < 0)
			{
				TLOG(TLVL_ERROR) << "Unable to enable multicast loopback on table socket";
				exit(1);
			}
			if (setsockopt(table_socket_, IPPROTO_IP, IP_MULTICAST_IF, &addr, sizeof(addr)) == -1)
			{
				TLOG(TLVL_ERROR) << "Cannot set outgoing interface. Errno: " << errno;
				exit(1);
			}
		}
		if (setsockopt(table_socket_, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes)) == -1)
		{
			TLOG(TLVL_ERROR) << "Cannot set request socket to broadcast. Errno: " << errno;
			exit(1);
		}
	}

	// Reconnect ack socket, if necessary
	if (ack_socket_ == -1)
	{
		ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (ack_socket_ < 0)
		{
			throw art::Exception(art::errors::Configuration) << "RoutingManagerCore: Error creating socket for receiving table update acks!" << std::endl; // NOLINT(cert-err60-cpp)
			exit(1);
		}

		struct sockaddr_in si_me_request;

		auto yes = 1;
		if (setsockopt(ack_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		{
			TLOG(TLVL_ERROR) << "RoutingManagerCore: Unable to enable port reuse on ack socket. errno=" << errno;
			throw art::Exception(art::errors::Configuration) << "RoutingManagerCore: Unable to enable port reuse on ack socket" << std::endl;  // NOLINT(cert-err60-cpp)
			exit(1);
		}

		// 10-Apr-2019, KAB: debugging information about the size of the receive buffer
		int sts;
		int len = 0;
		socklen_t arglen = sizeof(len);
		sts = getsockopt(ack_socket_, SOL_SOCKET, SO_RCVBUF, &len, &arglen);
		TLOG(TLVL_INFO) << "ACK RCVBUF initial: " << len << " sts/errno=" << sts << "/" << errno << " arglen=" << arglen;

		memset(&si_me_request, 0, sizeof(si_me_request));
		si_me_request.sin_family = AF_INET;
		si_me_request.sin_port = htons(receive_acks_port_);
		si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
		if (bind(ack_socket_, reinterpret_cast<struct sockaddr*>(&si_me_request), sizeof(si_me_request)) == -1) // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
		{
			TLOG(TLVL_ERROR) << "RoutingManagerCore: Cannot bind request socket to port " << receive_acks_port_ << ", errno=" << errno;
			throw art::Exception(art::errors::Configuration) << "RoutingManagerCore: Cannot bind request socket to port " << receive_acks_port_ << std::endl;  // NOLINT(cert-err60-cpp)
			exit(1);
		}
		TLOG(TLVL_DEBUG) << "Listening for acks on 0.0.0.0 port " << receive_acks_port_;
	}

	auto acks = std::unordered_map<int, bool>();
	for (auto& r : active_ranks_)
	{
		acks[r] = false;
	}
	auto counter = 0U;
	auto start_time = std::chrono::steady_clock::now();
	while (std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) { return !p.second; }) > 0 && !stop_requested_)
	{
		// Send table update
		auto header = detail::RoutingPacketHeader(routing_mode_, packet.size());
		auto packetSize = sizeof(detail::RoutingPacketEntry) * packet.size();

		// 10-Apr-2019, KAB: added information on which senders have already acknowledged this update
		for (auto& ack : acks)
		{
			TLOG(27) << "Table update already acknowledged? rank " << ack.first << " is " << ack.second
			         << " (size of 'already_acknowledged_ranks bitset is " << (8 * sizeof(header.already_acknowledged_ranks)) << ")";
			if (ack.first < static_cast<int>(8 * sizeof(header.already_acknowledged_ranks)))
			{
				if (ack.second) { header.already_acknowledged_ranks.set(ack.first); }
			}
		}

		assert(packetSize + sizeof(header) < MAX_ROUTING_TABLE_SIZE);
		std::vector<uint8_t> buffer(packetSize + sizeof(header));
		memcpy(&buffer[0], &header, sizeof(detail::RoutingPacketHeader));
		memcpy(&buffer[sizeof(detail::RoutingPacketHeader)], &packet[0], packetSize);

		TLOG(TLVL_DEBUG) << "Sending table information for " << header.nEntries << " events to multicast group " << send_tables_address_ << ", port " << send_tables_port_ << ", outgoing interface " << multicast_out_hostname_;
		TRACE(16, "headerData:0x%016lx%016lx packetData:0x%016lx%016lx", ((unsigned long*)&header)[0], ((unsigned long*)&header)[1], ((unsigned long*)&packet[0])[0], ((unsigned long*)&packet[0])[1]);  // NOLINT
		auto sts = sendto(table_socket_, &buffer[0], buffer.size(), 0, reinterpret_cast<struct sockaddr*>(&send_tables_addr_), sizeof(send_tables_addr_));                                               // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
		if (sts != static_cast<ssize_t>(buffer.size()))
		{
			TLOG(TLVL_ERROR) << "Error sending routing table. sts=" << sts;
		}

		// Collect acks

		auto first = packet[0].sequence_id;
		auto last = packet.rbegin()->sequence_id;
		TLOG(TLVL_DEBUG) << "Sent " << sts << " bytes. Expecting acks to have first= " << first << ", and last= " << last;

		auto startTime = std::chrono::steady_clock::now();
		while (std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) { return !p.second; }) > 0)
		{
			auto table_ack_wait_time_ms = current_table_interval_ms_ / max_ack_cycle_count_;
			if (TimeUtils::GetElapsedTimeMilliseconds(startTime) > table_ack_wait_time_ms)
			{
				if (++counter > max_ack_cycle_count_ && table_update_count_ > 0)
				{
					TLOG(TLVL_WARNING) << "Did not receive acks from all senders after resending table " << counter
					                   << " times during the table_update_interval. Check the status of the senders!";
				}
				else
				{
					TLOG(TLVL_WARNING) << "Did not receive acks from all senders within the timeout (" << table_ack_wait_time_ms << " ms). Resending table update";
				}

				if (std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) { return !p.second; }) <= 3)
				{
					auto ackIter = acks.begin();
					while (ackIter != acks.end())
					{
						if (!ackIter->second)
						{
							TLOG(TLVL_TRACE) << "Did not receive ack from rank " << ackIter->first;
						}
						++ackIter;
					}
				}
				break;
			}

			TLOG(20) << "send_event_table: Polling Request socket for new requests";
			auto ready = true;
			while (ready)
			{
				detail::RoutingAckPacket buffer;
				if (recvfrom(ack_socket_, &buffer, sizeof(detail::RoutingAckPacket), MSG_DONTWAIT, nullptr, nullptr) < 0)
				{
					if (errno == EWOULDBLOCK || errno == EAGAIN)
					{
						TLOG(20) << "send_event_table: No more ack datagrams on ack socket.";
						ready = false;
					}
					else
					{
						TLOG(TLVL_ERROR) << "An unexpected error occurred during ack packet receive";
						exit(2);
					}
				}
				else
				{
					TLOG(TLVL_DEBUG) << "Ack packet from rank " << buffer.rank << " has first= " << buffer.first_sequence_id
					                 << " and last= " << buffer.last_sequence_id << ", packet_size=" << sizeof(detail::RoutingAckPacket);
					if ((acks.count(buffer.rank) != 0u) && buffer.first_sequence_id == first && buffer.last_sequence_id == last)
					{
						TLOG(TLVL_DEBUG) << "Received table update acknowledgement from sender with rank " << buffer.rank << ".";
						acks[buffer.rank] = true;
						TLOG(TLVL_DEBUG) << "There are now " << std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) { return !p.second; })
						                 << " acks outstanding";
					}
					else if ((acks.count(buffer.rank) != 0u) && detail::RoutingAckPacket::isEndOfDataRoutingAckPacket(buffer))
					{
						TLOG(TLVL_INFO) << "Received table update acknowledgement indicating end-of-data from rank " << buffer.rank << ".";
						acks[buffer.rank] = true;
						active_ranks_.erase(buffer.rank);
					}
					else
					{
						if (acks.count(buffer.rank) == 0u)
						{
							TLOG(TLVL_ERROR) << "Received acknowledgement from invalid rank " << buffer.rank << "!"
							                 << " Cross-talk between RoutingManagers means there's a configuration error!";
						}
						else
						{
							TLOG(TLVL_WARNING) << "Received acknowledgement from rank " << buffer.rank
							                   << " that had incorrect sequence ID information. Discarding."
							                   << " Expected first/last=" << first << "/" << last
							                   << " recvd=" << buffer.first_sequence_id << "/" << buffer.last_sequence_id;
						}
					}
				}
			}
			usleep(table_ack_wait_time_ms * 1000 / 10);
		}
	}
	if (metricMan)
	{
		artdaq::TimeUtils::seconds delta = std::chrono::steady_clock::now() - start_time;
		metricMan->sendMetric("Avg Table Acknowledge Time", delta.count(), "seconds", 3, MetricMode::Average);
	}
}

std::string artdaq::RoutingManagerCore::report(std::string const& /*unused*/) const
{
	std::string resultString;

	// if we haven't been able to come up with any report so far, say so
	auto tmpString = app_name + " run number = " + std::to_string(run_id_.run()) + ", table updates sent = " + std::to_string(table_update_count_) + ", Receiver tokens received = " + std::to_string(token_receiver_->getReceivedTokenCount());
	return tmpString;
}

std::string artdaq::RoutingManagerCore::buildStatisticsString_() const
{
	std::ostringstream oss;
	oss << app_name << " statistics:" << std::endl;

	auto mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TABLE_UPDATES_STAT_KEY);
	if (mqPtr != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Table Update statistics: "
		    << stats.recentSampleCount << " table updates sent at "
		    << stats.recentSampleRate << " table updates/sec, , monitor window = "
		    << stats.recentDuration << " sec" << std::endl;
		oss << "  Average times per table update: ";
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
			    << (1.0 / stats.recentSampleRate) << " sec";
		}
		oss << ", avg table acknowledgement wait time = "
		    << (mqPtr->getRecentValueSum() / sender_ranks_.size()) << " sec" << std::endl;
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TOKENS_RECEIVED_STAT_KEY);
	if (mqPtr != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		oss << "  Received Token statistics: "
		    << stats.recentSampleCount << " tokens received at "
		    << stats.recentSampleRate << " tokens/sec, , monitor window = "
		    << stats.recentDuration << " sec" << std::endl;
		oss << "  Average times per token: ";
		if (stats.recentSampleRate > 0.0)
		{
			oss << " elapsed time = "
			    << (1.0 / stats.recentSampleRate) << " sec";
		}
		oss << ", input token wait time = "
		    << mqPtr->getRecentValueSum() << " sec" << std::endl;
	}

	return oss.str();
}

void artdaq::RoutingManagerCore::sendMetrics_()
{
	if (metricMan)
	{
		auto mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TABLE_UPDATES_STAT_KEY);
		if (mqPtr != nullptr)
		{
			artdaq::MonitoredQuantityStats stats;
			mqPtr->getStats(stats);
			metricMan->sendMetric("Table Update Count", stats.fullSampleCount, "updates", 1, MetricMode::LastPoint);
			metricMan->sendMetric("Table Update Rate", stats.recentSampleRate, "updates/sec", 1, MetricMode::Average);
			metricMan->sendMetric("Average Sender Acknowledgement Time", (mqPtr->getRecentValueSum() / sender_ranks_.size()), "seconds", 3, MetricMode::Average);
		}

		mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TOKENS_RECEIVED_STAT_KEY);
		if (mqPtr != nullptr)
		{
			artdaq::MonitoredQuantityStats stats;
			mqPtr->getStats(stats);
			metricMan->sendMetric("Receiver Token Count", stats.fullSampleCount, "updates", 1, MetricMode::LastPoint);
			metricMan->sendMetric("Receiver Token Rate", stats.recentSampleRate, "updates/sec", 1, MetricMode::Average);
			metricMan->sendMetric("Total Receiver Token Wait Time", mqPtr->getRecentValueSum(), "seconds", 3, MetricMode::Average);
		}
	}
}