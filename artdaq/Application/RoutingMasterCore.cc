#include <sys/un.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <sched.h>
#include <algorithm>

#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/Application/RoutingMasterCore.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/Routing/makeRoutingMasterPolicy.hh"
#include "artdaq/DAQdata/TCP_listen_fd.hh"
#include "artdaq/DAQdata/TCPConnect.hh"

#define TRACE_NAME "RoutingMasterCore"

const std::string artdaq::RoutingMasterCore::
TABLE_UPDATES_STAT_KEY("RoutingMasterCoreTableUpdates");
const std::string artdaq::RoutingMasterCore::
TOKENS_RECEIVED_STAT_KEY("RoutingMasterCoreTokensReceived");

artdaq::RoutingMasterCore::RoutingMasterCore(int rank, std::string name) :
	name_(name)
	, received_token_counter_()
	, shutdown_requested_(false)
	, stop_requested_(false)
	, pause_requested_(false)
	, token_socket_(-1)
	, table_socket_(-1)
	, ack_socket_(-1)
{
	TLOG_DEBUG(name_) << "Constructor" << TLOG_ENDL;
	statsHelper_.addMonitoredQuantityName(TABLE_UPDATES_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(TOKENS_RECEIVED_STAT_KEY);
	metricMan = &metricMan_;
	my_rank = rank;
}

artdaq::RoutingMasterCore::~RoutingMasterCore()
{
	TLOG_DEBUG(name_) << "Destructor" << TLOG_ENDL;
	if (ev_token_receive_thread_.joinable()) ev_token_receive_thread_.join();
}

bool artdaq::RoutingMasterCore::initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG_ERROR(name_)
			<< "Unable to find the DAQ parameters in the initialization "
			<< "ParameterSet: \"" + pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}
	try
	{
		policy_pset_ = daq_pset.get<fhicl::ParameterSet>("policy");
	}
	catch (...)
	{
		TLOG_ERROR(name_)
			<< "Unable to find the policy parameters in the DAQ initialization ParameterSet: \"" + daq_pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}

	// pull out the Metric part of the ParameterSet
	fhicl::ParameterSet metric_pset;
	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL 

	if (metric_pset.is_empty())
	{
		TLOG_INFO(name_) << "No metric plugins appear to be defined" << TLOG_ENDL;
	}
	try
	{
		metricMan_.initialize(metric_pset, name_);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
						 "Error loading metrics in RoutingMasterCore::initialize()");
	}

	// create the requested CommandableFragmentGenerator
	auto policy_plugin_spec = policy_pset_.get<std::string>("policy", "");
	if (policy_plugin_spec.length() == 0)
	{
		TLOG_ERROR(name_)
			<< "No fragment generator (parameter name = \"policy\") was "
			<< "specified in the policy ParameterSet.  The "
			<< "DAQ initialization PSet was \"" << daq_pset.to_string() << "\"." << TLOG_ENDL;
		return false;
	}
	try
	{
		policy_ = artdaq::makeRoutingMasterPolicy(policy_plugin_spec, policy_pset_);
	}
	catch (...)
	{
		std::stringstream exception_string;
		exception_string << "Exception thrown during initialization of policy of type \""
			<< policy_plugin_spec << "\"";

		ExceptionHandler(ExceptionHandlerRethrow::no, exception_string.str());

		TLOG_DEBUG(name_) << "FHiCL parameter set used to initialize the policy which threw an exception: " << policy_pset_.to_string() << TLOG_ENDL;

		return false;
	}

	rt_priority_ = daq_pset.get<int>("rt_priority", 0);
	sender_ranks_ = daq_pset.get<std::vector<int>>("sender_ranks");
	num_receivers_ = policy_->GetReceiverCount();

	receive_ack_events_ = std::vector<epoll_event>(sender_ranks_.size());
	receive_token_events_ = std::vector<epoll_event>(num_receivers_);

	auto mode = daq_pset.get<bool>("senders_send_by_send_count", false);
	routing_mode_ = mode ? detail::RoutingMasterMode::RouteBySendCount : detail::RoutingMasterMode::RouteBySequenceID;
	max_table_update_interval_ms_ = daq_pset.get<size_t>("table_update_interval_ms", 1000);
	current_table_interval_ms_ = max_table_update_interval_ms_;
	max_ack_cycle_count_ = daq_pset.get<size_t>("table_ack_retry_count", 5);
	receive_token_port_ = daq_pset.get<int>("routing_token_port", 35555);
	send_tables_port_ = daq_pset.get<int>("table_update_port", 35556);
	receive_acks_port_ = daq_pset.get<int>("table_acknowledge_port", 35557);
	send_tables_address_ = daq_pset.get<std::string>("table_update_address", "227.128.12.28");
	receive_address_ = daq_pset.get<std::string>("routing_master_hostname", "localhost");

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(daq_pset, 100, 30.0, 60.0, TABLE_UPDATES_STAT_KEY);

	shutdown_requested_.store(false);
	start_recieve_token_thread_();
	return true;
}

bool artdaq::RoutingMasterCore::start(art::RunID id, uint64_t, uint64_t)
{
	stop_requested_.store(false);
	pause_requested_.store(false);

	statsHelper_.resetStatistics();
	policy_->Reset();

	metricMan_.do_start();
	run_id_ = id;
	table_update_count_ = 0;
	received_token_count_ = 0;

	TLOG_DEBUG(name_) << "Started run " << std::to_string(run_id_.run()) << TLOG_ENDL;
	return true;
}

bool artdaq::RoutingMasterCore::stop(uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "Stopping run " << std::to_string(run_id_.run())
		<< " after " << std::to_string(table_update_count_) << " table updates."
		<< " and " << received_token_count_ << " received tokens." << TLOG_ENDL;
	stop_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::pause(uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "Pausing run " << std::to_string(run_id_.run())
		<< " after " << table_update_count_ << " table updates."
		<< " and " << received_token_count_ << " received tokens." << TLOG_ENDL;
	pause_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::resume(uint64_t, uint64_t)
{
	TLOG_DEBUG(name_) << "Resuming run " << run_id_.run() << TLOG_ENDL;
	policy_->Reset();
	pause_requested_.store(false);
	metricMan_.do_start();
	return true;
}

bool artdaq::RoutingMasterCore::shutdown(uint64_t)
{
	policy_.reset(nullptr);
	metricMan_.shutdown();
	shutdown_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	TLOG_DEBUG(name_) << "soft_initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return initialize(pset, e, f);
}

bool artdaq::RoutingMasterCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	TLOG_DEBUG(name_) << "reinitialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return initialize(pset, e, f);
}

size_t artdaq::RoutingMasterCore::process_event_table()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		if (pthread_setschedparam(pthread_self(), SCHED_RR, &s_param))
			TLOG_WARNING(name_) << "setting realtime priority failed" << TLOG_ENDL;
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
			TLOG_ERROR(name_)
				<< "Failed to set realtime priority to " << std::to_string(rt_priority_)
				<< ", return code = " << status << TLOG_ENDL;
		}
#pragma GCC diagnostic pop
	}

	//MPI_Barrier(local_group_comm_);

	TLOG_DEBUG(name_) << "Sending initial table." << TLOG_ENDL;
	auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
	auto nextSendTime = startTime;
	double delta_time;
	while (true)
	{
		if (stop_requested_ || pause_requested_) { break; }
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		if (startTime >= nextSendTime)
		{
			auto table = policy_->GetCurrentTable();
			if (table.size() > 0)
			{
				send_event_table(table);
				++table_update_count_;
				delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
				statsHelper_.addSample(TABLE_UPDATES_STAT_KEY, delta_time);
				TRACE(16, "%s::process_fragments TABLE_UPDATES_STAT_KEY=%f", name_.c_str(), delta_time);
			}
			else
			{
				TLOG_WARNING(name_) << "No tokens received in this update interval! This is most likely a Very Bad Thing!" << TLOG_ENDL;
			}
			auto max_tokens = policy_->GetMaxNumberOfTokens();
			if (max_tokens > 0)
			{
				auto frac = table.size() / static_cast<double>(max_tokens);
				if (frac > 0.75) current_table_interval_ms_ = 9 * current_table_interval_ms_ / 10;
				if (frac < 0.5) current_table_interval_ms_ = 11 * current_table_interval_ms_ / 10;
				if (current_table_interval_ms_ > max_table_update_interval_ms_) current_table_interval_ms_ = max_table_update_interval_ms_;
				if (current_table_interval_ms_ < 1) current_table_interval_ms_ = 1;
			}
			nextSendTime = startTime + current_table_interval_ms_ / 1000.0;
			TLOG_DEBUG(name_) << "current_table_interval_ms is now " << current_table_interval_ms_ << TLOG_ENDL;
		}
		else
		{
			usleep(current_table_interval_ms_ * 10); // 1/100 of the table update interval
		}
	}

	metricMan_.do_stop();

	policy_.reset(nullptr);
	return table_update_count_;
}

void artdaq::RoutingMasterCore::send_event_table(detail::RoutingPacket packet)
{
	// Reconnect table socket, if necessary
	if (table_socket_ == -1)
	{
		table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (!table_socket_)
		{
			TLOG_ERROR(name_) << "I failed to create the socket for sending Data Requests! Errno: " << std::to_string(errno) << TLOG_ENDL;
			exit(1);
		}
		auto sts = ResolveHost(send_tables_address_.c_str(), send_tables_port_, send_tables_addr_);
		if (sts == -1)
		{
			TLOG_ERROR(name_) << "Unable to resolve table_update_address" << TLOG_ENDL;
			exit(1);
		}

		auto yes = 1;
		if (receive_address_ != "localhost")
		{
			TLOG_DEBUG(name_) << "Making sure that multicast sending uses the correct interface for hostname " << receive_address_ << TLOG_ENDL;
			struct in_addr addr;
			sts = ResolveHost(receive_address_.c_str(), addr);
			if (sts == -1)
			{
				throw art::Exception(art::errors::Configuration) << "RoutingMasterCore: Unable to resolve routing_master_address" << std::endl;;
			}

			if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
			{
				throw art::Exception(art::errors::Configuration) <<
					"RoutingMasterCore: Unable to enable port reuse on table update socket" << std::endl;
				exit(1);
			}

			if (setsockopt(table_socket_, IPPROTO_IP, IP_MULTICAST_IF, &addr, sizeof(addr)) == -1)
			{
				TLOG_ERROR(name_) << "Cannot set outgoing interface. Errno: " << std::to_string(errno) << TLOG_ENDL;
				exit(1);
			}
		}
		if (setsockopt(table_socket_, SOL_SOCKET, SO_BROADCAST, (void*)&yes, sizeof(int)) == -1)
		{
			TLOG_ERROR(name_) << "Cannot set request socket to broadcast. Errno: " << std::to_string(errno) << TLOG_ENDL;
			exit(1);
		}
	}

	// Reconnect ack socket, if necessary
	if (ack_socket_ == -1)
	{
		ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (!ack_socket_)
		{
			throw art::Exception(art::errors::Configuration) << "RoutingMasterCore: Error creating socket for receiving table update acks!" << std::endl;
			exit(1);
		}

		struct sockaddr_in si_me_request;

		auto yes = 1;
		if (setsockopt(ack_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		{
			throw art::Exception(art::errors::Configuration) <<
				"RoutingMasterCore: Unable to enable port reuse on ack socket" << std::endl;
			exit(1);
		}
		memset(&si_me_request, 0, sizeof(si_me_request));
		si_me_request.sin_family = AF_INET;
		si_me_request.sin_port = htons(receive_acks_port_);
		si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
		if (bind(ack_socket_, reinterpret_cast<struct sockaddr *>(&si_me_request), sizeof(si_me_request)) == -1)
		{
			throw art::Exception(art::errors::Configuration) <<
				"RoutingMasterCore: Cannot bind request socket to port " << receive_acks_port_ << std::endl;
			exit(1);
		}
		TLOG_DEBUG(name_) << "Listening for acks on 0.0.0.0 port " << receive_acks_port_ << TLOG_ENDL;
	}

	auto acks = std::unordered_map<int, bool>();
	for (auto& r : sender_ranks_)
	{
		acks[r] = false;
	}
	auto counter = 0U;
	auto start_time = std::chrono::steady_clock::now();
	while (std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) {return !p.second; }) > 0)
	{
		// Send table update
		auto header = detail::RoutingPacketHeader(routing_mode_, packet.size());
		auto packetSize = sizeof(detail::RoutingPacketEntry) * packet.size();

		TLOG_DEBUG(name_) << "Sending table information for " << std::to_string(header.nEntries) << " events to multicast group " << send_tables_address_ << ", port " << send_tables_port_ << TLOG_ENDL;
		if (sendto(table_socket_, &header, sizeof(detail::RoutingPacketHeader), 0, reinterpret_cast<struct sockaddr *>(&send_tables_addr_), sizeof(send_tables_addr_)) < 0)
		{
			TLOG_ERROR(name_) << "Error sending request message header" << TLOG_ENDL;
		}
		if (sendto(table_socket_, &packet[0], packetSize, 0, reinterpret_cast<struct sockaddr *>(&send_tables_addr_), sizeof(send_tables_addr_)) < 0)
		{
			TLOG_ERROR(name_) << "Error sending request message data" << TLOG_ENDL;
		}

		// Collect acks

		auto first = packet[0].sequence_id;
		auto last = packet.rbegin()->sequence_id;
		TLOG_DEBUG(name_) << "Expecting acks to have first= " << std::to_string(first) << ", and last= " << std::to_string(last) << TLOG_ENDL;


		auto startTime = std::chrono::steady_clock::now();
		while (std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) {return !p.second; }) > 0)
		{
			auto currentTime = std::chrono::steady_clock::now();
			auto table_ack_wait_time_ms = current_table_interval_ms_ / max_ack_cycle_count_;
			if (static_cast<size_t>(std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - startTime).count()) > table_ack_wait_time_ms)
			{
				if (counter > max_ack_cycle_count_ && table_update_count_ > 0)
				{
					TLOG_ERROR(name_) << "Did not receive acks from all senders after resending table " << std::to_string(counter) << " times during the table_update_interval. Check the status of the senders!" << TLOG_ENDL;
					break;
				}
				TLOG_WARNING(name_) << "Did not receive acks from all senders within the table_ack_wait_time. Resending table update" << TLOG_ENDL;
				break;
			}

			TLOG_ARB(20, name_) << "send_event_table: Polling Request socket for new requests" << TLOG_ENDL;
			auto ready = true;
			while (ready)
			{
				detail::RoutingAckPacket buffer;
				if (recvfrom(ack_socket_, &buffer, sizeof(detail::RoutingAckPacket), MSG_DONTWAIT, NULL, NULL) < 0)
				{
					if (errno == EWOULDBLOCK || errno == EAGAIN)
					{
						TLOG_ARB(20, name_) << "send_event_table: No more ack datagrams on ack socket." << TLOG_ENDL;
						ready = false;
					}
					else
					{
						TLOG_ERROR(name_) << "An unexpected error occurred during ack packet receive" << TLOG_ENDL;
						exit(2);
					}
				}
				else
				{
					TLOG_DEBUG(name_) << "Ack packet from rank " << buffer.rank << " has first= " << std::to_string(buffer.first_sequence_id) << " and last= " << std::to_string(buffer.last_sequence_id) << TLOG_ENDL;
					if (acks.count(buffer.rank) && buffer.first_sequence_id == first && buffer.last_sequence_id == last)
					{
						TLOG_DEBUG(name_) << "Received table update acknowledgement from sender with rank " << std::to_string(buffer.rank) << "." << TLOG_ENDL;
						acks[buffer.rank] = true;
						TLOG_DEBUG(name_) << "There are now " << std::count_if(acks.begin(), acks.end(), [](std::pair<int, bool> p) {return !p.second; }) << " acks outstanding" << TLOG_ENDL;
					}
					else
					{
						if (!acks.count(buffer.rank)) { TLOG_ERROR(name_) << "Received acknowledgement from invalid rank " << buffer.rank << "! Cross-talk between RoutingMasters means there's a configuration error!" << TLOG_ENDL; }
						else { TLOG_WARNING(name_) << "Received acknowledgement from rank " << buffer.rank << " that had incorrect sequence ID information. Discarding." << TLOG_ENDL; }
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

void artdaq::RoutingMasterCore::receive_tokens_()
{
	while (!shutdown_requested_)
	{
		TLOG_DEBUG(name_) << "Receive Token loop start" << TLOG_ENDL;
		if (token_socket_ == -1)
		{
			TLOG_DEBUG(name_) << "Opening token listener socket" << TLOG_ENDL;
			token_socket_ = TCP_listen_fd(receive_token_port_, 3 * sizeof(detail::RoutingToken));

			if (token_epoll_fd_ != -1) close(token_epoll_fd_);
			struct epoll_event ev;
			token_epoll_fd_ = epoll_create1(0);
			ev.events = EPOLLIN | EPOLLPRI;
			ev.data.fd = token_socket_;
			if (epoll_ctl(token_epoll_fd_, EPOLL_CTL_ADD, token_socket_, &ev) == -1)
			{
				TLOG_ERROR(name_) << "Could not register listen socket to epoll fd" << TLOG_ENDL;
				exit(3);
			}
		}
		if (token_socket_ == -1 || token_epoll_fd_ == -1)
		{
			TLOG_DEBUG(name_) << "One of the listen sockets was not opened successfully." << TLOG_ENDL;
			return;
		}

		auto nfds = epoll_wait(token_epoll_fd_, &receive_token_events_[0], receive_token_events_.size(), current_table_interval_ms_);
		if (nfds == -1)
		{
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		TLOG_DEBUG(name_) << "Received " << std::to_string(nfds) << " events" << TLOG_ENDL;
		for (auto n = 0; n < nfds; ++n)
		{
			if (receive_token_events_[n].data.fd == token_socket_)
			{
				TLOG_DEBUG(name_) << "Accepting new connection on token_socket" << TLOG_ENDL;
				sockaddr_in addr;
				socklen_t arglen = sizeof(addr);
				auto conn_sock = accept(token_socket_, (struct sockaddr *)&addr, &arglen);

				if (conn_sock == -1)
				{
					perror("accept");
					exit(EXIT_FAILURE);
				}

				receive_token_addrs_[conn_sock] = std::string(inet_ntoa(addr.sin_addr));
				struct epoll_event ev;
				ev.events = EPOLLIN | EPOLLET;
				ev.data.fd = conn_sock;
				if (epoll_ctl(token_epoll_fd_, EPOLL_CTL_ADD, conn_sock, &ev) == -1)
				{
					perror("epoll_ctl: conn_sock");
					exit(EXIT_FAILURE);
				}
			}
			else
			{
				auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
				detail::RoutingToken buff;
				auto sts = read(receive_token_events_[n].data.fd, &buff, sizeof(detail::RoutingToken));
				if (sts != sizeof(detail::RoutingToken) || buff.header != TOKEN_MAGIC)
				{
					TLOG_ERROR(name_) << "Received invalid token from " << receive_token_addrs_[receive_token_events_[n].data.fd] << TLOG_ENDL;
				}
				else
				{
					TLOG_DEBUG(name_) << "Received token from " << std::to_string(buff.rank) << " indicating " << buff.new_slots_free << " slots are free." << TLOG_ENDL;
					received_token_count_ += buff.new_slots_free;
					if (routing_mode_ == detail::RoutingMasterMode::RouteBySequenceID)
					{
						policy_->AddReceiverToken(buff.rank, buff.new_slots_free);
					}
					else if (routing_mode_ == detail::RoutingMasterMode::RouteBySendCount)
					{
						if (!received_token_counter_.count(buff.rank)) received_token_counter_[buff.rank] = 0;
						received_token_counter_[buff.rank] += buff.new_slots_free;
						TLOG_DEBUG(name_) << "RoutingMasterMode is RouteBySendCount. I have " << received_token_counter_[buff.rank] << " tokens for rank " << buff.rank << " and I need " << sender_ranks_.size() << "." << TLOG_ENDL;
						while (received_token_counter_[buff.rank] >= sender_ranks_.size())
						{
							TLOG_DEBUG(name_) << "RoutingMasterMode is RouteBySendCount. I have " << received_token_counter_[buff.rank] << " tokens for rank " << buff.rank << " and I need " << sender_ranks_.size()
								<< "... Sending token to policy" << TLOG_ENDL;
							policy_->AddReceiverToken(buff.rank, 1);
							received_token_counter_[buff.rank] -= sender_ranks_.size();
						}
					}
				}
				auto delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
				statsHelper_.addSample(TOKENS_RECEIVED_STAT_KEY, delta_time);

			}
		}
	}
}

void artdaq::RoutingMasterCore::start_recieve_token_thread_()
{
	if (ev_token_receive_thread_.joinable()) ev_token_receive_thread_.join();
	TLOG_INFO(name_) << "Starting Token Reception Thread" << TLOG_ENDL;
	ev_token_receive_thread_ = std::thread(&RoutingMasterCore::receive_tokens_, this);
}

std::string artdaq::RoutingMasterCore::report(std::string const&) const
{
	std::string resultString;

	// if we haven't been able to come up with any report so far, say so
	auto tmpString = name_ + " run number = " + std::to_string(run_id_.run())
		+ ", table updates sent = " + std::to_string(table_update_count_)
		+ ", Receiver tokens received = " + std::to_string(received_token_count_);
	return tmpString;
}

std::string artdaq::RoutingMasterCore::buildStatisticsString_() const
{
	std::ostringstream oss;
	oss << name_ << " statistics:" << std::endl;

	auto mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TABLE_UPDATES_STAT_KEY);
	if (mqPtr.get() != nullptr)
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
	if (mqPtr.get() != nullptr)
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

void artdaq::RoutingMasterCore::sendMetrics_()
{
	auto mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TABLE_UPDATES_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		metricMan_.sendMetric("Table Update Count", static_cast<unsigned long>(stats.fullSampleCount), "updates", 1, MetricMode::Accumulate);
		metricMan_.sendMetric("Table Update Rate", stats.recentSampleRate, "updates/sec", 1, MetricMode::Average);
		metricMan_.sendMetric("Average Sender Acknowledgement Time", (mqPtr->getRecentValueSum() / sender_ranks_.size()), "seconds", 3, MetricMode::Average);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TOKENS_RECEIVED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		metricMan_.sendMetric("Receiver Token Count", static_cast<unsigned long>(stats.fullSampleCount), "updates", 1, MetricMode::Accumulate);
		metricMan_.sendMetric("Receiver Token Rate", stats.recentSampleRate, "updates/sec", 1, MetricMode::Average);
		metricMan_.sendMetric("Total Receiver Token Wait Time", mqPtr->getRecentValueSum(), "seconds", 3, MetricMode::Average);
	}
}
