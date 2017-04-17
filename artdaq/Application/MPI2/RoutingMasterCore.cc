#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/MPI2/RoutingMasterCore.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "canvas/Utilities/Exception.h"
#include "cetlib/exception.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include <pthread.h>
#include <sched.h>
#include <algorithm>
#include "trace.h"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/Routing/makeRoutingMasterPolicy.hh"
#include <sys/un.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include "artdaq/TransferPlugins/detail/TCP_listen_fd.hh"

#define TRACE_NAME "RoutingMasterCore"

const std::string artdaq::RoutingMasterCore::
TABLE_UPDATES_STAT_KEY("RoutingMasterCoreTableUpdates");
const std::string artdaq::RoutingMasterCore::
TOKENS_RECEIVED_STAT_KEY("RoutingMasterCoreTokensReceived");

/**
* Default constructor.
*/
artdaq::RoutingMasterCore::RoutingMasterCore(Commandable& parent_application,
											 MPI_Comm local_group_comm, std::string name) :
	parent_application_(parent_application)
	, local_group_comm_(local_group_comm)
	, name_(name)
	, stop_requested_(false)
	, pause_requested_(false)
	, token_socket_(-1)
	, table_socket_(-1)
	, ack_socket_(-1)
{
	mf::LogDebug(name_) << "Constructor";
	statsHelper_.addMonitoredQuantityName(TABLE_UPDATES_STAT_KEY);
	statsHelper_.addMonitoredQuantityName(TOKENS_RECEIVED_STAT_KEY);
	metricMan = &metricMan_;
}

/**
* Destructor.
*/
artdaq::RoutingMasterCore::~RoutingMasterCore()
{
	mf::LogDebug(name_) << "Destructor";
	if (ev_token_receive_thread_.joinable()) ev_token_receive_thread_.join();
}

/**
* Processes the initialize request.
*/
bool artdaq::RoutingMasterCore::initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "initialize method called with "
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
		mf::LogError(name_)
			<< "Unable to find the DAQ parameters in the initialization "
			<< "ParameterSet: \"" + pset.to_string() + "\".";
		return false;
	}
	try
	{
		policy_pset_ = daq_pset.get<fhicl::ParameterSet>("policy");
	}
	catch (...)
	{
		mf::LogError(name_)
			<< "Unable to find the policy parameters in the DAQ initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
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
		mf::LogInfo(name_) << "No metric plugins appear to be defined";
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
		mf::LogError(name_)
			<< "No fragment generator (parameter name = \"generator\") was "
			<< "specified in the fragment_receiver ParameterSet.  The "
			<< "DAQ initialization PSet was \"" << daq_pset.to_string() << "\".";
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

		mf::LogDebug(name_) << "FHiCL parameter set used to initialize the policy which threw an exception: " << policy_pset_.to_string();

		return false;
	}

	rt_priority_ = daq_pset.get<int>("rt_priority", 0);
	br_ranks_ = daq_pset.get<std::vector<int>>("boardreader_ranks");
	num_ebs_ = policy_->GetEventBuilderCount();

	receive_ack_events_ = std::vector<epoll_event>(br_ranks_.size());
	receive_token_events_ = std::vector<epoll_event>(num_ebs_);

	table_update_interval_ms_ = daq_pset.get<size_t>("table_update_interval_ms", 1000);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelper_.createCollectors(daq_pset, 100, 30.0, 60.0, TABLE_UPDATES_STAT_KEY);

	return true;
}

bool artdaq::RoutingMasterCore::start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	stop_requested_.store(false);
	pause_requested_.store(false);

	statsHelper_.resetStatistics();

	metricMan_.do_start();
	run_id_ = id;
	table_update_count_ = 0;
	received_token_count_ = 0;

	mf::LogDebug(name_) << "Started run " << run_id_.run() <<
		", timeout = " << timeout << ", timestamp = " << timestamp << std::endl;
	return true;
}

bool artdaq::RoutingMasterCore::stop(uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "Stopping run " << run_id_.run()
		<< " after " << table_update_count_ << " table updates."
		<< " and " << received_token_count_ << " received tokens.";
	stop_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::pause(uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "Pausing run " << run_id_.run()
		<< " after " << table_update_count_ << " table updates."
		<< " and " << received_token_count_ << " received tokens.";
	pause_requested_.store(true);
	return true;
}

bool artdaq::RoutingMasterCore::resume(uint64_t, uint64_t)
{
	mf::LogDebug(name_) << "Resuming run " << run_id_.run();
	pause_requested_.store(false);
	metricMan_.do_start();
	return true;
}

bool artdaq::RoutingMasterCore::shutdown(uint64_t)
{
	policy_.reset(nullptr);
	metricMan_.shutdown();
	return true;
}

bool artdaq::RoutingMasterCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	mf::LogDebug(name_) << "soft_initialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";
	return initialize(pset, e, f);
}

bool artdaq::RoutingMasterCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t e, uint64_t f)
{
	mf::LogDebug(name_) << "reinitialize method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\".";
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
			mf::LogWarning(name_) << "setting realtime priority failed";
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
			mf::LogError(name_)
				<< "Failed to set realtime priority to " << rt_priority_
				<< ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	start_recieve_token_thread_();

	MPI_Barrier(local_group_comm_);

	mf::LogDebug(name_) << "Sending initial table.";
	auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
	auto nextSendTime = startTime;
	double delta_time;
	while (true)
	{
		if (stop_requested_ || pause_requested_) { break; }
		startTime = artdaq::MonitoredQuantity::getCurrentTime();

		if (startTime >= nextSendTime)
		{
			send_event_table(policy_->GetCurrentTable());
			++table_update_count_;
			delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
			statsHelper_.addSample(TABLE_UPDATES_STAT_KEY, delta_time);
			TRACE(16, "%s::process_fragments TABLE_UPDATES_STAT_KEY=%f", name_.c_str(), delta_time);
			nextSendTime = startTime + table_update_interval_ms_ / 1000;
		}
		else
		{
			usleep(table_update_interval_ms_ * 10); // 1/100 of the table update interval
		}
	}

	if (ev_token_receive_thread_.joinable()) ev_token_receive_thread_.join();
	metricMan_.do_stop();

	policy_.reset(nullptr);
	return table_update_count_;
}

void artdaq::RoutingMasterCore::send_event_table(detail::RoutingPacket packet, std::vector<bool> acks, int level)
{
	if (acks.size() == 0) acks.resize(br_ranks_.size());
	// Reconnect table socket, if necessary
	if (table_socket_ == -1)
	{
		table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (!table_socket_)
		{
			mf::LogError("EventStore") << "I failed to create the socket for sending Data Requests!" << std::endl;
			exit(1);
		}
		send_tables_addr_.sin_addr.s_addr = inet_addr(send_tables_address_.c_str());
		send_tables_addr_.sin_port = htons(send_tables_port_);
		send_tables_addr_.sin_family = AF_INET;

		struct in_addr addr;
		addr.s_addr = inet_addr(receive_address_.c_str());

		if (setsockopt(table_socket_, IPPROTO_IP, IP_MULTICAST_IF, &addr, sizeof(addr)) == -1)
		{
			mf::LogError("EventStore") << "Cannot set outgoing interface." << std::endl;
			exit(1);
		}
		int yes = 1;
		if (setsockopt(table_socket_, SOL_SOCKET, SO_BROADCAST, (void*)&yes, sizeof(int)) == -1)
		{
			mf::LogError("EventStore") << "Cannot set request socket to broadcast." << std::endl;
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

		int yes = 1;
		if (setsockopt(ack_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		{
			throw art::Exception(art::errors::Configuration) <<
				"RoutingMasterCore: Unable to enable port reuse on request socket" << std::endl;
			exit(1);
		}
		memset(&si_me_request, 0, sizeof(si_me_request));
		si_me_request.sin_family = AF_INET;
		si_me_request.sin_port = htons(receive_acks_port_);
		si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
		if (bind(ack_socket_, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
		{
			throw art::Exception(art::errors::Configuration) <<
				"RoutingMasterCore: Cannot bind request socket to port " << receive_acks_port_ << std::endl;
			exit(1);
		}

		struct epoll_event ev;
		if (ack_epoll_fd_ != -1) close(ack_epoll_fd_);
		ack_epoll_fd_ = epoll_create1(0);
		if (ack_epoll_fd_ == -1)
		{
			mf::LogError("RoutingMasterCore") << "Could not create epoll fd";
			exit(3);
		}
		ev.events = EPOLLIN | EPOLLPRI;
		ev.data.fd = ack_socket_;
		if (epoll_ctl(ack_epoll_fd_, EPOLL_CTL_ADD, ack_socket_, &ev) == -1)
		{
			mf::LogError("RoutingMasterCore") << "Could not register listen socket to epoll fd";
			exit(3);
		}
	}

	// Send table update
	auto header = detail::RoutingPacketHeader(packet.size());
	auto packetSize = sizeof(detail::RoutingPacketEntry) * packet.size();

	mf::LogDebug("RoutingMasterCore") << "Sending request for " << std::to_string(header.nEntries) << " events to multicast group " << send_tables_address_ << std::endl;
	if (sendto(table_socket_, &header, sizeof(detail::RoutingPacketHeader), 0, (struct sockaddr *)&send_tables_addr_, sizeof(send_tables_addr_)) < 0)
	{
		mf::LogError("RoutingMasterCore") << "Error sending request message header" << std::endl;
	}
	if (sendto(table_socket_, &packet[0], packetSize, 0, (struct sockaddr *)&send_tables_addr_, sizeof(send_tables_addr_)) < 0)
	{
		mf::LogError("RoutingMasterCore") << "Error sending request message data" << std::endl;
	}

	// Collect acks

	auto first = packet[0].sequence_id;
	auto last = packet.rbegin()->sequence_id;


	auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
	while (std::count_if(acks.begin(), acks.end(), [](bool e) {return !e; }) > 0)
	{
		auto currentTime = artdaq::MonitoredQuantity::getCurrentTime();
		if (currentTime - startTime > table_ack_wait_time_ms_ / 1000)
		{
			if (level * table_ack_wait_time_ms_ > table_update_interval_ms_)
			{
				mf::LogError("RoutingMasterCore") << "Did not receive acks from all BRs after resending table " << std::to_string(level) << " times during the table_update_interval. Aborting";
				exit(2);
			}
			mf::LogWarning("RoutingMasterCore") << "Did not receive acks from all BRs within the table_ack_wait_time. Resending table update";
			send_event_table(packet, acks, ++level);
			break;
		}

		TRACE(4, "CFG::receiveRequestsLoop: Polling Request socket for new requests");
		auto nfds = epoll_wait(ack_epoll_fd_, &receive_ack_events_[0], receive_ack_events_.size(), table_ack_wait_time_ms_);
		if (nfds == -1)
		{
			mf::LogError("RoutingMasterCore") << "Error in epoll_wait, aborting";
			exit(3);
		}

		for (auto n = 0; n < nfds; ++n)
		{
			detail::RoutingAckPacket buffer;
			recv(receive_ack_events_[n].data.fd, &buffer, sizeof(detail::RoutingAckPacket), 0);

			if (buffer.first_sequence_id == first && buffer.last_sequence_id == last)
			{
				mf::LogDebug("RoutingMasterCore") << "Received table update acknowledgement from BoardReader with rank " << std::to_string(buffer.rank) << ".";
				acks[buffer.rank - br_ranks_[0]] = true;
			}
		}
	}
}

void artdaq::RoutingMasterCore::receive_tokens_()
{
	while (!(stop_requested_ || pause_requested_)) {
		if (token_socket_ == -1)
		{
			mf::LogDebug("RoutingMasterCore") << "Opening token listener socket";
			token_socket_ = TCP_listen_fd(receive_token_port_, 3 * sizeof(detail::RoutingToken));

			if (token_epoll_fd_ != -1) close(token_epoll_fd_);
			struct epoll_event ev;
			token_epoll_fd_ = epoll_create1(0);
			ev.events = EPOLLIN | EPOLLPRI;
			ev.data.fd = token_socket_;
			if (epoll_ctl(token_epoll_fd_, EPOLL_CTL_ADD, token_socket_, &ev) == -1)
			{
				mf::LogError("RoutingMasterCore") << "Could not register listen socket to epoll fd";
				exit(3);
			}
		}
		if (token_socket_ == -1 || token_epoll_fd_ == -1)
		{
			mf::LogDebug("RoutingMasterCore") << "One of the listen sockets was not opened successfully.";
			return;
		}

		auto nfds = epoll_wait(token_epoll_fd_, &receive_token_events_[0], receive_token_events_.size(), -1);
		if (nfds == -1) {
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		for (auto n = 0; n < nfds; ++n) {
			if (receive_token_events_[n].data.fd == token_socket_) {
				sockaddr_in addr;
				socklen_t arglen = sizeof(addr);
				auto conn_sock = accept(token_socket_, (struct sockaddr *)&addr, &arglen);

				if (conn_sock == -1) {
					perror("accept");
					exit(EXIT_FAILURE);
				}

				receive_token_addrs_[conn_sock] = std::string(inet_ntoa(addr.sin_addr));
				struct epoll_event ev;
				ev.events = EPOLLIN | EPOLLET;
				ev.data.fd = conn_sock;
				if (epoll_ctl(token_epoll_fd_, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
					perror("epoll_ctl: conn_sock");
					exit(EXIT_FAILURE);
				}
			}
			else {
				auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
				detail::RoutingToken buff;
				auto sts = read(receive_token_events_[n].data.fd, &buff, sizeof(detail::RoutingToken));
				if (sts != sizeof(detail::RoutingToken) || buff.header != TOKEN_MAGIC)
				{
					mf::LogError("RoutingMasterCore") << "Received invalid token from " << receive_token_addrs_[receive_token_events_[n].data.fd];
				}
				else
				{
					policy_->AddEventBuilderToken(buff.rank, buff.new_slots_free, buff.minimum_incomplete_event_number);
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
	mf::LogInfo("RoutingMasterCore") << "Starting Token Reception Thread" << std::endl;
	ev_token_receive_thread_ = std::thread(&RoutingMasterCore::receive_tokens_, this);
}

std::string artdaq::RoutingMasterCore::report(std::string const&) const
{
	std::string resultString;

	// if we haven't been able to come up with any report so far, say so
	auto tmpString = name_ + " run number = " + std::to_string(run_id_.run())
		+ ", table updates sent = " + std::to_string(table_update_count_)
		+ ", EventBuilder tokens received = " + std::to_string(received_token_count_);
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
			<< (mqPtr->getRecentValueSum() / br_ranks_.size()) << " sec" << std::endl;
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
		metricMan_.sendMetric("Table Update Count",
							  static_cast<unsigned long>(stats.fullSampleCount),
							  "updates", 1);
		metricMan_.sendMetric("Table Update Rate",
							  stats.recentSampleRate, "updates/sec", 1);

		metricMan_.sendMetric("Average BoardReader Acknowledgement Time",
			(mqPtr->getRecentValueSum() / br_ranks_.size()),
							  "seconds", 3, false);
	}

	mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(TOKENS_RECEIVED_STAT_KEY);
	if (mqPtr.get() != nullptr)
	{
		artdaq::MonitoredQuantityStats stats;
		mqPtr->getStats(stats);
		metricMan_.sendMetric("EventBuilder Token Count",
							  static_cast<unsigned long>(stats.fullSampleCount),
							  "updates", 1);
		metricMan_.sendMetric("EventBuilder Token Rate",
							  stats.recentSampleRate, "updates/sec", 1);
		metricMan_.sendMetric("Total EventBuilder Token Wait Time",
							  mqPtr->getRecentValueSum(),
							  "seconds", 3, false);
	}
}
