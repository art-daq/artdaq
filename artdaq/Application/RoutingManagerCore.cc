#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <sched.h>
#include <sys/time.h>
#include <sys/un.h>
#include <algorithm>
#include <memory>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"

#include "artdaq/DAQdata/Globals.hh"                           // include these 2 first to get tracemf.h -
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
const std::string artdaq::RoutingManagerCore::
    CURRENT_TABLE_INTERVAL_STAT_KEY("RoutingManagerCoreCurrentTableInterval");

artdaq::RoutingManagerCore::RoutingManagerCore()
    : shutdown_requested_(false)
    , stop_requested_(true)
    , pause_requested_(false)
    , statsHelperPtr_(new artdaq::StatisticsHelper())
{
	TLOG(TLVL_DEBUG + 32) << "Constructor";
	statsHelperPtr_->addMonitoredQuantityName(TABLE_UPDATES_STAT_KEY);
	statsHelperPtr_->addMonitoredQuantityName(TOKENS_RECEIVED_STAT_KEY);
	statsHelperPtr_->addMonitoredQuantityName(CURRENT_TABLE_INTERVAL_STAT_KEY);
}

artdaq::RoutingManagerCore::~RoutingManagerCore()
{
	TLOG(TLVL_DEBUG + 32) << "Destructor";
	artdaq::StatisticsCollection::getInstance().requestStop();
	token_receiver_->stopTokenReception(true);
}

bool artdaq::RoutingManagerCore::initialize(fhicl::ParameterSet const& pset, uint64_t /*unused*/, uint64_t /*unused*/)
{
	TLOG(TLVL_DEBUG + 32) << "initialize method called with "
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

		TLOG(TLVL_DEBUG + 32) << "FHiCL parameter set used to initialize the policy which threw an exception: " << policy_pset_.to_string();

		return false;
	}

	rt_priority_ = daq_pset.get<int>("rt_priority", 0);
	max_table_update_interval_ms_ = daq_pset.get<size_t>("table_update_interval_ms", 1000);
	current_table_interval_ms_ = max_table_update_interval_ms_;
	table_update_high_fraction_ = daq_pset.get<double>("table_update_interval_high_frac", 0.75);
	table_update_low_fraction_ = daq_pset.get<double>("table_update_interval_low_frac", 0.5);

	// fetch the monitoring parameters and create the MonitoredQuantity instances
	statsHelperPtr_->createCollectors(daq_pset, 100, 30.0, 60.0, TABLE_UPDATES_STAT_KEY);

	// create the requested TokenReceiver
	token_receiver_ = std::make_unique<TokenReceiver>(token_receiver_pset_, policy_, max_table_update_interval_ms_);
	token_receiver_->setStatsHelper(statsHelperPtr_, TOKENS_RECEIVED_STAT_KEY);
	token_receiver_->startTokenReception();
	token_receiver_->pauseTokenReception();

	table_listen_port_ = daq_pset.get<int>("table_update_port", 35556);

	shutdown_requested_.store(true);
	if (listen_thread_ && listen_thread_->joinable())
	{
		listen_thread_->join();
	}
	shutdown_requested_.store(false);
	TLOG(TLVL_INFO) << "Starting Listener Thread";

	try
	{
		listen_thread_ = std::make_unique<boost::thread>(&RoutingManagerCore::listen_, this);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting TCP Socket Listen thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting TCP Socket Listen thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
	return true;
}

bool artdaq::RoutingManagerCore::start(art::RunID id, uint64_t /*unused*/, uint64_t /*unused*/)
{
	run_id_ = id;
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
	TLOG(TLVL_DEBUG + 32) << "Resuming run " << run_id_.run();
	pause_requested_.store(false);
	metricMan->do_start();
	return true;
}

bool artdaq::RoutingManagerCore::shutdown(uint64_t /*unused*/)
{
	shutdown_requested_.store(true);
	if (listen_thread_ && listen_thread_->joinable())
	{
		listen_thread_->join();
	}
	token_receiver_->stopTokenReception();
	policy_.reset();
	metricMan->shutdown();
	return true;
}

bool artdaq::RoutingManagerCore::soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_INFO) << "soft_initialize method called with "
	                << "ParameterSet = \"" << pset.to_string()
	                << "\".";
	return initialize(pset, timeout, timestamp);
}

bool artdaq::RoutingManagerCore::reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	TLOG(TLVL_INFO) << "reinitialize method called with "
	                << "ParameterSet = \"" << pset.to_string()
	                << "\".";
	return initialize(pset, timeout, timestamp);
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

	TLOG(TLVL_DEBUG + 32) << "Sending initial table.";
	auto startTime = artdaq::MonitoredQuantity::getCurrentTime();
	auto nextSendTime = startTime;
	double delta_time;
	while (!stop_requested_ && !pause_requested_)
	{
		receive_();
		if (policy_->GetRoutingMode() == detail::RoutingManagerMode::EventBuilding || policy_->GetRoutingMode() == detail::RoutingManagerMode::RequestBasedEventBuilding)
		{
			startTime = artdaq::MonitoredQuantity::getCurrentTime();

			if (startTime >= nextSendTime)
			{
				auto table = policy_->GetCurrentTable();

				if (table.empty())
				{
					TLOG(TLVL_WARNING) << "Routing Policy generated Empty table for this routing interval (" << current_table_interval_ms_ << " ms)! This may indicate issues with the receivers, if it persists."
					                   << " Next seqID=" << policy_->GetNextSequenceID() << ", Policy held tokens=" << policy_->GetHeldTokenCount();
				}
				else
				{
					send_event_table(table);
					++table_update_count_;
					delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
					statsHelperPtr_->addSample(TABLE_UPDATES_STAT_KEY, delta_time);
					TLOG(TLVL_DEBUG + 34) << "process_fragments TABLE_UPDATES_STAT_KEY=" << delta_time;

					bool readyToReport = statsHelperPtr_->readyToReport();
					if (readyToReport)
					{
						std::string statString = buildStatisticsString_();
						TLOG(TLVL_INFO) << statString;
						sendMetrics_();
					}
				}

				auto max_tokens = policy_->GetMaxNumberOfTokens();
				if (max_tokens > 0)
				{
					auto frac = policy_->GetTokensUsedSinceLastUpdate() / static_cast<double>(max_tokens);
					policy_->ResetTokensUsedSinceLastUpdate();
					if (frac > table_update_high_fraction_) current_table_interval_ms_ = 9 * current_table_interval_ms_ / 10;
					if (frac < table_update_low_fraction_) current_table_interval_ms_ = 11 * current_table_interval_ms_ / 10;
					if (current_table_interval_ms_ > max_table_update_interval_ms_) current_table_interval_ms_ = max_table_update_interval_ms_;
					if (current_table_interval_ms_ < 1) current_table_interval_ms_ = 1;
				}
				nextSendTime = startTime + current_table_interval_ms_ / 1000.0;
				TLOG(TLVL_DEBUG + 32) << "current_table_interval_ms is now " << current_table_interval_ms_;
				statsHelperPtr_->addSample(CURRENT_TABLE_INTERVAL_STAT_KEY, current_table_interval_ms_ / 1000.0);
			}
			else
			{
				usleep(current_table_interval_ms_ * 10);  // 1/100 of the table update interval
			}
		}
	}

	TLOG(TLVL_DEBUG + 32) << "stop_requested_ is " << stop_requested_ << ", pause_requested_ is " << pause_requested_ << ", exiting process_event_table loop";
	policy_->Reset();
	metricMan->do_stop();
}

void artdaq::RoutingManagerCore::send_event_table(detail::RoutingPacket packet)
{
	std::lock_guard<std::mutex> lk(fd_mutex_);
	for (auto& dest : connected_fds_)
	{
		for (auto& connected_fd : dest.second)
		{
			auto header = detail::RoutingPacketHeader(packet.size());
			TLOG(TLVL_DEBUG + 32) << "Sending table information for " << header.nEntries << " events to destination " << dest.first;
			TRACE(16, "headerData:0x%016lx%016lx packetData:0x%016lx%016lx", ((unsigned long*)&header)[0], ((unsigned long*)&header)[1], ((unsigned long*)&packet[0])[0], ((unsigned long*)&packet[0])[1]);  // NOLINT
			auto sts = write(connected_fd, &header, sizeof(header));
			if (sts != sizeof(header))
			{
				TLOG(TLVL_ERROR) << "Error sending routing header to fd " << connected_fd << ", rank " << dest.first;
			}
			else
			{
				sts = write(connected_fd, &packet[0], packet.size() * sizeof(detail::RoutingPacketEntry));
				if (sts != static_cast<ssize_t>(packet.size() * sizeof(detail::RoutingPacketEntry)))
				{
					TLOG(TLVL_ERROR) << "Error sending routing table. sts=" << sts << "/" << packet.size() * sizeof(detail::RoutingPacketEntry) << ", fd=" << connected_fd << ", rank=" << dest.first;
				}
			}
		}
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

		mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(CURRENT_TABLE_INTERVAL_STAT_KEY);
		if (mqPtr.get() != nullptr)
		{
			artdaq::MonitoredQuantityStats stats;
			mqPtr->getStats(stats);
			metricMan->sendMetric("Table Update Interval", stats.recentValueAverage, "s", 3, MetricMode::Average);
		}
	}
}

void artdaq::RoutingManagerCore::listen_()
{
	if (epoll_fd_ == -1)
	{
		epoll_fd_ = epoll_create1(0);
	}
	int listen_fd = -1;
	while (shutdown_requested_ == false)
	{
		TLOG(TLVL_DEBUG + 33) << "listen_: Listening/accepting new connections on port " << table_listen_port_;
		if (listen_fd == -1)
		{
			TLOG(TLVL_DEBUG + 32) << "listen_: Opening listener";
			listen_fd = TCP_listen_fd(table_listen_port_, 0);
		}
		if (listen_fd == -1)
		{
			TLOG(TLVL_DEBUG + 32) << "listen_: Error creating listen_fd!";
			break;
		}

		int res;
		timeval tv = {2, 0};  // maybe increase of some global "debugging" flag set???
		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(listen_fd, &rfds);  // NOLINT

		res = select(listen_fd + 1, &rfds, static_cast<fd_set*>(nullptr), static_cast<fd_set*>(nullptr), &tv);
		if (res > 0)
		{
			int sts;
			sockaddr_un un;
			socklen_t arglen = sizeof(un);
			int fd;
			TLOG(TLVL_DEBUG + 32) << "listen_: Calling accept";
			fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&un), &arglen);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
			TLOG(TLVL_DEBUG + 32) << "listen_: Done with accept";

			TLOG(TLVL_DEBUG + 32) << "listen_: Reading connect message";
			socklen_t lenlen = sizeof(tv);
			/*sts=*/
			setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, lenlen);  // see man 7 socket.
			detail::RoutingRequest rch;
			uint64_t mark_us = TimeUtils::gettimeofday_us();
			sts = read(fd, &rch, sizeof(rch));
			uint64_t delta_us = TimeUtils::gettimeofday_us() - mark_us;
			TLOG(TLVL_DEBUG + 32) << "listen_: Read of connect message took " << delta_us << " microseconds.";
			if (sts != sizeof(rch))
			{
				TLOG(TLVL_DEBUG + 32) << "listen_: Wrong message header length received!";
				close(fd);
				continue;
			}

			// check for "magic" and valid source_id(aka rank)
			if (rch.header != ROUTING_MAGIC || !(rch.mode == detail::RoutingRequest::RequestMode::Connect))
			{
				TLOG(TLVL_DEBUG + 32) << "listen_: Wrong magic bytes in header! rch.header: " << std::hex << rch.header;
				close(fd);
				continue;
			}

			// now add (new) connection
			std::lock_guard<std::mutex> lk(fd_mutex_);
			connected_fds_[rch.rank].insert(fd);
			struct epoll_event ev;
			ev.data.fd = fd;
			ev.events = EPOLLIN;
			epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
			TLOG(TLVL_INFO) << "listen_: New fd is " << fd << " for table receiver rank " << rch.rank;
		}
		else
		{
			TLOG(TLVL_DEBUG + 34) << "listen_: No connections in timeout interval!";
		}
	}

	TLOG(TLVL_INFO) << "listen_: Shutting down connection listener";
	if (listen_fd != -1)
	{
		close(listen_fd);
	}
	std::lock_guard<std::mutex> lk(fd_mutex_);
	for (auto& fd_set : connected_fds_)
	{
		for (auto& fd : fd_set.second)
		{
			epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
			close(fd);
		}
	}
	connected_fds_.clear();

}  // listen_

void artdaq::RoutingManagerCore::receive_()
{
	if (epoll_fd_ == -1)
	{
		epoll_fd_ = epoll_create1(0);
	}
	std::vector<epoll_event> received_events(10);

	int nfds = 1;
	while (nfds > 0)
	{
		std::lock_guard<std::mutex> lk(fd_mutex_);
		nfds = epoll_wait(epoll_fd_, &received_events[0], received_events.size(), 1);
		if (nfds == -1)
		{
			TLOG(TLVL_ERROR) << "Error status received from epoll_wait, exiting with code " << EXIT_FAILURE << ", errno=" << errno << " (" << strerror(errno) << ")";
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		if (nfds > 0)
		{
			TLOG(TLVL_DEBUG + 35) << "Received " << nfds << " events on table sockets";
		}
		for (auto n = 0; n < nfds; ++n)
		{
			bool reading = true;
			int sts = 0;
			while (reading)
			{
				if ((received_events[n].events & EPOLLIN) != 0)
				{
					detail::RoutingRequest buff;
					auto stss = read(received_events[n].data.fd, &buff, sizeof(detail::RoutingRequest) - sts);
					sts += stss;
					if (stss == 0)
					{
						TLOG(TLVL_INFO) << "Received 0-size request from " << find_fd_(received_events[n].data.fd);
						reading = false;
					}
					else if (stss < 0 && errno == EAGAIN)
					{
						TLOG(TLVL_DEBUG + 32) << "No more requests from this rank. Continuing poll loop.";
						reading = false;
					}
					else if (stss < 0)
					{
						TLOG(TLVL_ERROR) << "Error reading from request socket: sts=" << sts << ", errno=" << errno << " (" << strerror(errno) << ")";
						close(received_events[n].data.fd);
						epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, received_events[n].data.fd, nullptr);
						reading = false;
					}
					else if (sts == sizeof(detail::RoutingRequest) && buff.header != ROUTING_MAGIC)
					{
						TLOG(TLVL_ERROR) << "Received invalid request from " << find_fd_(received_events[n].data.fd) << " sts=" << sts << ", header=" << std::hex << buff.header;
						reading = false;
					}
					else if (sts == sizeof(detail::RoutingRequest))
					{
						reading = false;
						sts = 0;
						TLOG(TLVL_DEBUG + 33) << "Received request from " << buff.rank << " mode=" << detail::RoutingRequest::RequestModeToString(buff.mode);
						detail::RoutingPacketEntry reply;

						switch (buff.mode)
						{
							case detail::RoutingRequest::RequestMode::Disconnect:
								connected_fds_[buff.rank].erase(received_events[n].data.fd);
								close(received_events[n].data.fd);
								epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, received_events[n].data.fd, nullptr);
								break;

							case detail::RoutingRequest::RequestMode::Request:
								reply = policy_->GetRouteForSequenceID(buff.sequence_id, buff.rank);
								if (reply.sequence_id == buff.sequence_id)
								{
									TLOG(TLVL_DEBUG + 33) << "Reply to request from " << buff.rank << " with route to " << reply.destination_rank << " for sequence ID " << buff.sequence_id;
									detail::RoutingPacketHeader hdr(1);
									write(received_events[n].data.fd, &hdr, sizeof(hdr));
									write(received_events[n].data.fd, &reply, sizeof(detail::RoutingPacketEntry));
								}
								else
								{
									TLOG(TLVL_DEBUG + 33) << "Unable to route request, replying with empty RoutingPacket";
									detail::RoutingPacketHeader hdr(0);
									write(received_events[n].data.fd, &hdr, sizeof(hdr));
								}
								break;
							default:
								TLOG(TLVL_WARNING) << "Received request from " << buff.rank << " with invalid mode " << detail::RoutingRequest::RequestModeToString(buff.mode) << " (currently only expecting Disconnect or Request)";
								break;
						}
					}
				}
				else
				{
					TLOG(TLVL_DEBUG + 32) << "Received event mask " << received_events[n].events << " from table socket rank " << find_fd_(received_events[n].data.fd);
				}
			}
		}
	}
}

int artdaq::RoutingManagerCore::find_fd_(int fd) const
{
	for (auto& rank : connected_fds_)
	{
		if (rank.second.count(fd) != 0)
		{
			return rank.first;
		}
	}
	return -1;
}
