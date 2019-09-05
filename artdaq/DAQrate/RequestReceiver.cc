#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RequestReceiver").c_str()

#include "artdaq/DAQrate/RequestReceiver.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"

#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include <iterator>
#include <limits>

#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Utilities/SimpleLookupPolicy.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/poll.h>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include "artdaq/DAQdata/TCPConnect.hh"

artdaq::RequestReceiver::RequestReceiver()
    : request_port_(3001)
    , request_addr_("227.128.12.26")
    , multicast_out_addr_("0.0.0.0")
    , ack_port_(3002)
    , ack_address_("localhost")
    , running_(false)
    , run_number_(0)
    , request_socket_(-1)
    , ack_socket_(-1)
    , requests_()
    , request_stop_requested_(false)
    , request_received_(false)
    , end_of_run_timeout_ms_(1000)
    , should_stop_(false)
    , highest_seen_request_(0)
    , out_of_order_requests_()
    , request_increment_(1)
{}

artdaq::RequestReceiver::RequestReceiver(const fhicl::ParameterSet& ps)
    : request_port_(ps.get<int>("request_port", 3001))
    , request_addr_(ps.get<std::string>("request_address", "227.128.12.26"))
    , multicast_out_addr_(ps.get<std::string>("multicast_interface_ip", "0.0.0.0"))
    , ack_port_(ps.get<int>("acknowledgement_port", 3002))
    , ack_address_(ps.get<std::string>("acknowledgement_address", "localhost"))
    , running_(false)
    , run_number_(0)
    , request_socket_(-1)
    , ack_socket_(-1)
    , requests_()
    , request_stop_requested_(false)
    , request_received_(false)
    , end_of_run_timeout_ms_(ps.get<size_t>("end_of_run_quiet_timeout_ms", 1000))
    , should_stop_(false)
    , highest_seen_request_(0)
    , out_of_order_requests_()
    , request_increment_(ps.get<artdaq::Fragment::sequence_id_t>("request_increment", 1))
{
	setupRequestListener();
}

void artdaq::RequestReceiver::setupRequestListener()
{
	TLOG(TLVL_INFO) << "Setting up request listen socket, rank=" << my_rank << ", address=" << request_addr_ << ":" << request_port_
	                << ", multicast interface=" << multicast_in_addr_;
	request_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (request_socket_ < 0)
	{
		TLOG(TLVL_ERROR) << "Error creating socket for receiving data requests! err=" << strerror(errno);
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(request_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		TLOG(TLVL_ERROR) << "Unable to enable port reuse on request socket, err=" << strerror(errno);
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(request_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(request_socket_, (struct sockaddr*)&si_me_request, sizeof(si_me_request)) == -1)
	{
		TLOG(TLVL_ERROR) << "Cannot bind request socket to port " << request_port_ << ", err=" << strerror(errno);
		exit(1);
	}

	if (request_addr_ != "localhost")
	{
		struct ip_mreq mreq;
		int sts = ResolveHost(request_addr_.c_str(), mreq.imr_multiaddr);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Unable to resolve multicast request address, err=" << strerror(errno);
			exit(1);
		}
		sts = GetInterfaceForNetwork(multicast_in_addr_.c_str(), mreq.imr_interface);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Unable to resolve hostname for " << multicast_in_addr_;
			exit(1);
		}
		if (setsockopt(request_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
		{
			TLOG(TLVL_ERROR) << "Unable to join multicast group, err=" << strerror(errno);
			exit(1);
		}
	}
	TLOG(TLVL_INFO) << "Done setting up request socket, rank=" << my_rank;
}

artdaq::RequestReceiver::~RequestReceiver()
{
	stopRequestReception(true);
}

void artdaq::RequestReceiver::stopRequestReception(bool force)
{
	std::unique_lock<std::mutex> lk(state_mutex_);
	if (!request_received_ && !force)
	{
		TLOG(TLVL_ERROR) << "Stop request received by RequestReceiver, but no requests have ever been received." << std::endl
		                 << "Check that UDP port " << request_port_ << " is open in the firewall config.";
	}
	should_stop_ = true;
	if (running_)
	{
		TLOG(TLVL_DEBUG) << "Joining requestThread";
		if (requestThread_.joinable()) requestThread_.join();
		bool once = true;
		while (running_)
		{
			if (once) TLOG(TLVL_ERROR) << "running_ is true after thread join! Should NOT happen";
			once = false;
			usleep(10000);
		}
	}

	if (request_socket_ != -1)
	{
		close(request_socket_);
		request_socket_ = -1;
	}
	if (ack_socket_ != -1)
	{
		close(ack_socket_);
		ack_socket_ = -1;
	}
	request_received_ = false;
	highest_seen_request_ = 0;
}

void artdaq::RequestReceiver::startRequestReception()
{
	std::unique_lock<std::mutex> lk(state_mutex_);
	if (requestThread_.joinable()) requestThread_.join();
	should_stop_ = false;
	request_stop_requested_ = false;

	if (request_socket_ == -1)
	{
		TLOG(TLVL_INFO) << "Connecting Request Reception socket";
		setupRequestListener();
	}

	TLOG(TLVL_INFO) << "Starting Request Reception Thread";
	try
	{
		requestThread_ = boost::thread(&RequestReceiver::receiveRequestsLoop, this);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Request Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Request Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
	running_ = true;
}

void artdaq::RequestReceiver::receiveRequestsLoop()
{
	while (!should_stop_)
	{
		TLOG(16) << "receiveRequestsLoop: Polling Request socket for new requests";

		if (request_socket_ == -1)
		{
			setupRequestListener();
		}

		int ms_to_wait = 10;
		struct pollfd ufds[1];
		ufds[0].fd = request_socket_;
		ufds[0].events = POLLIN | POLLPRI | POLLERR;
		int rv = poll(ufds, 1, ms_to_wait);

		// Continue loop if no message received or message does not have correct event ID
		if (rv <= 0 || (ufds[0].revents != POLLIN && ufds[0].revents != POLLPRI))
		{
			if (rv == 1 && (ufds[0].revents & (POLLNVAL | POLLERR | POLLHUP)))
			{
				close(request_socket_);
				request_socket_ = -1;
			}
			if (request_stop_requested_ && TimeUtils::GetElapsedTimeMilliseconds(request_stop_timeout_) > end_of_run_timeout_ms_)
			{
				break;
			}
			continue;
		}

		TLOG(11) << "Received packet on Request channel";
		std::vector<uint8_t> buffer(MAX_REQUEST_MESSAGE_SIZE);
		struct sockaddr_in from;
		socklen_t len = sizeof(from);
		auto sts = recvfrom(request_socket_, &buffer[0], MAX_REQUEST_MESSAGE_SIZE, 0, (struct sockaddr*)&from, &len);
		if (sts < 0)
		{
			TLOG(TLVL_ERROR) << "Error receiving request message header err=" << strerror(errno);
			close(request_socket_);
			request_socket_ = -1;
			continue;
		}

		auto message = artdaq::detail::RequestMessage(&buffer[0], sts);
		TLOG(11) << "Received RequestMessage from " << inet_ntoa(from.sin_addr) << ":" << from.sin_port;

		if (!message.isValid()) continue;

		request_received_ = true;

		// 19-Dec-2018, KAB: added check on current run number
		if (run_number_ != 0 && message.getRunNumber() != run_number_)
		{
			TLOG(TLVL_WARNING) << "Received a Request Message with the wrong run number ("
			                   << message.getRunNumber() << "), expected " << run_number_
			                   << ", ignoring this request.";
			continue;
		}

		if (message.getMode() == artdaq::detail::RequestMessageMode::EndOfRun)
		{
			TLOG(TLVL_INFO) << "Received Request Message with the EndOfRun marker. (Re)Starting 1-second timeout for receiving all outstanding requests...";
			request_stop_timeout_ = std::chrono::steady_clock::now();
			request_stop_requested_ = true;
		}

		bool anyNew = false;

		if (should_stop_) break;

		for (auto& buffer : message.getRequests())
		{
			TLOG(20) << "Request Packet: hdr=0x" << std::hex << buffer.header << std::dec << ", seq=" << buffer.sequence_id << ", ts=" << buffer.timestamp;
			if (!buffer.isValid()) continue;
			std::unique_lock<std::mutex> tlk(request_mutex_);
			if (requests_.count(buffer.sequence_id) && requests_[buffer.sequence_id].first.timestamp != buffer.timestamp)
			{
				TLOG(TLVL_ERROR) << "Received conflicting request for SeqID "
				                 << buffer.sequence_id << "!"
				                 << " Old ts=" << requests_[buffer.sequence_id].first.timestamp
				                 << ", new ts=" << buffer.timestamp << ". Keeping OLD!";
			}
			else if (!requests_.count(buffer.sequence_id) && (!message.getAcknowledge() || buffer.hasRank(my_rank)))
			{
				int delta = buffer.sequence_id - highest_seen_request_;
				TLOG(11) << "Received request for sequence ID " << buffer.sequence_id
				         << " and timestamp " << buffer.timestamp << " (delta: " << delta << ")";
				if (delta <= 0 || out_of_order_requests_.count(buffer.sequence_id))
				{
					TLOG(11) << "Already serviced this request ( sequence ID " << buffer.sequence_id << ")! Ignoring...";
					if (message.getAcknowledge() && buffer.hasRank(my_rank)) anyNew = true;
				}
				else
				{
					requests_[buffer.sequence_id] = std::make_pair(buffer, std::chrono::steady_clock::now());
					anyNew = true;
				}
			}
		}
		if (anyNew)
		{
			// If we have new requests, acknowledge the whole message
			if (message.getAcknowledge())
			{
				sendAcknowledgement(message);
			}
			request_cv_.notify_all();
		}
	}
	TLOG(TLVL_DEBUG) << "Ending Request Thread";
	running_ = false;
}

void artdaq::RequestReceiver::sendAcknowledgement(detail::RequestMessage message)
{
	if (ack_socket_ == -1)
	{
		ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		auto sts = ResolveHost(ack_address_.c_str(), ack_port_, ack_addr_);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << __func__ << ": Unable to resolve acknowledge_address";
			exit(1);
		}
		TLOG(TLVL_DEBUG) << __func__ << ": Ack socket is fd " << ack_socket_;
	}
	auto reqs = message.getRequests();
	detail::RequestAcknowledgement ack(message.size(), message.getRunNumber());

	std::vector<uint8_t> buffer(sizeof(ack) + message.size() * sizeof(Fragment::sequence_id_t));
	assert(buffer.size() < MAX_REQUEST_MESSAGE_SIZE);
	memcpy(&buffer[0], &ack, sizeof(ack));

	size_t offset = sizeof(ack);
	for (auto& req : reqs)
	{
		memcpy(&(buffer[offset]), &req.sequence_id, sizeof(Fragment::sequence_id_t));
		offset += sizeof(Fragment::sequence_id_t);
	}

	TLOG(TLVL_DEBUG) << __func__ << ": Sending RequestAcknowledgement with " << ack.packet_count << " entries to " << ack_address_ << ", port " << ack_port_ << " (my_rank = " << my_rank << ")";
	sendto(ack_socket_, &buffer[0], buffer.size(), 0, (struct sockaddr*)&ack_addr_, sizeof(ack_addr_));
}

void artdaq::RequestReceiver::RemoveRequest(artdaq::Fragment::sequence_id_t reqID)
{
	TLOG(10) << "RemoveRequest: Removing request for id " << reqID;
	std::unique_lock<std::mutex> lk(request_mutex_);

	if (metricMan)
	{
		metricMan->sendMetric("Request Response Time", TimeUtils::GetElapsedTime(requests_[reqID].second), "seconds", 2, MetricMode::Average);
	}
	requests_.erase(reqID);
	if (reqID > highest_seen_request_)
	{
		TLOG(10) << "RemoveRequest: out_of_order_requests_.size() == " << out_of_order_requests_.size() << ", reqID=" << reqID << ", expected=" << highest_seen_request_ + request_increment_;
		if (out_of_order_requests_.size() || reqID != highest_seen_request_ + request_increment_)
		{
			out_of_order_requests_.insert(reqID);

			auto it = out_of_order_requests_.begin();
			while (it != out_of_order_requests_.end() && !should_stop_)  // Stop accounting for requests after stop
			{
				if (*it == highest_seen_request_ + request_increment_)
				{
					highest_seen_request_ = *it;
					it = out_of_order_requests_.erase(it);
				}
				else
				{
					break;
				}
			}
		}
		else  // no out-of-order requests and this request is highest seen + request_increment_
		{
			highest_seen_request_ = reqID;
		}
		TLOG(10) << "RemoveRequest: reqID=" << reqID << " Set highest_seen_request_ to " << highest_seen_request_;
	}
}
