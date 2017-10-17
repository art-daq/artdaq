#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq/DAQrate/RequestSender.hh"
#include <utility>
#include <cstring>
#include <dlfcn.h>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>

#include "cetlib_except/exception.h"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/DAQdata/TCPConnect.hh"

namespace artdaq
{

	RequestSender::RequestSender(const fhicl::ParameterSet& pset)
		: send_requests_(pset.get<bool>("send_requests", false))
		, active_requests_()
		, request_port_(pset.get<int>("request_port", 3001))
		, request_delay_(pset.get<size_t>("request_delay_ms", 10) * 1000)
		, request_shutdown_timeout_us_(pset.get<size_t>("request_shutdown_timeout_us", 100000))
		, multicast_out_addr_(pset.get<std::string>("output_address", "0.0.0.0"))
		, request_mode_(detail::RequestMessageMode::Normal)
		, token_socket_(-1)
	{
		TLOG_DEBUG("RequestSender") << "RequestSender CONSTRUCTOR" << TLOG_ENDL;
		setup_requests_(pset.get<std::string>("request_address", "227.128.12.26"));

		auto rmConfig = pset.get<fhicl::ParameterSet>("routing_token_config", fhicl::ParameterSet());
		send_routing_tokens_ = rmConfig.get<bool>("use_routing_master", false);
		token_port_ = rmConfig.get<int>("routing_token_port", 35555);
		token_address_ = rmConfig.get<std::string>("routing_master_hostname", "localhost");
		setup_tokens_();
		TRACE(12, "artdaq::RequestSender::RequestSender ctor - reader_thread_ initialized");
	}


	RequestSender::~RequestSender()
	{
		TLOG_TRACE("RequestSender") << "Shutting down RequestSender: Waiting for requests to be sent" << TLOG_ENDL;

		auto start_time = std::chrono::steady_clock::now();

		while (request_sending_ && request_shutdown_timeout_us_ > static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_time).count()))
		{
			usleep(1000);
		}
		TLOG_TRACE("RequestSender") << "Shutting down RequestSender" << TLOG_ENDL;
		if (request_socket_)
		{
			shutdown(request_socket_, 2);
			close(request_socket_);
		}
		if (token_socket_)
		{
			shutdown(token_socket_, 2);
			close(token_socket_);
		}
	}


	void RequestSender::SetRequestMode(detail::RequestMessageMode mode)
	{
		request_mode_ = mode;
		SendRequest(true);
	}

	void
		RequestSender::setup_requests_(std::string request_address)
	{
		if (send_requests_)
		{
			request_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
			if (!request_socket_)
			{
				TLOG_ERROR("RequestSender") << "I failed to create the socket for sending Data Requests!" << TLOG_ENDL;
				exit(1);
			}
			int sts = ResolveHost(request_address.c_str(), request_port_, request_addr_);
			if (sts == -1)
			{
				TLOG_ERROR("RequestSender") << "Unable to resolve Data Request address" << TLOG_ENDL;
				exit(1);
			}

			if (multicast_out_addr_ == "0.0.0.0")
			{
				multicast_out_addr_.reserve(HOST_NAME_MAX);
				sts = gethostname(&multicast_out_addr_[0], HOST_NAME_MAX);
				if (sts < 0)
				{
					TLOG_ERROR("RequestSender") << "Could not get current hostname" << TLOG_ENDL;
					exit(1);
				}
			}

			if (multicast_out_addr_ != "localhost")
			{
				struct in_addr addr;
				sts = ResolveHost(multicast_out_addr_.c_str(), addr);
				if (sts == -1)
				{
					TLOG_ERROR("RequestSender") << "Unable to resolve multicast interface address" << TLOG_ENDL;
					exit(1);
				}

				int yes = 1;
				if (setsockopt(request_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
				{
					TLOG_ERROR("RequestSender") << "Unable to enable port reuse on request socket" << TLOG_ENDL;
					exit(1);
				}
				if (setsockopt(request_socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &yes, sizeof(yes)) < 0)
				{
					TLOG_ERROR("RequestSender") << "Unable to enable multicast loopback on request socket" << TLOG_ENDL;
					exit(1);
				}
				if (setsockopt(request_socket_, IPPROTO_IP, IP_MULTICAST_IF, &addr, sizeof(addr)) == -1)
				{
					TLOG_ERROR("RequestSender") << "Cannot set outgoing interface." << TLOG_ENDL;
					exit(1);
				}
			}
			int yes = 1;
			if (setsockopt(request_socket_, SOL_SOCKET, SO_BROADCAST, (void*)&yes, sizeof(int)) == -1)
			{
				TLOG_ERROR("RequestSender") << "Cannot set request socket to broadcast." << TLOG_ENDL;
				exit(1);
			}
		}
	}

	void
		RequestSender::setup_tokens_()
	{
		if (send_routing_tokens_)
		{
			TLOG_DEBUG("RequestSender") << "Creating Routing Token sending socket" << TLOG_ENDL;
			token_socket_ = TCPConnect(token_address_.c_str(), token_port_);
			if (!token_socket_)
			{
				TLOG_ERROR("RequestSender") << "I failed to create the socket for sending Routing Tokens!" << TLOG_ENDL;
				exit(1);
			}
		}
	}

	void RequestSender::do_send_request_()
	{
		if (!send_requests_) return;
		TLOG_TRACE("RequestSender") << "Waiting for " << request_delay_ << " microseconds." << TLOG_ENDL;
		std::this_thread::sleep_for(std::chrono::microseconds(request_delay_));

		TLOG_TRACE("RequestSender") << "Creating RequestMessage" << TLOG_ENDL;
		detail::RequestMessage message;
		{
			std::lock_guard<std::mutex> lk(request_mutex_);
			for (auto& req : active_requests_)
			{
				message.addRequest(req.first, req.second);
			}
		}
		TLOG_TRACE("RequestSender") << "Setting mode flag in Message Header" << TLOG_ENDL;
		message.header()->mode = request_mode_;
		char str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(request_addr_.sin_addr), str, INET_ADDRSTRLEN);
		std::lock_guard<std::mutex> lk2(request_send_mutex_);
		TLOG_TRACE("RequestSender") << "Sending request for " << std::to_string(message.size()) << " events to multicast group " << str << TLOG_ENDL;
		if (sendto(request_socket_, message.header(), sizeof(detail::RequestHeader), 0, (struct sockaddr *)&request_addr_, sizeof(request_addr_)) < 0)
		{
			TLOG_ERROR("RequestSender") << "Error sending request message header" << TLOG_ENDL;
		}
		if (sendto(request_socket_, message.buffer(), sizeof(detail::RequestPacket) * message.size(), 0, (struct sockaddr *)&request_addr_, sizeof(request_addr_)) < 0)
		{
			TLOG_ERROR("RequestSender") << "Error sending request message data" << TLOG_ENDL;
		}
		request_sending_ = false;
	}

	void RequestSender::send_routing_token_(int nSlots)
	{
		TLOG_TRACE("RequestSender") << "send_routing_token_ called, send_routing_tokens_=" << std::boolalpha << send_routing_tokens_ << TLOG_ENDL;
		if (!send_routing_tokens_) return;
		if (token_socket_ == -1) setup_tokens_();
		detail::RoutingToken token;
		token.header = TOKEN_MAGIC;
		token.rank = my_rank;
		token.new_slots_free = nSlots;

		TLOG_TRACE("RequestSender") << "Sending RoutingToken to " << token_address_ << ":" << token_port_ << TLOG_ENDL;
		size_t sts = 0;
		while (sts < sizeof(detail::RoutingToken))
		{
			auto res = send(token_socket_, reinterpret_cast<uint8_t*>(&token) + sts, sizeof(detail::RoutingToken) - sts, 0);
			if (res == -1)
			{
				usleep(1000);
				continue;
			}
			sts += res;
		}
		TLOG_TRACE("RequestSender") << "Done sending RoutingToken to " << token_address_ << ":" << token_port_ << TLOG_ENDL;
	}

	void RequestSender::SendRoutingToken(int nSlots)
	{
		if (!send_routing_tokens_) return;
		std::thread token([=] {send_routing_token_(nSlots); });
		token.detach();
		usleep(0); // Give up time slice
	}

	void RequestSender::SendRequest(bool endOfRunOnly)
	{
		if (!send_requests_) return;
		if (endOfRunOnly && request_mode_ != detail::RequestMessageMode::EndOfRun) return;
		request_sending_ = true;
		std::thread request([=] { do_send_request_(); });
		request.detach();
	}

	void RequestSender::AddRequest(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp)
	{
		{
			std::lock_guard<std::mutex> lk(request_mutex_);
			if (!active_requests_.count(seqID)) active_requests_[seqID] = timestamp;
		}
		SendRequest();
	}

	void RequestSender::RemoveRequest(Fragment::sequence_id_t seqID)
	{
		std::lock_guard<std::mutex> lk(request_mutex_);
		active_requests_.erase(seqID);
	}
}