#include "artdaq/DAQdata/Globals.hh"  // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#define TRACE_NAME (app_name + "_RequestSender").c_str()

#include <dlfcn.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>
#include "artdaq/DAQrate/RequestSender.hh"

#include "RequestSender.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "cetlib_except/exception.h"

namespace artdaq {
RequestSender::RequestSender(const fhicl::ParameterSet& pset)
    : send_requests_(pset.get<bool>("send_requests", false))
    , initialized_(false)
    , active_requests_()
    , request_address_(pset.get<std::string>("request_address", "227.128.12.26"))
    , request_port_(pset.get<int>("request_port", 3001))
    , ack_port_(pset.get<int>("acknowledgement_port", 3002))
    , request_delay_(pset.get<size_t>("request_delay_ms", 0) * 1000)
    , request_shutdown_timeout_us_(pset.get<size_t>("request_shutdown_timeout_us", 100000))
    , request_socket_(-1)
    , ack_socket_(-1)
    , multicast_out_addr_(pset.get<std::string>("multicast_interface_ip", pset.get<std::string>("output_address", "0.0.0.0")))
    , request_mode_(detail::RequestMessageMode::Normal)
    , request_timeout_s_(pset.get<double>("request_timeout_s", -1))
    , token_socket_(-1)
    , request_sending_(0)
    , requests_sent_(0)
    , tokens_sent_(0)
    , run_number_(0)
    , request_acknowledgements_(pset.get<bool>("request_acknowledgements", false))
    , receive_acknowledgements_enabled_(false)
    , ack_thread_running_(false)
    , ack_messages_received_(0)
    , sender_ranks_()
{
	TLOG(TLVL_DEBUG) << "RequestSender CONSTRUCTOR";
	setup_requests_();

	auto rmConfig = pset.get<fhicl::ParameterSet>("routing_token_config", fhicl::ParameterSet());
	send_routing_tokens_ = rmConfig.get<bool>("use_routing_master", false);
	token_port_ = rmConfig.get<int>("routing_token_port", 35555);
	token_address_ = rmConfig.get<std::string>("routing_master_hostname", "localhost");

	if (pset.has_key("sender_ranks"))
	{
		auto ranks = pset.get<std::vector<int>>("sender_ranks");
		for (auto& rank : ranks) sender_ranks_.insert(rank);
	}
	setup_tokens_();
	TLOG(12) << "artdaq::RequestSender::RequestSender ctor - reader_thread_ initialized";
	initialized_ = true;
}

RequestSender::~RequestSender()
{
	TLOG(TLVL_INFO) << "Shutting down RequestSender: Waiting for " << request_sending_.load() << " requests to be sent";

	auto start_time = std::chrono::steady_clock::now();

	while (request_sending_.load() > 0 && request_shutdown_timeout_us_ + request_delay_ > TimeUtils::GetElapsedTimeMicroseconds(start_time))
	{
		usleep(1000);
	}
	{
		std::unique_lock<std::mutex> lk(request_mutex_);
		std::unique_lock<std::mutex> lk2(request_send_mutex_);
	}
	TLOG(TLVL_INFO) << "Shutting down RequestSender: request_socket_: " << request_socket_ << ", token_socket_: " << token_socket_;
	if (request_socket_ != -1)
	{
		if (shutdown(request_socket_, 2) != 0 && errno == ENOTSOCK)
		{
			TLOG(TLVL_ERROR) << "Shutdown of request_socket_ resulted in ENOTSOCK. NOT Closing file descriptor!";
		}
		else
		{
			close(request_socket_);
		}
		request_socket_ = -1;
	}
	if (token_socket_ != -1)
	{
		if (shutdown(token_socket_, 2) != 0 && errno == ENOTSOCK)
		{
			TLOG(TLVL_ERROR) << "Shutdown of token_socket_ resulted in ENOTSOCK. NOT Closing file descriptor!";
		}
		else
		{
			close(token_socket_);
		}
		token_socket_ = -1;
	}
	StopAcknowledgementReception();
}

void RequestSender::SetRequestMode(detail::RequestMessageMode mode)
{
	request_mode_ = mode;
	SendRequest(true);
}

void RequestSender::setup_requests_()
{
	if (send_requests_)
	{
		request_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (request_socket_ < 0)
		{
			TLOG(TLVL_ERROR) << "I failed to create the socket for sending Data Requests! err=" << strerror(errno);
			exit(1);
		}
		int sts = ResolveHost(request_address_.c_str(), request_port_, request_addr_);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Unable to resolve Data Request address, err=" << strerror(errno);
			exit(1);
		}

		if (multicast_out_addr_ == "0.0.0.0")
		{
			multicast_out_addr_.reserve(HOST_NAME_MAX);
			sts = gethostname(&multicast_out_addr_[0], HOST_NAME_MAX);
			if (sts < 0)
			{
				TLOG(TLVL_ERROR) << "Could not get current hostname,  err=" << strerror(errno);
				exit(1);
			}
		}

		if (multicast_out_addr_ != "localhost")
		{
			struct in_addr addr;
			sts = GetInterfaceForNetwork(multicast_out_addr_.c_str(), addr);
			//sts = ResolveHost(multicast_out_addr_.c_str(), addr);
			if (sts == -1)
			{
				TLOG(TLVL_ERROR) << "Unable to resolve multicast interface address, err=" << strerror(errno);
				exit(1);
			}

			if (setsockopt(request_socket_, IPPROTO_IP, IP_MULTICAST_IF, &addr, sizeof(addr)) == -1)
			{
				TLOG(TLVL_ERROR) << "Cannot set outgoing interface, err=" << strerror(errno);
				exit(1);
			}
		}
		int yes = 1;
		if (setsockopt(request_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		{
			TLOG(TLVL_ERROR) << "Unable to enable port reuse on request socket, err=" << strerror(errno);
			exit(1);
		}
		if (setsockopt(request_socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &yes, sizeof(yes)) < 0)
		{
			TLOG(TLVL_ERROR) << "Unable to enable multicast loopback on request socket, err=" << strerror(errno);
			exit(1);
		}
		if (setsockopt(request_socket_, SOL_SOCKET, SO_BROADCAST, (void*)&yes, sizeof(int)) == -1)
		{
			TLOG(TLVL_ERROR) << "Cannot set request socket to broadcast, err=" << strerror(errno);
			exit(1);
		}
	}
}

void RequestSender::setup_tokens_()
{
	if (send_routing_tokens_)
	{
		TLOG(TLVL_DEBUG) << "Creating Routing Token sending socket";
		auto start_time = std::chrono::steady_clock::now();
		while (token_socket_ < 0 && TimeUtils::GetElapsedTime(start_time) < 30)
		{
			token_socket_ = TCPConnect(token_address_.c_str(), token_port_, 0, sizeof(detail::RoutingToken));
			if (token_socket_ < 0)
			{
				TLOG(TLVL_TRACE) << "Waited " << TimeUtils::GetElapsedTime(start_time) << " s for Routing Master to open token socket";
				usleep(100000);
			}
		}
		if (token_socket_ < 0)
		{
			TLOG(TLVL_ERROR) << "I failed to create the socket for sending Routing Tokens! err=" << strerror(errno);
			exit(1);
		}
		TLOG(TLVL_DEBUG) << "Routing Token sending socket created successfully";
	}
}

void RequestSender::do_send_request_()
{
	if (!send_requests_)
	{
		request_sending_--;
		return;
	}
	if (request_socket_ == -1) setup_requests_();

	TLOG(TLVL_TRACE) << "Waiting for " << request_delay_ << " microseconds.";
	std::this_thread::sleep_for(std::chrono::microseconds(request_delay_));

	TLOG(TLVL_TRACE) << "Creating RequestMessage";
	detail::RequestMessage message;
	message.setRunNumber(run_number_);
	message.setAcknowledge(request_acknowledgements_);
	{
		std::lock_guard<std::mutex> lk(request_mutex_);
		auto req = active_requests_.begin();
		while (req != active_requests_.end())
		{
			if (request_acknowledgements_ && req != active_requests_.end() && !req->second.isActive())
			{
				TLOG(12) << "Removing request " << req->first << " (" << req->second << ") because all configured senders have acknowledged it";
				req = active_requests_.erase(req);
				continue;
			}
			double elapsed = TimeUtils::GetElapsedTime(req->second.request_time);
			if (elapsed > request_timeout_s_ && request_timeout_s_ > 0)
			{
				TLOG(12) << "Removing request " << req->second << " because it has timed out ( " << elapsed << " s/ " << request_timeout_s_ << " s)";
				req = active_requests_.erase(req);
				continue;
			}
			TLOG(12) << "Adding a request (" << req->second << ") to request message";
			message.addRequest(req->second);
			++req;
		}
	}
	TLOG(TLVL_TRACE) << "Setting mode flag in Message Header to " << request_mode_;
	message.setMode(request_mode_);
	char str[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &(request_addr_.sin_addr), str, INET_ADDRSTRLEN);
	std::unique_lock<std::mutex> lk2(request_send_mutex_);
	TLOG(TLVL_TRACE) << "Sending request for " << message.size() << " events to multicast group " << str
	                 << ", port " << request_port_ << ", interface " << multicast_out_addr_;
	auto buf = message.GetMessage();
	auto sts = sendto(request_socket_, &buf[0], buf.size(), 0, (struct sockaddr*)&request_addr_, sizeof(request_addr_));
	if (sts < 0 || static_cast<size_t>(sts) != buf.size())
	{
		TLOG(TLVL_ERROR) << "Error sending request message err=" << strerror(errno) << "sts=" << sts;
		request_socket_ = -1;
		request_sending_--;
		return;
	}
	TLOG(TLVL_TRACE) << "Done sending request sts=" << sts;
	requests_sent_++;

	request_sending_--;
}

void RequestSender::send_routing_token_(int nSlots, int run_number)
{
	TLOG(TLVL_TRACE) << "send_routing_token_ called, send_routing_tokens_=" << std::boolalpha << send_routing_tokens_;
	if (!send_routing_tokens_) return;
	if (token_socket_ == -1) setup_tokens_();
	detail::RoutingToken token;
	token.header = TOKEN_MAGIC;
	token.rank = my_rank;
	token.new_slots_free = nSlots;
	token.run_number = run_number;

	TLOG(TLVL_TRACE) << "Sending RoutingToken to " << token_address_ << ":" << token_port_;
	size_t sts = 0;
	while (sts < sizeof(detail::RoutingToken))
	{
		auto res = send(token_socket_, reinterpret_cast<uint8_t*>(&token) + sts, sizeof(detail::RoutingToken) - sts, 0);
		if (res < 0)
		{
			close(token_socket_);
			token_socket_ = -1;
			sts = 0;
			setup_tokens_();
			continue;
		}
		sts += res;
	}
	tokens_sent_ += nSlots;
	TLOG(TLVL_TRACE) << "Done sending RoutingToken to " << token_address_ << ":" << token_port_;
}

void RequestSender::setup_acknowledgements_()
{
	TLOG(TLVL_DEBUG) << "Creating Acknowledgement socket";
	ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (ack_socket_ < 0)
	{
		throw cet::exception("ConfigurationError") << "RequestSender: Error creating socket for receiving acks!" << std::endl;
		exit(1);
	}

	struct sockaddr_in si_me_request;

	auto yes = 1;
	if (setsockopt(ack_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		throw cet::exception("ConfigurationError") << "RequestSender: Unable to enable port reuse on ack socket" << std::endl;
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(ack_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(ack_socket_, reinterpret_cast<struct sockaddr*>(&si_me_request), sizeof(si_me_request)) == -1)
	{
		throw cet::exception("ConfigurationError") << "RequestSender: Cannot bind request socket to port " << ack_port_ << std::endl;
		exit(1);
	}
	TLOG(TLVL_DEBUG) << "Listening for acks on 0.0.0.0 port " << ack_port_;
}

void RequestSender::receive_acknowledgements_()
{
	std::vector<uint8_t> buffer(MAX_REQUEST_MESSAGE_SIZE);
	ack_thread_running_.store(true);

	while (request_acknowledgements_ && receive_acknowledgements_enabled_.load())
	{
		// Reconnect ack socket, if necessary
		if (ack_socket_ == -1)
		{
			setup_acknowledgements_();
		}

		TLOG(20) << "receive_acknowledgements_: Calling recvfrom on ack socket";
		auto sts = recvfrom(ack_socket_, &buffer[0], MAX_REQUEST_MESSAGE_SIZE, MSG_DONTWAIT, NULL, NULL);
		TLOG(20) << "receive_acknowledgements_: recvfrom on ack socket done, sts=" << sts;

		if (sts < 0)
		{
			if (errno == EWOULDBLOCK || errno == EAGAIN)
			{
				TLOG(20) << "receive_acknowledgements_: No more ack datagrams on ack socket.";
			}
			else
			{
				TLOG(TLVL_ERROR) << "receive_acknowledgements_: An unexpected error occurred during ack packet receive";
				close(ack_socket_);
				ack_socket_ = -1;
			}
			usleep(10000);
		}
		else
		{
			auto ack = *reinterpret_cast<detail::RequestAcknowledgement*>(&buffer[0]);
			if (!ack.isValid())
			{
				TLOG(TLVL_WARNING) << "receive_acknowledgements_: Received ack message is invalid! Closing ack_socket and retrying";
				close(ack_socket_);
				ack_socket_ = -1;
				continue;
			}

			TLOG(TLVL_DEBUG) << "receive_acknowledgements_: Ack packet from rank " << ack.rank << " has run number " << ack.run_number << " and contains " << ack.packet_count << " entries";
			ack_messages_received_++;

			if (ack.run_number != run_number_)
			{
				TLOG(TLVL_WARNING) << "receive_acknowledgements_: Received ack message is for the wrong run! Not processing";
				continue;
			}

			std::lock_guard<std::mutex> lk(request_mutex_);
			size_t offset = sizeof(ack);
			for (size_t ii = 0; ii < ack.packet_count; ++ii)
			{
				if (offset >= static_cast<size_t>(sts))
				{
					TLOG(TLVL_WARNING) << "receive_acknowledgements_: UDP datagram corruption detected! offset/sts: " << offset << "/" << sts << ", at packet " << ii << " of " << ack.packet_count;
					break;
				}
				auto seq = *reinterpret_cast<Fragment::sequence_id_t*>(&buffer[offset]);
				TLOG(6) << "receive_acknowledgements_: Rank " << ack.rank << " acknowledges sequence_id " << seq;
				if (active_requests_.count(seq))
				{
					active_requests_[seq].clearRank(ack.rank);
				}
				offset += sizeof(Fragment::sequence_id_t);
			}
		}
	}
	ack_thread_running_.store(false);
}

void RequestSender::ClearCompletedRequests()
{
	std::lock_guard<std::mutex> lk(request_mutex_);
	auto req = active_requests_.begin();
	while (req != active_requests_.end())
	{
		if (request_acknowledgements_ && req != active_requests_.end() && !req->second.isActive())
		{
			TLOG(12) << "Removing request " << req->first << " (" << req->second << ") because all configured senders have acknowledged it";
			req = active_requests_.erase(req);
			continue;
		}
		double elapsed = TimeUtils::GetElapsedTime(req->second.request_time);
		if (elapsed > request_timeout_s_ && request_timeout_s_ > 0)
		{
			TLOG(12) << "Removing request " << req->second << " because it has timed out ( " << elapsed << " s/ " << request_timeout_s_ << " s)";
			req = active_requests_.erase(req);
			continue;
		}
		++req;
	}
}

void RequestSender::SendRoutingToken(int nSlots, int run_number)
{
	while (!initialized_) usleep(1000);
	if (!send_routing_tokens_) return;
	boost::thread token([=] { send_routing_token_(nSlots, run_number); });
	token.detach();
	usleep(0);  // Give up time slice
}

void RequestSender::StartAcknowledgementReception()
{
	if (!ack_thread_running_)
	{
		if (ack_thread_.joinable()) ack_thread_.join();
		receive_acknowledgements_enabled_ = true;

		TLOG(TLVL_INFO) << "Starting Acknowledgement Reception Thread";
		try
		{
			ack_thread_ = boost::thread(&RequestSender::receive_acknowledgements_, this);
		}
		catch (const boost::exception& e)
		{
			TLOG(TLVL_ERROR) << "Caught boost::exception starting Acknowledgement Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
			std::cerr << "Caught boost::exception starting Acknowledgement Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
			exit(5);
		}
	}
}

void RequestSender::StopAcknowledgementReception()
{
	if (ack_thread_running_)
	{
		TLOG(TLVL_DEBUG) << "Stopping Acknowledgement reception thread";
		receive_acknowledgements_enabled_.store(false);
		if (ack_thread_.joinable()) ack_thread_.join();
	}

	if (ack_socket_ != -1)
	{
		TLOG(TLVL_DEBUG) << "Closing Acknowledgement reception socket";
		close(ack_socket_);
		ack_socket_ = -1;
	}
}

void RequestSender::SendRequest(bool endOfRunOnly)
{
	while (!initialized_) usleep(1000);

	if (!send_requests_) return;
	if (endOfRunOnly && request_mode_ != detail::RequestMessageMode::EndOfRun) return;

	if (request_acknowledgements_ && !ack_thread_running_)
	{
		StartAcknowledgementReception();
	}

	request_sending_++;
	boost::thread request([=] { do_send_request_(); });
	request.detach();
}

void RequestSender::AddRequest(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp, int rank)
{
	while (!initialized_) usleep(1000);

	{
		std::lock_guard<std::mutex> lk(request_mutex_);
		if (!active_requests_.count(seqID))
		{
			TLOG(12) << "Adding request for sequence ID " << seqID << " and timestamp " << timestamp << " to request list.";
			active_requests_[seqID] = detail::RequestPacket(seqID, timestamp, rank);
			active_requests_[seqID].setActiveRanks(sender_ranks_);
		}
	}
	SendRequest();
}

void RequestSender::RemoveRequest(Fragment::sequence_id_t seqID)
{
	while (!initialized_) usleep(1000);
	std::lock_guard<std::mutex> lk(request_mutex_);
	TLOG(12) << "Removing request for sequence ID " << seqID << " from request list.";
	active_requests_.erase(seqID);
}
}  // namespace artdaq
