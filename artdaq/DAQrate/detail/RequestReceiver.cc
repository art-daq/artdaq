#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RequestReceiver").c_str()

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "artdaq/DAQrate/detail/RequestReceiver.hh"

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
    : request_stop_requested_(false)
    , requests_received_(0)
    , should_stop_(false)
    , request_addr_("227.128.12.26")
    , receive_requests_(false)
{}

artdaq::RequestReceiver::RequestReceiver(const fhicl::ParameterSet& ps, std::shared_ptr<RequestBuffer> output_buffer)
    : request_stop_requested_(false)
    , requests_received_(0)
    , should_stop_(false)
    , request_port_(ps.get<int>("request_port", 3001))
    , request_addr_(ps.get<std::string>("request_address", "227.128.12.26"))
    , multicast_in_addr_(ps.get<std::string>("multicast_interface_ip", "0.0.0.0"))
    , receive_requests_(ps.get<bool>("receive_requests", false))
    , end_of_run_timeout_ms_(ps.get<size_t>("end_of_run_quiet_timeout_ms", 1000))
    , requests_(output_buffer)
{
	TLOG(TLVL_DEBUG + 32) << "RequestReceiver CONSTRUCTOR ps: " << ps.to_string();
	if (receive_requests_)
	{
		setupRequestListener();
	}
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
	if (bind(request_socket_, reinterpret_cast<struct sockaddr*>(&si_me_request), sizeof(si_me_request)) == -1)  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
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
			TLOG(TLVL_ERROR) << "Unable to determine the multicast network interface for " << multicast_in_addr_;
			exit(1);
		}
		char addr_str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(mreq.imr_interface), addr_str, INET_ADDRSTRLEN);
		TLOG(TLVL_INFO) << "Successfully determined the multicast network interface for " << multicast_in_addr_ << ": " << addr_str << " (RequestReceiver)";
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
	if (!receive_requests_) return;
	if (requests_received_ == 0 && !force)
	{
		TLOG(TLVL_ERROR) << "Stop request received by RequestReceiver, but no requests have ever been received." << std::endl
		                 << "Check that UDP port " << request_port_ << " is open in the firewall config.";
	}
	should_stop_ = true;
	if (running_)
	{
		TLOG(TLVL_DEBUG + 32) << "Joining requestThread";
		try
		{
			if (requestThread_.joinable())
			{
				requestThread_.join();
			}
		}
		catch (...)
		{
			// IGNORED
		}
		bool once = true;
		while (running_)
		{
			if (once)
			{
				TLOG(TLVL_ERROR) << "running_ is true after thread join! Should NOT happen";
			}
			once = false;
			usleep(10000);
		}
	}

	if (request_socket_ != -1)
	{
		close(request_socket_);
		request_socket_ = -1;
	}
	TLOG(TLVL_INFO) << "RequestReceiver stopped, received " << requests_received_ << " request messages";
	requests_received_ = 0;
}

void artdaq::RequestReceiver::startRequestReception()
{
	if (!receive_requests_) return;
	std::unique_lock<std::mutex> lk(state_mutex_);
	if (requestThread_.joinable())
	{
		requestThread_.join();
	}
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
		char tname[16];                                             // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-ReqRecv", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                            // assure term. snprintf is not too evil :)
		auto handle = requestThread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Request Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Request Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}

void artdaq::RequestReceiver::receiveRequestsLoop()
{
	running_ = true;
	requests_->reset();
	requests_->setRunning(true);
	while (!should_stop_)
	{
		TLOG(TLVL_DEBUG + 35) << "receiveRequestsLoop: Polling Request socket for new requests";

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
			if (rv == 1 && ((ufds[0].revents & (POLLNVAL | POLLERR | POLLHUP)) != 0))
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

		TLOG(TLVL_DEBUG + 34) << "Received packet on Request channel";
		std::vector<uint8_t> buffer(MAX_REQUEST_MESSAGE_SIZE);
		struct sockaddr_in from;
		socklen_t len = sizeof(from);
		auto sts = recvfrom(request_socket_, &buffer[0], MAX_REQUEST_MESSAGE_SIZE, 0, reinterpret_cast<struct sockaddr*>(&from), &len);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
		if (sts < 0)
		{
			TLOG(TLVL_ERROR) << "Error receiving request message header err=" << strerror(errno);
			close(request_socket_);
			request_socket_ = -1;
			continue;
		}

		auto hdr_buffer = reinterpret_cast<artdaq::detail::RequestHeader*>(&buffer[0]);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
		TLOG(TLVL_DEBUG + 34) << "Request header word: 0x" << std::hex << hdr_buffer->header << std::dec << ", packet_count: " << hdr_buffer->packet_count << " from rank " << hdr_buffer->rank << ", " << inet_ntoa(from.sin_addr) << ":" << from.sin_port << ", run number: " << hdr_buffer->run_number;
		if (!hdr_buffer->isValid())
		{
			continue;
		}

		// 19-Dec-2018, KAB: added check on current run number
		if (run_number_ != 0 && hdr_buffer->run_number != run_number_)
		{
			TLOG(TLVL_WARNING) << "Received a Request Message with the wrong run number ("
			                   << hdr_buffer->run_number << "), expected " << run_number_
			                   << ", ignoring this request.";
			continue;
		}

		requests_received_++;

		if (hdr_buffer->mode == artdaq::detail::RequestMessageMode::EndOfRun)
		{
			TLOG(TLVL_INFO) << "Received Request Message with the EndOfRun marker. (Re)Starting 1-second timeout for receiving all outstanding requests...";
			request_stop_timeout_ = std::chrono::steady_clock::now();
			request_stop_requested_ = true;
		}

		std::vector<artdaq::detail::RequestPacket> pkt_buffer(hdr_buffer->packet_count);
		memcpy(&pkt_buffer[0], &buffer[sizeof(artdaq::detail::RequestHeader)], sizeof(artdaq::detail::RequestPacket) * hdr_buffer->packet_count);

		if (should_stop_)
		{
			break;
		}

		for (auto& buffer : pkt_buffer)
		{
			TLOG(TLVL_DEBUG + 36) << "Request Packet: hdr=" << /*std::dec <<*/ buffer.header << ", seq=" << buffer.sequence_id << ", ts=" << buffer.timestamp;
			if (!buffer.isValid()) continue;
			requests_->push(buffer.sequence_id, buffer.timestamp);
		}
	}
	TLOG(TLVL_DEBUG + 32) << "Ending Request Thread";
	running_ = false;
	requests_->setRunning(false);
}
