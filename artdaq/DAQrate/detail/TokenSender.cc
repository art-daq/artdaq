#include "artdaq/DAQdata/Globals.hh"  // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#define TRACE_NAME (app_name + "_TokenSender").c_str()
#include <dlfcn.h>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>
#include "artdaq/DAQrate/detail/TokenSender.hh"

#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "cetlib_except/exception.h"

namespace artdaq {
TokenSender::TokenSender(const fhicl::ParameterSet& pset)
    : initialized_(false)
    , send_routing_tokens_(pset.get<bool>("use_routing_manager", false))
    , token_port_(pset.get<int>("routing_token_port", 35555))
    , token_socket_(-1)
    , token_address_(pset.get<std::string>("routing_manager_hostname", "localhost"))
    , tokens_sent_(0)
    , run_number_(0)
{
	TLOG(TLVL_DEBUG + 32) << "TokenSender CONSTRUCTOR";

	setup_tokens_();
	TLOG(TLVL_DEBUG + 35) << "artdaq::TokenSender::TokenSender ctor - reader_thread_ initialized";
	initialized_ = true;
}

TokenSender::~TokenSender()
{
	TLOG(TLVL_INFO) << "Shutting down TokenSender, token_socket_: " << token_socket_;

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
}

void TokenSender::setup_tokens_()
{
	if (send_routing_tokens_)
	{
		TLOG(TLVL_DEBUG + 32) << "Creating Routing Token sending socket";
		auto start_time = std::chrono::steady_clock::now();
		while (token_socket_ < 0 && TimeUtils::GetElapsedTime(start_time) < 30)
		{
			token_socket_ = TCPConnect(token_address_.c_str(), token_port_, 0, sizeof(detail::RoutingToken));
			if (token_socket_ < 0)
			{
				TLOG(TLVL_DEBUG + 33) << "Waited " << TimeUtils::GetElapsedTime(start_time) << " s for Routing Manager to open token socket";
				usleep(100000);
			}
		}
		if (token_socket_ < 0)
		{
			TLOG(TLVL_ERROR) << "I failed to create the socket for sending Routing Tokens! err=" << strerror(errno);
			exit(1);
		}
		TLOG(TLVL_INFO) << "Routing Token sending socket created successfully for address " << token_address_;
	}
}

void TokenSender::send_routing_token_(int nSlots, int run_number, int rank)
{
	TLOG(TLVL_DEBUG + 33) << "send_routing_token_ called, send_routing_tokens_=" << std::boolalpha << send_routing_tokens_;
	if (!send_routing_tokens_)
	{
		return;
	}
	if (token_socket_ == -1)
	{
		setup_tokens_();
	}
	detail::RoutingToken token;
	token.header = TOKEN_MAGIC;
	token.rank = rank;
	token.new_slots_free = nSlots;
	token.run_number = run_number;

	TLOG(TLVL_DEBUG + 33) << "Sending RoutingToken to " << token_address_ << ":" << token_port_;
	size_t sts = 0;
	while (sts < sizeof(detail::RoutingToken))
	{
		auto res = send(token_socket_, reinterpret_cast<uint8_t*>(&token) + sts, sizeof(detail::RoutingToken) - sts, 0);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-bounds-pointer-arithmetic)
		if (res < 0)
		{
			TLOG(TLVL_WARNING) << "Error on token_socket, reconnecting";
			close(token_socket_);
			token_socket_ = -1;
			sts = 0;
			setup_tokens_();
			continue;
		}
		sts += res;
	}
	tokens_sent_ += nSlots;
	TLOG(TLVL_DEBUG + 33) << "Done sending RoutingToken to " << token_address_ << ":" << token_port_;
}

void TokenSender::SendRoutingToken(int nSlots, int run_number, int rank)
{
	while (!initialized_)
	{
		usleep(1000);
	}
	if (!send_routing_tokens_)
	{
		return;
	}
	boost::thread token([=,this] { send_routing_token_(nSlots, run_number, rank); });
	token.detach();
	usleep(0);  // Give up time slice
}

}  // namespace artdaq
