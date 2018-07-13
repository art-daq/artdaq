#define TRACE_NAME (app_name + "_TokenSender").c_str()
#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq/DAQrate/TokenSender.hh"
#include <utility>
#include <cstring>
#include <dlfcn.h>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <chrono>

#include "cetlib_except/exception.h"
#include "artdaq-core/Core/StatisticsCollection.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/DAQdata/TCPConnect.hh"

namespace artdaq
{
	TokenSender::TokenSender(const fhicl::ParameterSet& pset)
		: initialized_(false)
		, token_socket_(-1)
	{
		fhicl::ParameterSet ps = pset;
		if (pset.has_key("routing_token_config"))
		{
			ps = pset.get<fhicl::ParameterSet>("routing_token_config");
		}
		if (pset.has_key("routing_master_config"))
		{
			ps = pset.get<fhicl::ParameterSet>("routing_master_config");
		}

		TLOG(TLVL_DEBUG) << "TokenSender CONSTRUCTOR";

		send_routing_tokens_ = ps.get<bool>("use_routing_master", false);
		token_port_ = ps.get<int>("routing_token_port", 35555);
		token_address_ = ps.get<std::string>("routing_master_hostname", "localhost");
		setup_tokens_();
		TLOG(12) << "artdaq::TokenSender::TokenSender ctor - reader_thread_ initialized";
		initialized_ = true;
	}


	TokenSender::~TokenSender()
	{
		TLOG(TLVL_INFO) << "Shutting down TokenSender";
		if (token_socket_ > 0)
		{
			shutdown(token_socket_, 2);
			close(token_socket_);
		}
	}
	
	void TokenSender::setup_tokens_()
	{
		if (send_routing_tokens_)
		{
			TLOG(TLVL_DEBUG) << "Creating Routing Token sending socket";
			int retry = 5;
			while (retry > 0 && token_socket_ < 0)
			{
				token_socket_ = TCPConnect(token_address_.c_str(), token_port_, 0, sizeof(detail::RoutingToken));
				if (token_socket_ < 0) usleep(100000);
				retry--;
			}
			if (token_socket_ < 0)
			{
				TLOG(TLVL_ERROR) << "I failed to create the socket for sending Routing Tokens! err=" << strerror(errno);
				exit(1);
			}
			TLOG(TLVL_DEBUG) << "Routing Token sending socket created successfully";
		}
	}

	void TokenSender::send_routing_token_(int nSlots)
	{
		TLOG(TLVL_TRACE) << "send_routing_token_ called, send_routing_tokens_=" << std::boolalpha << send_routing_tokens_;
		if (!send_routing_tokens_) return;
		if (token_socket_ == -1) setup_tokens_();
		detail::RoutingToken token;
		token.header = TOKEN_MAGIC;
		token.rank = my_rank;
		token.new_slots_free = nSlots;

		TLOG(TLVL_TRACE) << "Sending RoutingToken to " << token_address_ << ":" << token_port_;
		size_t sts = 0;
		while (sts < sizeof(detail::RoutingToken))
		{
			auto res = send(token_socket_, reinterpret_cast<uint8_t*>(&token) + sts, sizeof(detail::RoutingToken) - sts, 0);
			if (res == -1)
			{
				close(token_socket_);
				token_socket_ = -1;
				sts = 0;
				setup_tokens_();
				continue;
			}
			sts += res;
		}
		TLOG(TLVL_TRACE) << "Done sending RoutingToken to " << token_address_ << ":" << token_port_;
	}

	void TokenSender::SendRoutingToken(int nSlots)
	{
		while (!initialized_) usleep(1000);
		if (!send_routing_tokens_) return;
		boost::thread token([=] { send_routing_token_(nSlots); });
		token.detach();
		usleep(0); // Give up time slice
	}

}
