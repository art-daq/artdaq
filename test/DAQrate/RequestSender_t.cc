#include "artdaq/DAQrate/RequestSender.hh"

#define BOOST_TEST_MODULE(RequestSender_t)
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"
#include "artdaq/DAQdata/TCP_listen_fd.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include <sys/poll.h>


BOOST_AUTO_TEST_SUITE(RequestSender_test)

BOOST_AUTO_TEST_CASE(Construct) {
	fhicl::ParameterSet pset;
	artdaq::RequestSender t(pset);
}

BOOST_AUTO_TEST_CASE(Tokens)
{
	artdaq::configureMessageFacility("RequestSender_t");
	const int TOKEN_PORT = seedAndRandom() % 60000 + 1024;
	TLOG_DEBUG("RequestSender_t") << "Opening token listener socket" << TLOG_ENDL;
	auto token_socket = TCP_listen_fd(TOKEN_PORT, 3 * sizeof(artdaq::detail::RoutingToken));

	fhicl::ParameterSet token_pset;
	token_pset.put("routing_token_port", TOKEN_PORT);
	token_pset.put("use_routing_master", true);
	fhicl::ParameterSet pset;
	pset.put("routing_token_config", token_pset);
	artdaq::RequestSender t(pset);

	my_rank = 0;


	if (token_socket == -1)
	{
		BOOST_ERROR("Token listener socket was not opened successfully.");
		return;
	}

	TLOG_DEBUG("RequestSender_t") << "Accepting new connection on token_socket" << TLOG_ENDL;
	sockaddr_in addr;
	socklen_t arglen = sizeof(addr);
	auto conn_sock = accept(token_socket, (struct sockaddr*)&addr, &arglen);

	t.SendRoutingToken(120);

	artdaq::detail::RoutingToken buff;
	auto sts = read(conn_sock, &buff, sizeof(artdaq::detail::RoutingToken));

	BOOST_REQUIRE_EQUAL(sts, sizeof(artdaq::detail::RoutingToken));
	BOOST_REQUIRE_EQUAL(buff.header, TOKEN_MAGIC);
	BOOST_REQUIRE_EQUAL(buff.new_slots_free, 120);
	BOOST_REQUIRE_EQUAL(buff.rank, 0);

	my_rank = 13;
	t.SendRoutingToken(335);

	sts = read(conn_sock, &buff, sizeof(artdaq::detail::RoutingToken));

	BOOST_REQUIRE_EQUAL(sts, sizeof(artdaq::detail::RoutingToken));
	BOOST_REQUIRE_EQUAL(buff.header, TOKEN_MAGIC);
	BOOST_REQUIRE_EQUAL(buff.new_slots_free, 335);
	BOOST_REQUIRE_EQUAL(buff.rank, 13);
}

BOOST_AUTO_TEST_CASE(Requests)
{
	artdaq::configureMessageFacility("RequestSender_t");
	const int REQUEST_PORT = seedAndRandom() % 60000 + 1024;
	const int DELAY_TIME = 100;
	fhicl::ParameterSet pset;
	pset.put("request_port", REQUEST_PORT);
	pset.put("request_delay_ms", DELAY_TIME);
	pset.put("send_requests", true);
	pset.put("request_address", "localhost");
	artdaq::RequestSender t(pset);


	TLOG_DEBUG("RequestSender_t") << "Opening request listener socket" << TLOG_ENDL;
	auto request_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(request_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		BOOST_ERROR("Unable to set reuse on request socket");
		return;
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(REQUEST_PORT);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(request_socket, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
	{
		BOOST_ERROR("Cannot bind request socket to port " + std::to_string(REQUEST_PORT));
		return;
	}
	
	TLOG_DEBUG("RequestSender_t") << "Sending request" << TLOG_ENDL;
	auto start_time = std::chrono::steady_clock::now();
	t.AddRequest(0, 0x10);
	struct pollfd ufds[1];

	TLOG_DEBUG("RequestSender_t") << "Receiving Request" << TLOG_ENDL;
	ufds[0].fd = request_socket;
	ufds[0].events = POLLIN | POLLPRI;
	int rv = poll(ufds, 1, 10000);
	if (rv > 0)
	{
		if (ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
		{
			auto delay_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count();
			BOOST_REQUIRE_GE(delay_time, DELAY_TIME);
			TRACE(4, "CFG: Recieved packet on Request channel");
			artdaq::detail::RequestHeader hdr_buffer;
			recv(request_socket, &hdr_buffer, sizeof(hdr_buffer), 0);
			BOOST_REQUIRE_EQUAL(hdr_buffer.isValid(), true);
			BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr_buffer.mode),
								static_cast<uint8_t>(artdaq::detail::RequestMessageMode::Normal));
			BOOST_REQUIRE_EQUAL(hdr_buffer.packet_count, 1);
			if (hdr_buffer.isValid())
			{

				std::vector<artdaq::detail::RequestPacket> pkt_buffer(hdr_buffer.packet_count);
				recv(request_socket, &pkt_buffer[0], sizeof(artdaq::detail::RequestPacket) * hdr_buffer.packet_count, 0);

				for (auto& buffer : pkt_buffer)
				{
					BOOST_REQUIRE_EQUAL(buffer.isValid(), true);
					BOOST_REQUIRE_EQUAL(buffer.sequence_id, 0);
					BOOST_REQUIRE_EQUAL(buffer.timestamp, 0x10);
				}
			}
			else
			{
				BOOST_ERROR("Invalid header received");
				return;
			}
		}
		else
		{
			BOOST_ERROR("Wrong event type from poll");
			return;
		}
	}
	else
	{
		BOOST_ERROR("Timeout occured waiting for request");
		return;
	}


	t.SetRequestMode(artdaq::detail::RequestMessageMode::EndOfRun);
	t.AddRequest(2, 0x20);
	rv = poll(ufds, 1, 1000);
	if (rv > 0)
	{
		if (ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
		{
			auto delay_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count();
			BOOST_REQUIRE_GE(delay_time, DELAY_TIME);
			TRACE(4, "CFG: Recieved packet on Request channel");
			artdaq::detail::RequestHeader hdr_buffer;
			recv(request_socket, &hdr_buffer, sizeof(hdr_buffer), 0);
			BOOST_REQUIRE_EQUAL(hdr_buffer.isValid(), true);
			BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr_buffer.mode),
								static_cast<uint8_t>(artdaq::detail::RequestMessageMode::EndOfRun));
			BOOST_REQUIRE_EQUAL(hdr_buffer.packet_count, 2);
			if (hdr_buffer.isValid())
			{

				std::vector<artdaq::detail::RequestPacket> pkt_buffer(hdr_buffer.packet_count);
				recv(request_socket, &pkt_buffer[0], sizeof(artdaq::detail::RequestPacket) * hdr_buffer.packet_count, 0);

				BOOST_REQUIRE_EQUAL(pkt_buffer[0].isValid(), true);
				BOOST_REQUIRE_EQUAL(pkt_buffer[0].sequence_id, 0);
				BOOST_REQUIRE_EQUAL(pkt_buffer[0].timestamp, 0x10);
				BOOST_REQUIRE_EQUAL(pkt_buffer[1].isValid(), true);
				BOOST_REQUIRE_EQUAL(pkt_buffer[1].sequence_id, 2);
				BOOST_REQUIRE_EQUAL(pkt_buffer[1].timestamp, 0x20);

			}
			else
			{
				BOOST_ERROR("Invalid header received");
				return;
			}
		}
		else
		{
			BOOST_ERROR("Wrong event type from poll");
			return;
		}
	}
	else
	{
		BOOST_ERROR("Timeout occured waiting for request");
		return;
	}

	t.RemoveRequest(0);
	t.RemoveRequest(2);
	t.AddRequest(3, 0x30);
	rv = poll(ufds, 1, 1000);
	if (rv > 0)
	{
		if (ufds[0].revents == POLLIN || ufds[0].revents == POLLPRI)
		{
			auto delay_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count();
			BOOST_REQUIRE_GE(delay_time, DELAY_TIME);
			TRACE(4, "CFG: Recieved packet on Request channel");
			artdaq::detail::RequestHeader hdr_buffer;
			recv(request_socket, &hdr_buffer, sizeof(hdr_buffer), 0);
			BOOST_REQUIRE_EQUAL(hdr_buffer.isValid(), true);
			BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr_buffer.mode),
								static_cast<uint8_t>(artdaq::detail::RequestMessageMode::EndOfRun));
			BOOST_REQUIRE_EQUAL(hdr_buffer.packet_count, 1);
			if (hdr_buffer.isValid())
			{

				std::vector<artdaq::detail::RequestPacket> pkt_buffer(hdr_buffer.packet_count);
				recv(request_socket, &pkt_buffer[0], sizeof(artdaq::detail::RequestPacket) * hdr_buffer.packet_count, 0);

				BOOST_REQUIRE_EQUAL(pkt_buffer[0].isValid(), true);
				BOOST_REQUIRE_EQUAL(pkt_buffer[0].sequence_id, 3);
				BOOST_REQUIRE_EQUAL(pkt_buffer[0].timestamp, 0x30);

			}
			else
			{
				BOOST_ERROR("Invalid header received");
				return;
			}
		}
		else
		{
			BOOST_ERROR("Wrong event type from poll");
			return;
		}
	}
	else
	{
		BOOST_ERROR("Timeout occured waiting for request");
		return;
	}

}

BOOST_AUTO_TEST_SUITE_END()