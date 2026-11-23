#define TRACE_NAME "TokenSender_t"

#include "artdaq/DAQrate/detail/TokenSender.hh"

#define BOOST_TEST_MODULE TokenSender_t
#include <sys/poll.h>
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQdata/TCP_listen_fd.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"

BOOST_AUTO_TEST_SUITE(TokenSender_test)

#define TRACE_REQUIRE_EQUAL(l, r)                                                                                                    \
	do                                                                                                                               \
	{                                                                                                                                \
		if ((l) == (r))                                                                                                              \
		{                                                                                                                            \
			TLOG(TLVL_DEBUG) << __LINE__ << ": Checking if " << #l << " (" << (l) << ") equals " << #r << " (" << (r) << ")...YES!"; \
		}                                                                                                                            \
		else                                                                                                                         \
		{                                                                                                                            \
			TLOG(TLVL_ERROR) << __LINE__ << ": Checking if " << #l << " (" << (l) << ") equals " << #r << " (" << (r) << ")...NO!";  \
		}                                                                                                                            \
		BOOST_REQUIRE_EQUAL((l), (r));                                                                                               \
	} while (0)

BOOST_AUTO_TEST_CASE(Construct)
{
	artdaq::configureMessageFacility("TokenSender_t", true, true);
	metricMan->initialize(fhicl::ParameterSet());
	metricMan->do_start();
	TLOG(TLVL_INFO) << "Construct Test Case BEGIN";
	fhicl::ParameterSet pset;
	artdaq::TokenSender t(pset);
	TLOG(TLVL_INFO) << "Construct Test Case END";
}

BOOST_AUTO_TEST_CASE(Tokens)
{
	artdaq::configureMessageFacility("TokenSender_t", true, true);
	metricMan->initialize(fhicl::ParameterSet());
	metricMan->do_start();
	TLOG(TLVL_INFO) << "Tokens Test Case BEGIN";
	const int TOKEN_PORT = (seedAndRandom() % (32768 - 1024)) + 1024;
	TLOG(TLVL_DEBUG) << "Opening token listener socket";
	auto token_socket = TCP_listen_fd(TOKEN_PORT, 3 * sizeof(artdaq::detail::RoutingToken));

	fhicl::ParameterSet token_pset;
	token_pset.put("routing_token_port", TOKEN_PORT);
	token_pset.put("use_routing_manager", true);
	fhicl::ParameterSet pset;
	pset.put("routing_token_config", token_pset);
	artdaq::TokenSender t(pset);

	my_rank = 0;

	BOOST_REQUIRE(token_socket != -1);
	if (token_socket == -1)
	{
		TLOG(TLVL_ERROR) << "Token listener socket was not opened successfully.";
		BOOST_REQUIRE_EQUAL(false, true);
		return;
	}

	TLOG(TLVL_DEBUG) << "Accepting new connection on token_socket";
	sockaddr_in addr;
	socklen_t arglen = sizeof(addr);
	auto conn_sock = accept(token_socket, reinterpret_cast<struct sockaddr*>(&addr), &arglen);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

	t.SendRoutingToken(120, 130);

	artdaq::detail::RoutingToken buff;
	auto sts = read(conn_sock, &buff, sizeof(artdaq::detail::RoutingToken));

	TRACE_REQUIRE_EQUAL(sts, sizeof(artdaq::detail::RoutingToken));
	TRACE_REQUIRE_EQUAL(buff.header, TOKEN_MAGIC);
	TRACE_REQUIRE_EQUAL(buff.new_slots_free, 120);
	TRACE_REQUIRE_EQUAL(buff.run_number, 130);
	TRACE_REQUIRE_EQUAL(buff.rank, 0);

	my_rank = 13;
	t.SendRoutingToken(335, 17);

	sts = read(conn_sock, &buff, sizeof(artdaq::detail::RoutingToken));

	TRACE_REQUIRE_EQUAL(sts, sizeof(artdaq::detail::RoutingToken));
	TRACE_REQUIRE_EQUAL(buff.header, TOKEN_MAGIC);
	TRACE_REQUIRE_EQUAL(buff.new_slots_free, 335);
	TRACE_REQUIRE_EQUAL(buff.run_number, 17);
	TRACE_REQUIRE_EQUAL(buff.rank, 13);

	close(conn_sock);
	close(token_socket);
	TLOG(TLVL_INFO) << "Tokens Test Case END";
}

BOOST_AUTO_TEST_SUITE_END()
