#define BOOST_TEST_MODULE NoOp_policy_t
#include <boost/test/unit_test.hpp>

#include "TRACE/tracemf.h"

#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"

BOOST_AUTO_TEST_SUITE(NoOp_policy_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	TLOG(TLVL_INFO) << "NoOp_policy_t Test Case Simple BEGIN";
	fhicl::ParameterSet ps = fhicl::ParameterSet::make("");

	auto noop = artdaq::makeRoutingManagerPolicy("NoOp", ps);

	noop->Reset();
	noop->AddReceiverToken(1, 1);
	noop->AddReceiverToken(3, 1);
	noop->AddReceiverToken(2, 1);
	noop->AddReceiverToken(4, 1);
	noop->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(noop->GetReceiverCount(), 4);
	auto secondTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 4);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 5);

	noop->AddReceiverToken(1, 0);

	auto thirdTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);
	TLOG(TLVL_INFO) << "NoOp_policy_t Test Case Simple END";
}

BOOST_AUTO_TEST_CASE(DataFlowMode)
{
	TLOG(TLVL_INFO) << "NoOp_policy_t Test Case DataFlowMode BEGIN";
	fhicl::ParameterSet ps = fhicl::ParameterSet::make("routing_manager_mode: DataFlow");

	auto noop = artdaq::makeRoutingManagerPolicy("NoOp", ps);

	noop->Reset();
	noop->AddReceiverToken(1, 1);
	noop->AddReceiverToken(3, 1);
	noop->AddReceiverToken(2, 1);
	noop->AddReceiverToken(3, 1);
	noop->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(noop->GetReceiverCount(), 3);
	auto route = noop->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID are allowed, and should receive different information
	route = noop->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Except, that the same sequence ID from the same host should always get the same info
	route = noop->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	route = noop->GetRouteForSequenceID(2, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	noop->AddReceiverToken(1, 1);
	route = noop->GetRouteForSequenceID(2, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	// Out-of-order sequence IDs are allowed
	route = noop->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Arbitrary sequence IDs are allowed
	route = noop->GetRouteForSequenceID(10343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10343);
	TLOG(TLVL_INFO) << "NoOp_policy_t Test Case DataFlowMode END";
}

BOOST_AUTO_TEST_CASE(RequestBasedEventBuilding)
{
	TLOG(TLVL_INFO) << "NoOp_policy_t Test Case RequestBasedEventBuilding BEGIN";
	fhicl::ParameterSet ps = fhicl::ParameterSet::make("routing_manager_mode: RequestBasedEventBuilding routing_cache_size: 2");

	auto noop = artdaq::makeRoutingManagerPolicy("NoOp", ps);

	noop->Reset();
	noop->AddReceiverToken(1, 1);
	noop->AddReceiverToken(3, 1);
	noop->AddReceiverToken(2, 1);
	noop->AddReceiverToken(3, 1);
	noop->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(noop->GetReceiverCount(), 3);

	auto route = noop->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID should receive the same routing
	route = noop->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Only events which have started routing should be in the table
	auto firstTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 1);
	BOOST_REQUIRE_EQUAL(firstTable[0].destination_rank, 1);

	// Arbitrary Sequence IDs are allowed
	route = noop->GetRouteForSequenceID(12343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 12343);

	// Out-of-order Sequence IDs are allowed
	route = noop->GetRouteForSequenceID(4, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 4);

	// Requests that arrive late still get the same info
	route = noop->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Check that things behave when tokens are exhausted...
	route = noop->GetRouteForSequenceID(10, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10);
	route = noop->GetRouteForSequenceID(11, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 11);

	route = noop->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, -1);

	noop->AddReceiverToken(1, 1);
	route = noop->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	route = noop->GetRouteForSequenceID(50, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	// Routing cache is sorted by sequence ID
	auto secondTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 4);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 10);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 11);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 12343);

	// Since the routing cache has been set to 2, only the highest two events routed are here, as the cache is checked when generating tables
	BOOST_REQUIRE_EQUAL(noop->GetCacheSize(), 2);
	auto thirdTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	BOOST_REQUIRE(noop->CacheHasRoute(50));
	BOOST_REQUIRE(!noop->CacheHasRoute(4));
	route = noop->GetRouteForSequenceID(50, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	TLOG(TLVL_INFO) << "NoOp_policy_t Test Case RequestBasedEventBuilding END";
}

BOOST_AUTO_TEST_SUITE_END()
