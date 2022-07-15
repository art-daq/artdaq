#define BOOST_TEST_MODULE CapacityTest_policy_t
#include <boost/test/unit_test.hpp>

#include "TRACE/tracemf.h"

#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"

BOOST_AUTO_TEST_SUITE(CapacityTest_policy_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	TLOG(TLVL_INFO) << "CapacityTest_policy_t Test Case Simple BEGIN";
	fhicl::ParameterSet ps = fhicl::ParameterSet::make("tokens_used_per_table_percent: 50");

	auto ct = artdaq::makeRoutingManagerPolicy("CapacityTest", ps);

	ct->AddReceiverToken(1, 10);
	ct->AddReceiverToken(2, 10);
	ct->AddReceiverToken(3, 10);
	ct->AddReceiverToken(4, 10);
	BOOST_REQUIRE_EQUAL(ct->GetReceiverCount(), 4);

	auto firstTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 20);
	BOOST_REQUIRE_EQUAL(firstTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(firstTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(firstTable[firstTable.size() - 1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(firstTable[firstTable.size() - 1].sequence_id, 20);

	ct->ResetTokensUsedSinceLastUpdate();

	//ct->Reset();
	ct->AddReceiverToken(1, 1);
	ct->AddReceiverToken(3, 1);
	ct->AddReceiverToken(2, 1);
	ct->AddReceiverToken(4, 1);
	ct->AddReceiverToken(2, 1);
	auto secondTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 13);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 21);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 22);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 23);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 24);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 25);
	BOOST_REQUIRE_EQUAL(secondTable[5].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[5].sequence_id, 26);
	BOOST_REQUIRE_EQUAL(secondTable[6].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[6].sequence_id, 27);
	BOOST_REQUIRE_EQUAL(secondTable[7].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[7].sequence_id, 28);
	BOOST_REQUIRE_EQUAL(secondTable[8].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[8].sequence_id, 29);
	BOOST_REQUIRE_EQUAL(secondTable[9].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[9].sequence_id, 30);
	BOOST_REQUIRE_EQUAL(secondTable[10].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[10].sequence_id, 31);
	BOOST_REQUIRE_EQUAL(secondTable[11].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[11].sequence_id, 32);
	BOOST_REQUIRE_EQUAL(secondTable[12].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[12].sequence_id, 33);

	ct->ResetTokensUsedSinceLastUpdate();
	//ct->Reset();
	ct->AddReceiverToken(1, 0);
	auto thirdTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 6);
	BOOST_REQUIRE_EQUAL(thirdTable[0].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(thirdTable[0].sequence_id, 34);
	BOOST_REQUIRE_EQUAL(thirdTable[1].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(thirdTable[1].sequence_id, 35);
	BOOST_REQUIRE_EQUAL(thirdTable[2].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(thirdTable[2].sequence_id, 36);
	BOOST_REQUIRE_EQUAL(thirdTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(thirdTable[3].sequence_id, 37);
	BOOST_REQUIRE_EQUAL(thirdTable[4].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(thirdTable[4].sequence_id, 38);
	BOOST_REQUIRE_EQUAL(thirdTable[5].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(thirdTable[5].sequence_id, 39);

	ct->AddReceiverToken(1, 2);
	ct->AddReceiverToken(2, 1);
	ct->AddReceiverToken(3, 1);
	ct->AddReceiverToken(4, 2);
	ct->ResetTokensUsedSinceLastUpdate();
	auto fourthTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fourthTable.size(), 6);
	BOOST_REQUIRE_EQUAL(fourthTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(fourthTable[0].sequence_id, 40);
	BOOST_REQUIRE_EQUAL(fourthTable[1].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(fourthTable[1].sequence_id, 41);
	BOOST_REQUIRE_EQUAL(fourthTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(fourthTable[2].sequence_id, 42);
	BOOST_REQUIRE_EQUAL(fourthTable[3].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(fourthTable[3].sequence_id, 43);
	BOOST_REQUIRE_EQUAL(fourthTable[4].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(fourthTable[4].sequence_id, 44);
	BOOST_REQUIRE_EQUAL(fourthTable[5].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(fourthTable[5].sequence_id, 45);

	ct->AddReceiverToken(3, 1);
	ct->ResetTokensUsedSinceLastUpdate();
	auto fifthTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fifthTable.size(), 4);
	BOOST_REQUIRE_EQUAL(fifthTable[0].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(fifthTable[0].sequence_id, 46);
	BOOST_REQUIRE_EQUAL(fifthTable[1].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(fifthTable[1].sequence_id, 47);
	BOOST_REQUIRE_EQUAL(fifthTable[2].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(fifthTable[2].sequence_id, 48);
	BOOST_REQUIRE_EQUAL(fifthTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(fifthTable[3].sequence_id, 49);

	ct->Reset();
	ct->AddReceiverToken(1, 2);
	ct->ResetTokensUsedSinceLastUpdate();
	auto sixthTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(sixthTable.size(), 1);
	BOOST_REQUIRE_EQUAL(sixthTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(sixthTable[0].sequence_id, 1);
	TLOG(TLVL_INFO) << "CapacityTest_policy_t Test Case Simple END";
}

BOOST_AUTO_TEST_CASE(DataFlowMode)
{
	TLOG(TLVL_INFO) << "CapacityTest_policy_t Test Case DataFlowMode BEGIN";
	fhicl::ParameterSet ps = fhicl::ParameterSet::make("routing_manager_mode: DataFlow");

	auto ct = artdaq::makeRoutingManagerPolicy("CapacityTest", ps);

	ct->Reset();
	ct->AddReceiverToken(1, 3);
	ct->AddReceiverToken(2, 3);
	BOOST_REQUIRE_EQUAL(ct->GetReceiverCount(), 2);
	auto route = ct->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID are allowed, and should receive different information
	route = ct->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Except, that the same sequence ID from the same host should always get the same info
	route = ct->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	route = ct->GetRouteForSequenceID(2, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	route = ct->GetRouteForSequenceID(2, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);

	ct->AddReceiverToken(1, 1);
	route = ct->GetRouteForSequenceID(2, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	// Out-of-order sequence IDs are allowed
	route = ct->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Arbitrary sequence IDs are allowed
	route = ct->GetRouteForSequenceID(10343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10343);

	TLOG(TLVL_INFO) << "CapacityTest_policy_t Test Case DataFlowMode END";
}

BOOST_AUTO_TEST_CASE(RequestBasedEventBuilding)
{
	TLOG(TLVL_INFO) << "CapacityTest_policy_t Test Case RequestBasedEventBuilding BEGIN";
	fhicl::ParameterSet ps = fhicl::ParameterSet::make("routing_manager_mode: RequestBasedEventBuilding routing_cache_size: 2");

	auto ct = artdaq::makeRoutingManagerPolicy("CapacityTest", ps);

	ct->Reset();
	ct->AddReceiverToken(1, 3);
	ct->AddReceiverToken(2, 3);
	BOOST_REQUIRE_EQUAL(ct->GetReceiverCount(), 2);

	auto route = ct->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID should receive the same routing
	route = ct->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Only events which have started routing should be in the table
	auto firstTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 1);
	BOOST_REQUIRE_EQUAL(firstTable[0].destination_rank, 1);

	// Arbitrary Sequence IDs are allowed
	route = ct->GetRouteForSequenceID(12343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 12343);

	// Out-of-order Sequence IDs are allowed
	route = ct->GetRouteForSequenceID(4, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 4);

	// Requests that arrive late still get the same info
	route = ct->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Check that things behave when tokens are exhausted...
	route = ct->GetRouteForSequenceID(10, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10);
	route = ct->GetRouteForSequenceID(11, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 11);
	route = ct->GetRouteForSequenceID(12, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 12);

	route = ct->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, -1);

	ct->AddReceiverToken(1, 1);
	route = ct->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	route = ct->GetRouteForSequenceID(50, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	// Routing cache is sorted by sequence ID
	auto secondTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 6);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 4);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 10);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 11);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 12);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(secondTable[5].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[5].sequence_id, 12343);

	// Since the routing cache has been set to 2, only the highest two events routed are here, as the cache is checked when generating tables
	BOOST_REQUIRE_EQUAL(ct->GetCacheSize(), 2);
	auto thirdTable = ct->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	BOOST_REQUIRE(ct->CacheHasRoute(50));
	BOOST_REQUIRE(!ct->CacheHasRoute(4));
	route = ct->GetRouteForSequenceID(50, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	TLOG(TLVL_INFO) << "CapacityTest_policy_t Test Case RequestBasedEventBuilding END";
}

BOOST_AUTO_TEST_SUITE_END()
