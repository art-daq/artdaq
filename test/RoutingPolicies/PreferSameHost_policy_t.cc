#define BOOST_TEST_MODULE PreferSameHost_policy_t
#include <boost/test/unit_test.hpp>

#include "artdaq-utilities/Plugins/MakeParameterSet.hh"
#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"

BOOST_AUTO_TEST_SUITE(PreferSameHost_policy_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	fhicl::ParameterSet ps = artdaq::make_pset("");

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(4, 1);
	psh->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 4);
	auto secondTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 4);

	psh->AddReceiverToken(1, 0);

	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	psh->AddReceiverToken(1, 2);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(4, 2);
	auto fourthTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fourthTable.size(), 4);
	BOOST_REQUIRE_EQUAL(fourthTable[0].destination_rank, 1);

	psh->AddReceiverToken(3, 1);
	auto fifthTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fifthTable.size(), 4);
	BOOST_REQUIRE_EQUAL(fifthTable[0].destination_rank, 1);
}

BOOST_AUTO_TEST_CASE(MinimumParticipants)
{
	fhicl::ParameterSet ps = artdaq::make_pset("minimum_participants: 2");

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(4, 1);
	psh->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 4);
	auto secondTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 4);

	psh->AddReceiverToken(1, 0);

	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	psh->AddReceiverToken(1, 1);
	auto fourthTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fourthTable.size(), 2);

	BOOST_REQUIRE_EQUAL(fourthTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(fourthTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(fourthTable[0].sequence_id, 5);
	BOOST_REQUIRE_EQUAL(fourthTable[1].sequence_id, 6);

	psh->AddReceiverToken(1, 2);
	psh->AddReceiverToken(2, 2);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(4, 2);
	auto fifthTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fifthTable.size(), 7);
	BOOST_REQUIRE_EQUAL(fifthTable[0].destination_rank, 1);

	psh->AddReceiverToken(3, 1);
	auto sixthTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(sixthTable.size(), 0);
}

// RECENT CHANGE IN BEHAVIOR! Since the number of receivers is now dynamic as tokens are added, RoundRobin_policy will ALWAYS wait for at least minimum_participants to be available when it is positive!
BOOST_AUTO_TEST_CASE(LargeMinimumParticipants)
{
	TLOG(TLVL_INFO) << "PreferSameHost_policy_t Test Case LargeMinimumParticipants BEGIN";
	fhicl::ParameterSet ps = artdaq::make_pset("minimum_participants: 5");

	auto rr = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	rr->Reset();
	rr->AddReceiverToken(1, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(rr->GetReceiverCount(), 3);
	auto firstTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 0);

	rr->AddReceiverToken(5, 1);
	rr->AddReceiverToken(6, 1);

	auto secondTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 5);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 6);

	rr->AddReceiverToken(1, 1);
	auto thirdTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	TLOG(TLVL_INFO) << "PreferSameHost_policy_t Test Case LargeMinimumParticipants END";
}

BOOST_AUTO_TEST_CASE(ManyMissingParticipants)
{
	fhicl::ParameterSet ps = artdaq::make_pset("minimum_participants: -5");

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);
	auto secondTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);

	psh->AddReceiverToken(1, 1);
	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 1);
}

BOOST_AUTO_TEST_CASE(DataFlowMode)
{
	fhicl::ParameterSet ps = artdaq::make_pset(
	    "routing_manager_mode: DataFlow \
                              host_map: [{rank: 1 host: \"testHost1\"}, \
                                         {rank: 2 host: \"testHost1\"}, \
                                         {rank: 3 host: \"testHost2\"}, \
                                         {rank: 4 host: \"testHost1\"}, \
                                         {rank: 5 host: \"testHost2\"}, \
                                         {rank: 6 host: \"testHost3\"}]");

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);
	auto route = psh->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID are allowed, and should receive different information
	route = psh->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Except, that the same sequence ID from the same host should always get the same info
	route = psh->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	route = psh->GetRouteForSequenceID(2, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	psh->AddReceiverToken(1, 1);
	route = psh->GetRouteForSequenceID(2, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	// Out-of-order sequence IDs are allowed
	route = psh->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Arbitrary sequence IDs are allowed
	route = psh->GetRouteForSequenceID(10343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10343);
}

BOOST_AUTO_TEST_CASE(RequestBasedEventBuilding)
{
	fhicl::ParameterSet ps = artdaq::make_pset(
	    "routing_manager_mode: RequestBasedEventBuilding routing_cache_size: 2 \
                              host_map: [{rank: 1 host: \"testHost1\"}, \
                                         {rank: 2 host: \"testHost1\"}, \
                                         {rank: 3 host: \"testHost2\"}, \
                                         {rank: 4 host: \"testHost1\"}, \
                                         {rank: 5 host: \"testHost2\"}, \
                                         {rank: 6 host: \"testHost3\"}]");

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);

	auto route = psh->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID should receive the same routing
	route = psh->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Only events which have started routing should be in the table
	auto firstTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 1);
	BOOST_REQUIRE_EQUAL(firstTable[0].destination_rank, 2);

	// Arbitrary Sequence IDs are allowed
	route = psh->GetRouteForSequenceID(12343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 12343);

	// Out-of-order Sequence IDs are allowed
	route = psh->GetRouteForSequenceID(4, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 4);

	// Requests that arrive late still get the same info
	route = psh->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Check that things behave when tokens are exhausted...
	route = psh->GetRouteForSequenceID(10, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10);
	route = psh->GetRouteForSequenceID(11, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 11);

	route = psh->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, -1);

	psh->AddReceiverToken(1, 1);
	route = psh->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	route = psh->GetRouteForSequenceID(50, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	// Routing cache is sorted by sequence ID
	auto secondTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 4);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 10);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 11);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 12343);

	// Since the routing cache has been set to 2, only the highest two events routed are here, as the cache is checked when generating tables
	BOOST_REQUIRE_EQUAL(psh->GetCacheSize(), 2);
	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	BOOST_REQUIRE(psh->CacheHasRoute(50));
	BOOST_REQUIRE(!psh->CacheHasRoute(4));
	route = psh->GetRouteForSequenceID(50, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case RequestBasedEventBuilding END";
}

BOOST_AUTO_TEST_SUITE_END()
