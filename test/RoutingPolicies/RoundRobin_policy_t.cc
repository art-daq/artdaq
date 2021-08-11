#define BOOST_TEST_MODULE RoundRobin_policy_t
#include <boost/test/unit_test.hpp>

#include "artdaq-utilities/Plugins/MakeParameterSet.hh"
#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"

BOOST_AUTO_TEST_SUITE(RoundRobin_policy_t)

BOOST_AUTO_TEST_CASE(VerifyRMPSharedPtr)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case VerifyRMPSharedPtr BEGIN";
	auto ps4 = artdaq::make_pset("receiver_ranks: [1,2,3,4]");
	auto ps3 = artdaq::make_pset("receiver_ranks: [7,8,9]");
	auto ps2 = artdaq::make_pset("receiver_ranks: [5,6]");

	auto rrA = artdaq::makeRoutingManagerPolicy("RoundRobin", ps4);

	auto rrB = rrA;
	BOOST_REQUIRE_EQUAL(rrA, rrB);

	rrA = artdaq::makeRoutingManagerPolicy("RoundRobin", ps3);
	BOOST_REQUIRE(rrA != rrB);

	// force destructors to be run on first set of policies
	rrB = artdaq::makeRoutingManagerPolicy("RoundRobin", ps2);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case VerifyRMPSharedPtr END";
}

BOOST_AUTO_TEST_CASE(Simple)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case Simple BEGIN";
	fhicl::ParameterSet ps = artdaq::make_pset("receiver_ranks: [1,2,3,4]");

	auto rr = artdaq::makeRoutingManagerPolicy("RoundRobin", ps);


	rr->Reset();
	rr->AddReceiverToken(1, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(4, 1);
	rr->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(rr->GetReceiverCount(), 4);
	auto secondTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 4);

	rr->AddReceiverToken(1, 0);

	auto thirdTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	rr->AddReceiverToken(1, 2);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(4, 2);
	auto fourthTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fourthTable.size(), 4);
	BOOST_REQUIRE_EQUAL(fourthTable[0].destination_rank, 1);

	rr->AddReceiverToken(3, 1);
	auto fifthTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fifthTable.size(), 4);
	BOOST_REQUIRE_EQUAL(fifthTable[0].destination_rank, 1);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case Simple END";
}

BOOST_AUTO_TEST_CASE(MinimumParticipants)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case MinimumParticipants BEGIN";
	fhicl::ParameterSet ps = artdaq::make_pset("receiver_ranks: [1,2,3,4] minimum_participants: 2");

	auto rr = artdaq::makeRoutingManagerPolicy("RoundRobin", ps);


	rr->Reset();
	rr->AddReceiverToken(1, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(4, 1);
	rr->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(rr->GetReceiverCount(), 4);
	auto secondTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 4);

	rr->AddReceiverToken(1, 0);

	auto thirdTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);

	rr->AddReceiverToken(1, 1);
	auto fourthTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fourthTable.size(), 2);

	BOOST_REQUIRE_EQUAL(fourthTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(fourthTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(fourthTable[0].sequence_id, 5);
	BOOST_REQUIRE_EQUAL(fourthTable[1].sequence_id, 6);

	rr->AddReceiverToken(1, 2);
	rr->AddReceiverToken(2, 2);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(4, 2);
	auto fifthTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(fifthTable.size(), 7);
	BOOST_REQUIRE_EQUAL(fifthTable[0].destination_rank, 1);

	rr->AddReceiverToken(3, 1);
	auto sixthTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(sixthTable.size(), 0);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case MinimumParticipants END";
}

// RECENT CHANGE IN BEHAVIOR! Since the number of receivers is now dynamic as tokens are added, RoundRobin_policy will ALWAYS wait for at least minimum_participants to be available when it is positive!
BOOST_AUTO_TEST_CASE(LargeMinimumParticipants)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case LargeMinimumParticipants BEGIN";
	fhicl::ParameterSet ps = artdaq::make_pset("receiver_ranks: [1,2,3] minimum_participants: 5");

	auto rr = artdaq::makeRoutingManagerPolicy("RoundRobin", ps);


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

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case LargeMinimumParticipants END";
}

BOOST_AUTO_TEST_CASE(ManyMissingParticipants)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case ManyMissingParticipants BEGIN";
	fhicl::ParameterSet ps = artdaq::make_pset("receiver_ranks: [1,2,3] minimum_participants: -5");

	auto rr = artdaq::makeRoutingManagerPolicy("RoundRobin", ps);


	rr->Reset();
	rr->AddReceiverToken(1, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(rr->GetReceiverCount(), 3);
	auto secondTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);

	rr->AddReceiverToken(1, 1);
	auto thirdTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 1);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case ManyMissingParticipants END";
}

BOOST_AUTO_TEST_CASE(DataFlowMode)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case DataFlowMode BEGIN";
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("routing_manager_mode: DataFlow", ps);

	auto rr = artdaq::makeRoutingManagerPolicy("RoundRobin", ps);


	rr->Reset();
	rr->AddReceiverToken(1, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(rr->GetReceiverCount(), 3);
	auto route = rr->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID are allowed, and should receive different information
	route = rr->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Except, that the same sequence ID from the same host should always get the same info
	route = rr->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	route = rr->GetRouteForSequenceID(2, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	route = rr->GetRouteForSequenceID(2, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, -1);

	rr->AddReceiverToken(1, 1);
	route = rr->GetRouteForSequenceID(2, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 2);

	// Out-of-order sequence IDs are allowed
	route = rr->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Arbitrary sequence IDs are allowed
	route = rr->GetRouteForSequenceID(10343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 10343);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case DataFlowMode END";
}

BOOST_AUTO_TEST_CASE(RequestBasedEventBuilding)
{
	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case RequestBasedEventBuilding BEGIN";
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("routing_manager_mode: RequestBasedEventBuilding routing_cache_size: 2", ps);

	auto rr = artdaq::makeRoutingManagerPolicy("RoundRobin", ps);


	rr->Reset();
	rr->AddReceiverToken(1, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	rr->AddReceiverToken(3, 1);
	rr->AddReceiverToken(2, 1);
	BOOST_REQUIRE_EQUAL(rr->GetReceiverCount(), 3);

	auto route = rr->GetRouteForSequenceID(1, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Multiple hits for the same sequence ID should receive the same routing
	route = rr->GetRouteForSequenceID(1, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Only events which have started routing should be in the table
	auto firstTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 1);
	BOOST_REQUIRE_EQUAL(firstTable[0].destination_rank, 1);

	// Arbitrary Sequence IDs are allowed
	route = rr->GetRouteForSequenceID(12343, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 2);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 12343);

	// Out-of-order Sequence IDs are allowed
	route = rr->GetRouteForSequenceID(4, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 3);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 4);

	// Requests that arrive late still get the same info
	route = rr->GetRouteForSequenceID(1, 6);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 1);

	// Check that things behave when tokens are exhausted...
	route = rr->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, -1);

	rr->AddReceiverToken(1, 1);
	route = rr->GetRouteForSequenceID(50, 4);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	route = rr->GetRouteForSequenceID(50, 5);
	BOOST_REQUIRE_EQUAL(route.destination_rank, 1);
	BOOST_REQUIRE_EQUAL(route.sequence_id, 50);

	// Routing cache is sorted by sequence ID
	auto secondTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 4);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 4);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 12343);

	// Since the routing cache has been set to 2, only the highest two events routed are here, as the cache is checked when generating tables
	auto thirdTable = rr->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 2);
	BOOST_REQUIRE_EQUAL(thirdTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(thirdTable[0].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(thirdTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(thirdTable[1].sequence_id, 12343);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case RequestBasedEventBuilding END";
}

BOOST_AUTO_TEST_SUITE_END()
