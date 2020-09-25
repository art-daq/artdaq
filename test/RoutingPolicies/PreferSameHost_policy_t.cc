#define BOOST_TEST_MODULE PreferSameHost_policy_t
#include <boost/test/unit_test.hpp>

#include "artdaq/RoutingPolicies/makeRoutingManagerPolicy.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

BOOST_AUTO_TEST_SUITE(PreferSameHost_policy_t)

BOOST_AUTO_TEST_CASE(VerifyRMPSharedPtr)
{
	fhicl::ParameterSet ps4, ps3, ps2;
	fhicl::make_ParameterSet("receiver_ranks: [1,2,3,4]", ps4);
	fhicl::make_ParameterSet("receiver_ranks: [7,8,9]", ps3);
	fhicl::make_ParameterSet("receiver_ranks: [5,6]", ps2);

	auto rrA = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps4);
	BOOST_REQUIRE_EQUAL(rrA->GetReceiverCount(), 4);

	auto rrB = rrA;
	BOOST_REQUIRE_EQUAL(rrA->GetReceiverCount(), 4);
	BOOST_REQUIRE_EQUAL(rrB->GetReceiverCount(), 4);

	rrA = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps3);
	BOOST_REQUIRE_EQUAL(rrA->GetReceiverCount(), 3);
	BOOST_REQUIRE_EQUAL(rrB->GetReceiverCount(), 4);

	// force destructors to be run on first set of policies
	rrA = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps2);
	rrB = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps2);
}

BOOST_AUTO_TEST_CASE(Simple)
{
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("receiver_ranks: [1,2,3,4]", ps);

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 4);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(4, 1);
	psh->AddReceiverToken(2, 1);
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
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("receiver_ranks: [1,2,3,4] minimum_participants: 2", ps);

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 4);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(4, 1);
	psh->AddReceiverToken(2, 1);
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

BOOST_AUTO_TEST_CASE(LargeMinimumParticipants)
{
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("receiver_ranks: [1,2,3] minimum_participants: 5", ps);

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	auto secondTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 3);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 3);

	psh->AddReceiverToken(1, 1);
	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 3);
	BOOST_REQUIRE_EQUAL(thirdTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(thirdTable[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(thirdTable[2].destination_rank, 3);
}

BOOST_AUTO_TEST_CASE(ManyMissingParticipants)
{
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("receiver_ranks: [1,2,3] minimum_participants: -5", ps);

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	auto secondTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);

	psh->AddReceiverToken(1, 1);
	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 1);
}

BOOST_AUTO_TEST_CASE(DataFlowMode)
{
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet(
	    "receiver_ranks: [1,2,3] routing_manager_mode: DataFlow \
                              host_map: [{rank: 1 host: \"testHost1\"}, \
                                         {rank: 2 host: \"testHost1\"}, \
                                         {rank: 3 host: \"testHost2\"}, \
                                         {rank: 4 host: \"testHost1\"}, \
                                         {rank: 5 host: \"testHost2\"}, \
                                         {rank: 6 host: \"testHost3\"}]",
	    ps);

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
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
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet(
	    "receiver_ranks: [1,2,3] routing_manager_mode: RequestBasedEventBuilding routing_cache_size: 2 \
                              host_map: [{rank: 1 host: \"testHost1\"}, \
                                         {rank: 2 host: \"testHost1\"}, \
                                         {rank: 3 host: \"testHost2\"}, \
                                         {rank: 4 host: \"testHost1\"}, \
                                         {rank: 5 host: \"testHost2\"}, \
                                         {rank: 6 host: \"testHost3\"}]",
	    ps);

	auto psh = artdaq::makeRoutingManagerPolicy("PreferSameHost", ps);

	BOOST_REQUIRE_EQUAL(psh->GetReceiverCount(), 3);

	psh->Reset();
	psh->AddReceiverToken(1, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);
	psh->AddReceiverToken(3, 1);
	psh->AddReceiverToken(2, 1);

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
	BOOST_REQUIRE_EQUAL(secondTable.size(), 6);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 4);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 10);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 11);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(secondTable[5].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[5].sequence_id, 12343);

	// Since the routing cache has been set to 2, only the highest two events routed are here, as the cache is checked when generating tables
	auto thirdTable = psh->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 2);
	BOOST_REQUIRE_EQUAL(thirdTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(thirdTable[0].sequence_id, 50);
	BOOST_REQUIRE_EQUAL(thirdTable[1].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(thirdTable[1].sequence_id, 12343);

	TLOG(TLVL_INFO) << "RoundRobin_policy_t Test Case RequestBasedEventBuilding END";
}

BOOST_AUTO_TEST_SUITE_END()
