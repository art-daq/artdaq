#define BOOST_TEST_MODULE ( NoOp_policy_t )
#include "boost/test/auto_unit_test.hpp"

#include "artdaq/Application/Routing/makeRoutingMasterPolicy.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"


BOOST_AUTO_TEST_SUITE(NoOp_policy_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet("event_builder_buffer_count: 10 event_builder_ranks: [1,2,3,4]", ps);

	auto noop = artdaq::makeRoutingMasterPolicy("NoOp", ps);

	BOOST_REQUIRE_EQUAL(noop->GetEventBuilderCount(), 4);

	auto firstTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(firstTable.size(), 40);
	BOOST_REQUIRE_EQUAL(firstTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(firstTable[0].sequence_id, 0);
	BOOST_REQUIRE_EQUAL(firstTable[firstTable.size() - 1].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(firstTable[firstTable.size() - 1].sequence_id, 39);

	noop->Reset();
	noop->AddEventBuilderToken(1,1);
	noop->AddEventBuilderToken(3, 1);
	noop->AddEventBuilderToken(2, 1);
	noop->AddEventBuilderToken(4, 1);
	noop->AddEventBuilderToken(2, 1);
	auto secondTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(secondTable.size(), 5);
	BOOST_REQUIRE_EQUAL(secondTable[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(secondTable[1].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(secondTable[2].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[3].destination_rank, 4);
	BOOST_REQUIRE_EQUAL(secondTable[4].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(secondTable[0].sequence_id, 0);
	BOOST_REQUIRE_EQUAL(secondTable[1].sequence_id, 1);
	BOOST_REQUIRE_EQUAL(secondTable[2].sequence_id, 2);
	BOOST_REQUIRE_EQUAL(secondTable[3].sequence_id, 3);
	BOOST_REQUIRE_EQUAL(secondTable[4].sequence_id, 4);

	noop->AddEventBuilderToken(1, 0);

	auto thirdTable = noop->GetCurrentTable();
	BOOST_REQUIRE_EQUAL(thirdTable.size(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
