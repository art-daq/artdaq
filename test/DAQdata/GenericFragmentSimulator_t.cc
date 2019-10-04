#define BOOST_TEST_MODULE GenericFragmentSimulator_t
#include <boost/test/auto_unit_test.hpp>

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/detail/RawFragmentHeader.hh"
#include "artdaq/DAQdata/GenericFragmentSimulator.hh"
#include "fhiclcpp/ParameterSet.h"

#include <cstddef>

std::size_t const NUM_EVENTS = 2;
std::size_t const NUM_FRAGS_PER_EVENT = 5;
std::size_t const FRAGMENT_SIZE = 110;

BOOST_AUTO_TEST_SUITE(GenericFragmentSimulator_t)

BOOST_AUTO_TEST_CASE(Simple)
{
	fhicl::ParameterSet sim_config;
	sim_config.put("fragments_per_event", NUM_FRAGS_PER_EVENT);
	sim_config.put("want_random_payload_size", false);
	sim_config.put("payload_size", FRAGMENT_SIZE);
	artdaq::GenericFragmentSimulator sim(sim_config);
	artdaq::PostmarkedFragmentPtrs fragments;
	std::size_t num_events_seen = 0;
	while (fragments.clear(), num_events_seen < NUM_EVENTS && sim.getNext(fragments))
	{
		BOOST_REQUIRE_EQUAL(fragments.size(), NUM_FRAGS_PER_EVENT);
		for (auto&& pm_fragptr : fragments)
		{
			BOOST_CHECK(pm_fragptr.first.get());
			BOOST_CHECK_EQUAL(pm_fragptr.first->sequenceID(), num_events_seen + 1);
			BOOST_CHECK_EQUAL(pm_fragptr.first->size(), FRAGMENT_SIZE + artdaq::detail::RawFragmentHeader::num_words());
			BOOST_CHECK_EQUAL(pm_fragptr.first->dataSize(), FRAGMENT_SIZE);
		}
		++num_events_seen;
	}
	BOOST_REQUIRE_EQUAL(num_events_seen, NUM_EVENTS);
}

BOOST_AUTO_TEST_SUITE_END()
