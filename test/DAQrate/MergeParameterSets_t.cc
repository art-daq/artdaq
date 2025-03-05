#include "artdaq/DAQrate/detail/MergeParameterSets.hh"

#define BOOST_TEST_MODULE MergeParameterSets_t
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(MergeParameterSets_test)

BOOST_AUTO_TEST_CASE(Simple)
{
	fhicl::ParameterSet p1, p2;
	p1.put<std::string>("test_str", "test1");
	p1.put<int>("test_int", 1);

	p2.put<std::string>("another_str", "test2");
	p2.put<bool>("test_bool", false);

	auto p3 = artdaq::merge(p1, p2);

	BOOST_REQUIRE(p3.has_key("test_str"));
	BOOST_REQUIRE(p3.has_key("another_str"));

	BOOST_REQUIRE_EQUAL(p3.get<int>("test_int"), 1);
	BOOST_REQUIRE_EQUAL(p3.get<bool>("test_bool"), false);
}

BOOST_AUTO_TEST_CASE(Overwrite)
{
	fhicl::ParameterSet p1, p2;
	p1.put<std::string>("test_str", "test1");
	p1.put<int>("test_int", 1);

	p2.put<std::string>("test_str", "test2");
	p2.put<bool>("test_bool", false);

	auto p3 = artdaq::merge(p1, p2);

	BOOST_REQUIRE(p3.has_key("test_str"));

	BOOST_REQUIRE_EQUAL(p3.get<std::string>("test_str"), "test2");
	BOOST_REQUIRE_EQUAL(p3.get<int>("test_int"), 1);
	BOOST_REQUIRE_EQUAL(p3.get<bool>("test_bool"), false);
}

BOOST_AUTO_TEST_SUITE_END()
