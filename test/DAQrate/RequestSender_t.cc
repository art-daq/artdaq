#include "artdaq/DAQrate/RequestSender.hh"

#define BOOST_TEST_MODULE(RequestSender_t)
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"


BOOST_AUTO_TEST_SUITE(RequestSender_test)

BOOST_AUTO_TEST_CASE(Construct) {
	fhicl::ParameterSet pset;
	artdaq::RequestSender t(pset);

}
BOOST_AUTO_TEST_SUITE_END()