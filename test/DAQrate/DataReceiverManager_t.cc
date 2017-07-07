#include "artdaq/DAQrate/DataReceiverManager.hh"

#define BOOST_TEST_MODULE(DataReceiverManager_t)
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"


BOOST_AUTO_TEST_SUITE(DataReceiverManager_test)

BOOST_AUTO_TEST_CASE(Construct) {
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("shared_memory_key", rand() % 0xBEE7BEE7);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("fragment_count", 2);
	auto shm = std::make_shared<artdaq::SharedMemoryEventManager>(pset, pset.to_string());
	artdaq::DataReceiverManager t(pset, shm);

}
BOOST_AUTO_TEST_SUITE_END()