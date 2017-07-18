
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"

#define BOOST_TEST_MODULE(SharedMemoryEventManager_t)
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"


BOOST_AUTO_TEST_SUITE(SharedMemoryEventManager_test)

BOOST_AUTO_TEST_CASE(Construct) {
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t( pset, pset.to_string());

	BOOST_REQUIRE_EQUAL(t.runID(), 0);
	BOOST_REQUIRE_EQUAL(t.subrunID(), 0);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(0), 0);
}

BOOST_AUTO_TEST_CASE(AddFragment) {

	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset.to_string());

	artdaq::FragmentPtr frag(new artdaq::Fragment(1,0,artdaq::Fragment::FirstUserFragmentType,0UL)), tmpFrag;
	frag->resize(4);
	for(auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	t.AddFragment(std::move(frag), 1000000, tmpFrag);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(0), 1);
}

BOOST_AUTO_TEST_CASE(DataFlow) {
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 3);
	artdaq::SharedMemoryEventManager t(pset, pset.to_string());

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
	auto fragLoc = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(0), 1);

	frag->setFragmentID(1);
	hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
	auto fragLoc2 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(0), 2);
	BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);

	frag->setFragmentID(2);
	hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
	auto fragLoc3 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc3, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc2 + frag->size(), fragLoc3);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);


}

BOOST_AUTO_TEST_CASE(RunNumbers)
{
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t( pset, pset.to_string());

	t.startRun(1);
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	BOOST_REQUIRE_EQUAL(t.subrunID(), 1);
	t.startSubrun();
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	BOOST_REQUIRE_EQUAL(t.subrunID(), 2);
	t.startSubrun();
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	BOOST_REQUIRE_EQUAL(t.subrunID(), 3);
	t.startRun(3);
	BOOST_REQUIRE_EQUAL(t.runID(), 3);
	BOOST_REQUIRE_EQUAL(t.subrunID(), 1);
	

	artdaq::SharedMemoryEventReceiver r(t.GetKey());
	t.endSubrun();
	bool errflag = false;
	auto hdr = r.ReadHeader(errflag);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(hdr->is_complete, true);
	BOOST_REQUIRE_EQUAL(hdr->run_id, 3);
	BOOST_REQUIRE_EQUAL(hdr->subrun_id, 1);
	auto frags = r.GetFragmentsByType(errflag, artdaq::Fragment::EndOfSubrunFragmentType);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(frags->size(), 1);
	r.ReleaseBuffer();

	t.endRun();
	hdr = r.ReadHeader(errflag);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(hdr->is_complete, true);
	BOOST_REQUIRE_EQUAL(hdr->run_id, 3);
	BOOST_REQUIRE_EQUAL(hdr->subrun_id, 1);
	frags = r.GetFragmentsByType(errflag, artdaq::Fragment::EndOfRunFragmentType);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(frags->size(), 1);
	r.ReleaseBuffer();

}

BOOST_AUTO_TEST_SUITE_END()