#define TRACE_NAME "SharedMemoryEventManager_t"

#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"

#define BOOST_TEST_MODULE SharedMemoryEventManager_t
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"

#include <thread>

BOOST_AUTO_TEST_SUITE(SharedMemoryEventManager_test)

artdaq::detail::RawFragmentHeader GetHeader(artdaq::FragmentPtr const& frag)
{
	return *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

BOOST_AUTO_TEST_CASE(Construct)
{
	TLOG(TLVL_INFO) << "Test Construct BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset);

	BOOST_REQUIRE_EQUAL(t.runID(), 0);
	BOOST_REQUIRE_EQUAL(t.GetSubrunForSequenceID(1), 1);
	BOOST_REQUIRE_EQUAL(t.GetLockedBufferCount(), 0);
	t.startRun(1);
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	TLOG(TLVL_INFO) << "Test Construct END";
}

BOOST_AUTO_TEST_CASE(AddFragment)
{
	TLOG(TLVL_INFO) << "Test AddFragment BEGIN";

	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset);
	t.startRun(1);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	bool sts = t.AddFragment(std::move(frag), 1000000, tmpFrag);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
	TLOG(TLVL_INFO) << "Test AddFragment END";
}

BOOST_AUTO_TEST_CASE(DataFlow)
{
	TLOG(TLVL_INFO) << "Test DataFlow BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 3);
	artdaq::SharedMemoryEventManager t(pset, pset);
	t.startRun(1);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	auto hdr = GetHeader(frag);
	auto fragLoc = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);

	frag->setFragmentID(1);
	hdr = GetHeader(frag);
	auto fragLoc2 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 2);
	BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

	frag->setFragmentID(2);
	hdr = GetHeader(frag);
	auto fragLoc3 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc3, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc2 + frag->size(), fragLoc3);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

	TLOG(TLVL_INFO) << "Test DataFlow END";
}

BOOST_AUTO_TEST_CASE(EndOfData)
{
	TLOG(TLVL_INFO) << "Test EndOfData BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 1);
	artdaq::SharedMemoryEventManager t(pset, pset);
	t.startRun(1);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	auto hdr = GetHeader(frag);
	auto fragLoc = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

	t.endOfData();

	t.startRun(2);

	frag->setSequenceID(1);
	hdr = GetHeader(frag);
	auto fragLoc3 = t.WriteFragmentHeader(hdr);

	std::atomic<bool> endComplete = false;
	std::thread thread{
	    [&] { endComplete = t.endOfData(); }};

	memcpy(fragLoc3, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	usleep(100000);
	BOOST_REQUIRE_EQUAL(endComplete.load(), false);
	t.DoneWritingFragment(hdr);
	thread.join();
	BOOST_REQUIRE_EQUAL(endComplete.load(), true);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

	frag->setSequenceID(2);
	hdr = GetHeader(frag);
	auto fragLoc2 = t.WriteFragmentHeader(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc2, nullptr);

	TLOG(TLVL_INFO) << "Test EndOfData END";
}

BOOST_AUTO_TEST_CASE(TooManyFragments_InterleavedWrites)
{
	TLOG(TLVL_INFO) << "Test TooManyFragments_InterleavedWrites BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 3);
	pset.put("stale_buffer_timeout_usec", 100000);
	artdaq::SharedMemoryEventManager t(pset, pset);
	t.startRun(1);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	auto hdr = GetHeader(frag);
	auto fragLoc = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);

	frag->setFragmentID(1);
	hdr = GetHeader(frag);
	auto fragLoc2 = t.WriteFragmentHeader(hdr);
	frag->setFragmentID(2);
	hdr = GetHeader(frag);
	auto fragLoc3 = t.WriteFragmentHeader(hdr);
	frag->setFragmentID(3);
	hdr = GetHeader(frag);
	auto fragLoc4 = t.WriteFragmentHeader(hdr);
	frag->setFragmentID(4);
	hdr = GetHeader(frag);
	auto fragLoc5 = t.WriteFragmentHeader(hdr);

	memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

	memcpy(fragLoc3, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc2 + frag->size(), fragLoc3);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

	memcpy(fragLoc4, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

	memcpy(fragLoc5, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc4 + frag->size(), fragLoc5);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

	usleep(1000000);
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = GetHeader(frag);
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);
	}
	TLOG(TLVL_INFO) << "Test TooManyFragments_InterleavedWrites END";
}

BOOST_AUTO_TEST_CASE(TooManyFragments_DiscreteWrites)
{
	TLOG(TLVL_INFO) << "Test TooManyFragments_DiscreteWrites BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 3);
	pset.put("stale_buffer_timeout_usec", 100000);
	artdaq::SharedMemoryEventManager t(pset, pset);
	t.startRun(1);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	auto hdr = GetHeader(frag);
	auto fragLoc = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);

	frag->setFragmentID(1);
	hdr = GetHeader(frag);
	auto fragLoc2 = t.WriteFragmentHeader(hdr);

	memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

	frag->setFragmentID(2);
	hdr = GetHeader(frag);
	auto fragLoc3 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc3, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc2 + frag->size(), fragLoc3);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

	frag->setFragmentID(3);
	hdr = GetHeader(frag);
	auto fragLoc4 = t.WriteFragmentHeader(hdr);
#if !ART_SUPPORTS_DUPLICATE_EVENTS
	BOOST_REQUIRE_EQUAL(fragLoc4, t.GetDroppedDataAddress(hdr));
#endif
	memcpy(fragLoc4, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
#if ART_SUPPORTS_DUPLICATE_EVENTS
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);
#else
	BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);
#endif

	usleep(1000000);
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = GetHeader(frag);
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);
	}
	TLOG(TLVL_INFO) << "Test TooManyFragments_DiscreteWrites END";
}

// SharedMemoryEventManager should print error messages, but consume data for buffers which have timed out
BOOST_AUTO_TEST_CASE(ConsumeDroppedData_Active)
{
	TLOG(TLVL_INFO) << "Test ConsumeDroppedData_Active BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 20);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	pset.put("stale_buffer_timeout_usec", 100000);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	artdaq::SharedMemoryEventManager t(pset, pset);
	t.startRun(1);
	{
		auto hdr = GetHeader(frag);
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = GetHeader(frag);
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 2);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = GetHeader(frag);
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		// BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = GetHeader(frag);
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 2);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(3), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

		frag->setFragmentID(1);
		hdr = GetHeader(frag);
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 2);
	}

	sleep(1);

	{
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 2);
		frag->setSequenceID(4);
		frag->setFragmentID(1);
		auto hdr = GetHeader(frag);
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount() + t.GetArtEventCount(), 3);

		frag->setFragmentID(1);
		hdr = GetHeader(frag);
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount() + t.GetArtEventCount(), 4);
	}
	{
		frag->setSequenceID(1);
		frag->setFragmentID(1);
		auto hdr = GetHeader(frag);
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
#if !ART_SUPPORTS_DUPLICATE_EVENTS
		BOOST_REQUIRE_EQUAL(fragLoc2, t.GetDroppedDataAddress(hdr));
#endif
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		// BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetOpenEventCount(), 0);
#if ART_SUPPORTS_DUPLICATE_EVENTS
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 5);
#else
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 4);
#endif
	}

	TLOG(TLVL_INFO) << "Test ConsumeDroppedData_Active END";
}

BOOST_AUTO_TEST_CASE(RunNumbers)
{
	TLOG(TLVL_INFO) << "Test RunNumbers BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset);

	t.startRun(1);
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	BOOST_REQUIRE_EQUAL(t.GetCurrentSubrun(), 1);
	t.rolloverSubrun();
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	BOOST_REQUIRE_EQUAL(t.GetCurrentSubrun(), 2);
	t.rolloverSubrun();
	BOOST_REQUIRE_EQUAL(t.runID(), 1);
	BOOST_REQUIRE_EQUAL(t.GetCurrentSubrun(), 3);

	// Check repeated requests for same subrun
	t.rolloverSubrun(3, 4);
	t.rolloverSubrun(4, 4);
	t.rolloverSubrun(5, 4);

	t.startRun(3);
	BOOST_REQUIRE_EQUAL(t.runID(), 3);
	BOOST_REQUIRE_EQUAL(t.GetCurrentSubrun(), 1);

	artdaq::SharedMemoryEventReceiver r(t.GetKey(), t.GetBroadcastKey());
	bool errflag = false;

	t.endRun();
	bool sts = r.ReadyForRead();
	BOOST_REQUIRE_EQUAL(sts, true);
	auto hdr = r.ReadHeader(errflag);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE(hdr != nullptr);
	if (hdr != nullptr)
	{  // Make static analyzer happy
		BOOST_REQUIRE_EQUAL(hdr->is_complete, true);
		BOOST_REQUIRE_EQUAL(hdr->run_id, 3);
		BOOST_REQUIRE_EQUAL(hdr->subrun_id, 1);
	}
	auto frags = r.GetFragmentsByType(errflag, artdaq::Fragment::EndOfRunFragmentType);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(frags->size(), 1);
	r.ReleaseBuffer();

	TLOG(TLVL_INFO) << "Test RunNumbers END";
}

BOOST_AUTO_TEST_SUITE_END()
