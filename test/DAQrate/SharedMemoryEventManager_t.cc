
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"

#define BOOST_TEST_MODULE SharedMemoryEventManager_t
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"


BOOST_AUTO_TEST_SUITE(SharedMemoryEventManager_test)

BOOST_AUTO_TEST_CASE(Construct)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Construct BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset);

	BOOST_REQUIRE_EQUAL(t.runID(), 0);
	BOOST_REQUIRE_EQUAL(t.subrunID(), 0);
	BOOST_REQUIRE_EQUAL(t.GetLockedBufferCount(), 0);
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Construct END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(AddFragment)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test AddFragment BEGIN" << TLOG_ENDL;

	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	bool sts = t.AddFragment(std::move(frag), 1000000, tmpFrag);
	BOOST_REQUIRE_EQUAL(sts, true);
	BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
	TLOG_INFO("SharedMemoryEventManager_t") << "Test AddFragment END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(DataFlow)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test DataFlow BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 3);
	artdaq::SharedMemoryEventManager t(pset, pset);

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
	BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);

	frag->setFragmentID(1);
	hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
	auto fragLoc2 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
	BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 2);
	BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);

	frag->setFragmentID(2);
	hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
	auto fragLoc3 = t.WriteFragmentHeader(hdr);
	memcpy(fragLoc3, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
	t.DoneWritingFragment(hdr);
	BOOST_REQUIRE_EQUAL(fragLoc2 + frag->size(), fragLoc3);
	BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
	BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 1);

	TLOG_INFO("SharedMemoryEventManager_t") << "Test DataFlow END" << TLOG_ENDL;
}

/*
// Need to check the following scenarios:
// 1. Active buffer with lower sequence id than a completed buffer (b. timeout case)
// 2a. Inactive buffer with lower sequence id than a completed buffer (b. timeout case)
// 2c. Inactive buffer times out and then data arrives (Error case)
BOOST_AUTO_TEST_CASE(Ordering_IncompleteActiveBuffer)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_IncompleteActiveBuffer BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 20);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	artdaq::SharedMemoryEventManager t(pset, pset);
	{

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}

	{
		frag->setSequenceID(1);
		frag->setFragmentID(1);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 3);
	}
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_IncompleteActiveBuffer END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(Ordering_IncompleteActiveBuffer_Timeout)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_IncompleteActiveBuffer_Timeout BEGIN" << TLOG_ENDL;
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
	{
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);

	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}

	sleep(1);

	{
		frag->setSequenceID(4);
		frag->setFragmentID(0);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 3);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 4);
	}
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_IncompleteActiveBuffer_Timeout END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(Ordering_InactiveBuffer)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_InactiveBuffer BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 20);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);

	artdaq::FragmentPtr frag(new artdaq::Fragment(1, 0, artdaq::Fragment::FirstUserFragmentType, 0UL)), tmpFrag;
	frag->resize(4);
	for (auto ii = 0; ii < 4; ++ii)
	{
		*(frag->dataBegin() + ii) = ii;
	}

	artdaq::SharedMemoryEventManager t(pset, pset);
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(3), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}

	{
		frag->setSequenceID(1);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		frag->setFragmentID(1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 3);
	}
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_InactiveBuffer END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_CASE(Ordering_InactiveBuffer_Timeout)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_InactiveBuffer_Timeout BEGIN" << TLOG_ENDL;
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
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);

	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(3), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}

	sleep(1);

	{
		frag->setSequenceID(4);
		frag->setFragmentID(1);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 2);
	}
	TLOG_INFO("SharedMemoryEventManager_t") << "Test Ordering_InactiveBuffer_Timeout END" << TLOG_ENDL;
}
*/
//SharedMemoryEventManager should print error messages, but consume data for buffers which have timed out
BOOST_AUTO_TEST_CASE(ConsumeDroppedData_Active)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test ConsumeDroppedData_Active BEGIN" << TLOG_ENDL;
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
	{

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(1), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 2);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);

	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 2);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(3), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}

	sleep(1);

	{
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
		frag->setSequenceID(4);
		frag->setFragmentID(1);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 3);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 4);
	}
	{
		frag->setSequenceID(1);
		frag->setFragmentID(1);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
#if !ART_SUPPORTS_DUPLICATE_EVENTS
		BOOST_REQUIRE_EQUAL(fragLoc2, t.GetDroppedDataAddress());
#endif
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		//BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
#if ART_SUPPORTS_DUPLICATE_EVENTS
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 5);
#else
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 4);
#endif
	}

	TLOG_INFO("SharedMemoryEventManager_t") << "Test ConsumeDroppedData_Active END" << TLOG_ENDL;
}
/*
//SharedMemoryEventManager should print error messages, but consume data for buffers which have timed out
BOOST_AUTO_TEST_CASE(ConsumeDroppedData_Inactive)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test ConsumeDroppedData_Inactive BEGIN" << TLOG_ENDL;
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
	{
		frag->setSequenceID(2);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(2), 2);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
		BOOST_REQUIRE_EQUAL(fragLoc + frag->size(), fragLoc2);

	}
	{
		frag->setSequenceID(3);
		frag->setFragmentID(0);

		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetFragmentCount(3), 1);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(),1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 2);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
	}

	sleep(1);

	{
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 0);
		frag->setSequenceID(4);
		frag->setFragmentID(1);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 1);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 2);

		frag->setFragmentID(1);
		hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 3);
	}
	{
		frag->setSequenceID(1);
		frag->setFragmentID(1);
		auto hdr = *reinterpret_cast<artdaq::detail::RawFragmentHeader*>(frag->headerAddress());
		auto fragLoc2 = t.WriteFragmentHeader(hdr);
		BOOST_REQUIRE_EQUAL(fragLoc2, t.GetDroppedDataAddress());
		memcpy(fragLoc2, frag->dataBegin(), 4 * sizeof(artdaq::RawDataType));
		t.DoneWritingFragment(hdr);
		BOOST_REQUIRE_EQUAL(t.GetPendingEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetInactiveEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetIncompleteEventCount(), 0);
		BOOST_REQUIRE_EQUAL(t.GetArtEventCount(), 3);
	}
	TLOG_INFO("SharedMemoryEventManager_t") << "Test ConsumeDroppedData_Inactive END" << TLOG_ENDL;
}
*/

BOOST_AUTO_TEST_CASE(RunNumbers)
{
	TLOG_INFO("SharedMemoryEventManager_t") << "Test RunNumbers BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);
	artdaq::SharedMemoryEventManager t(pset, pset);

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


	artdaq::SharedMemoryEventReceiver r(t.GetKey(), t.GetBroadcastKey());
	t.endSubrun();
	bool errflag = false;
	bool sts = r.ReadyForRead();
	BOOST_REQUIRE_EQUAL(sts, true);
	auto hdr = r.ReadHeader(errflag);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE(hdr != nullptr);
	BOOST_REQUIRE_EQUAL(hdr->is_complete, true);
	BOOST_REQUIRE_EQUAL(hdr->run_id, 3);
	BOOST_REQUIRE_EQUAL(hdr->subrun_id, 1);
	auto frags = r.GetFragmentsByType(errflag, artdaq::Fragment::EndOfSubrunFragmentType);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(frags->size(), 1);
	r.ReleaseBuffer();

	t.endRun();
	sts = r.ReadyForRead();
	BOOST_REQUIRE_EQUAL(sts, true);
	hdr = r.ReadHeader(errflag);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE(hdr != nullptr);
	BOOST_REQUIRE_EQUAL(hdr->is_complete, true);
	BOOST_REQUIRE_EQUAL(hdr->run_id, 3);
	BOOST_REQUIRE_EQUAL(hdr->subrun_id, 1);
	frags = r.GetFragmentsByType(errflag, artdaq::Fragment::EndOfRunFragmentType);
	BOOST_REQUIRE_EQUAL(errflag, false);
	BOOST_REQUIRE_EQUAL(frags->size(), 1);
	r.ReleaseBuffer();

	TLOG_INFO("SharedMemoryEventManager_t") << "Test RunNumbers END" << TLOG_ENDL;
}

BOOST_AUTO_TEST_SUITE_END()
