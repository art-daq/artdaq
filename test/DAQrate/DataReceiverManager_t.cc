#include "artdaq/DAQrate/DataReceiverManager.hh"

#define BOOST_TEST_MODULE DataReceiverManager_t
#include "artdaq/TransferPlugins/ShmemTransfer.hh"
#include "cetlib/quiet_unit_test.hpp"
#include "cetlib_except/exception.h"

#define TRACE_REQUIRE_EQUAL(l, r)                                                                                                \
	do                                                                                                                           \
	{                                                                                                                            \
		if (l == r)                                                                                                              \
		{                                                                                                                        \
			TLOG(TLVL_DEBUG) << __LINE__ << ": Checking if " << #l << " (" << l << ") equals " << #r << " (" << r << ")...YES!"; \
		}                                                                                                                        \
		else                                                                                                                     \
		{                                                                                                                        \
			TLOG(TLVL_ERROR) << __LINE__ << ": Checking if " << #l << " (" << l << ") equals " << #r << " (" << r << ")...NO!";  \
		}                                                                                                                        \
		BOOST_REQUIRE_EQUAL(l, r);                                                                                               \
	} while (0)

BOOST_AUTO_TEST_SUITE(DataReceiverManager_test)

BOOST_AUTO_TEST_CASE(Construct)
{
	artdaq::configureMessageFacility("DataReceiverManager_t", true, true);

	TLOG(TLVL_DEBUG) << "Test Case Construct BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);

	fhicl::ParameterSet source_fhicl;
	source_fhicl.put("transferPluginType", "Shmem");
	source_fhicl.put("destination_rank", 1);
	source_fhicl.put("source_rank", 0);

	fhicl::ParameterSet sources_fhicl;
	sources_fhicl.put("shmem", source_fhicl);
	pset.put("sources", sources_fhicl);
	auto shm = std::make_shared<artdaq::SharedMemoryEventManager>(pset, pset);
	artdaq::DataReceiverManager t(pset, shm);
	TLOG(TLVL_DEBUG) << "Test Case Construct END";
}

BOOST_AUTO_TEST_CASE(ReceiveData)
{
	artdaq::configureMessageFacility("DataReceiverManager_t", true, true);
	TLOG(TLVL_DEBUG) << "Test Case ReceiveData BEGIN";
	fhicl::ParameterSet pset;
	pset.put("use_art", false);
	pset.put("buffer_count", 2);
	pset.put("max_event_size_bytes", 1000);
	pset.put("expected_fragments_per_event", 2);

	fhicl::ParameterSet source_fhicl;
	source_fhicl.put("transferPluginType", "Shmem");
	source_fhicl.put("destination_rank", 1);
	source_fhicl.put("source_rank", 0);
	source_fhicl.put("shm_key", 0xFEEE0000 + getpid());

	fhicl::ParameterSet sources_fhicl;
	sources_fhicl.put("shmem", source_fhicl);
	pset.put("sources", sources_fhicl);
	auto shm = std::make_shared<artdaq::SharedMemoryEventManager>(pset, pset);
	shm->startRun(1);
	artdaq::DataReceiverManager t(pset, shm);
	{
		artdaq::ShmemTransfer transfer(source_fhicl, artdaq::TransferInterface::Role::kSend);
		TRACE_REQUIRE_EQUAL(t.getSharedMemoryEventManager().get(), shm.get());
		TRACE_REQUIRE_EQUAL(t.enabled_sources().size(), 1);
		TRACE_REQUIRE_EQUAL(t.running_sources().size(), 0);
		t.start_threads();
		TRACE_REQUIRE_EQUAL(t.enabled_sources().size(), 1);
		TRACE_REQUIRE_EQUAL(t.running_sources().size(), 1);

		artdaq::Fragment testFrag(10);
		testFrag.setSequenceID(1);
		testFrag.setFragmentID(0);
		testFrag.setTimestamp(0x100);
		testFrag.setSystemType(artdaq::Fragment::DataFragmentType);

		transfer.transfer_fragment_reliable_mode(std::move(testFrag));

		sleep(1);
		TRACE_REQUIRE_EQUAL(t.count(), 1);
		TRACE_REQUIRE_EQUAL(t.slotCount(0), 1);
		TRACE_REQUIRE_EQUAL(t.byteCount(), (10 + artdaq::detail::RawFragmentHeader::num_words()) * sizeof(artdaq::RawDataType));

		artdaq::FragmentPtr eodFrag = artdaq::Fragment::eodFrag(1);

		transfer.transfer_fragment_reliable_mode(std::move(*(eodFrag.get())));
	}
	sleep(2);
	TRACE_REQUIRE_EQUAL(t.count(), 1);
	TRACE_REQUIRE_EQUAL(t.slotCount(0), 1);
	TRACE_REQUIRE_EQUAL(t.enabled_sources().size(), 1);
	TRACE_REQUIRE_EQUAL(t.running_sources().size(), 0);
	TLOG(TLVL_DEBUG) << "Test Case ReceiveData END";
}

BOOST_AUTO_TEST_SUITE_END()