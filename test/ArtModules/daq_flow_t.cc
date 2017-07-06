#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/GenericFragmentSimulator.hh"
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "cetlib/exception.h"
#include "fhiclcpp/make_ParameterSet.h"
#include "proto/LoadParameterSet.hh"

#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

using artdaq::FragmentPtrs;
using artdaq::GenericFragmentSimulator;
using artdaq::SharedMemoryEventManager;
using std::size_t;


int main(int argc, char* argv[])
{
	auto pset = LoadParameterSet(argc, argv);
	artdaq::MPISentry mpiSentry(&argc, &argv);
	int rc = -1;
	try
	{
		size_t const NUM_FRAGS_PER_EVENT = 5;
		SharedMemoryEventManager::run_id_t const RUN_ID = 2112;
		size_t const NUM_EVENTS = 100;
		pset.put("fragment_count", NUM_FRAGS_PER_EVENT);
		pset.put("run_number", RUN_ID);
		pset.put("print_event_store_stats", true);
		pset.put("event_queue_wait_time", 10.0);
		pset.put("max_event_size_bytes", 0x100000);
		pset.put("shared_memory_key", 0xDA8F10E);
		pset.put("buffer_count", 10);

		auto temp = pset.to_string() + " source.waiting_time: 10 source.buffer_count: 10 source.shared_memory_key: 0xDA8F10E source.fragment_count: " + std::to_string(NUM_FRAGS_PER_EVENT);
		pset = fhicl::ParameterSet();
		fhicl::make_ParameterSet(temp, pset);
		// Eventually, this test should make a mixed-up streams of
		// Fragments; this has too clean a pattern to be an interesting
		// test of the EventStore's ability to deal with multiple events
		// simulatenously.
		GenericFragmentSimulator sim(pset);
		SharedMemoryEventManager events(pset, pset.to_string());
		events.startRun(RUN_ID);
		FragmentPtrs frags;
		size_t event_count = 0;
		while (frags.clear() , event_count++ < NUM_EVENTS && sim.getNext(frags))
		{
			LOG_DEBUG("main") << "Number of fragments: " << frags.size() << '\n';
			assert(frags.size() == NUM_FRAGS_PER_EVENT);
			for (auto&& frag : frags)
			{
				assert(frag != nullptr);
				artdaq::FragmentPtr tempFrag;
				auto sts = events.AddFragment(std::move(frag), 1000000, tempFrag);
				if (!sts)
				{
					TLOG_ERROR("daq_flow_t") << "Fragment was not added after 1s. Check art thread status!" << TLOG_ENDL;
					exit(1);
				}
			}
		}

		std::vector<int> readerReturnValues;
		bool endSucceeded = events.endOfData(readerReturnValues);
		if (endSucceeded)
		{
			rc = *std::max_element(readerReturnValues.begin(), readerReturnValues.end());
		}
		else
		{
			rc = 15;
		}
	}
	catch (cet::exception& x)
	{
		std::cerr << argv[0] << " failure\n" << x << std::endl;
		rc = 1;
	}
	catch (std::string& x)
	{
		std::cerr << argv[0] << " failure\n" << x << std::endl;
		rc = 2;
	}
	catch (char const* x)
	{
		std::cerr << argv[0] << " failure\n" << x << std::endl;
		rc = 3;
	}
	return rc;
}
