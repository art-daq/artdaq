#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/GenericFragmentSimulator.hh"
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "cetlib/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"
#include "proto/LoadParameterSet.hh"

#include <cstddef>
#include <iostream>
#include <string>
#include <vector>

using artdaq::SharedMemoryEventManager;
using artdaq::FragmentPtrs;
using artdaq::GenericFragmentSimulator;
using fhicl::ParameterSet;
using std::size_t;


int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("reconfigure_t");
	auto pset = LoadParameterSet(argc, argv);
	artdaq::MPISentry mpiSentry(&argc, &argv);
	int rc = -1;
	try
	{
		size_t const NUM_FRAGS_PER_EVENT = 5;
		SharedMemoryEventManager::run_id_t const RUN_ID = 2112;
		size_t const NUM_EVENTS = 100;
		// We may want to add ParameterSet parsing to this code, but right
		// now this will do...
		pset.put("fragment_count", NUM_FRAGS_PER_EVENT);
		pset.put("run_number", RUN_ID);
		pset.put("print_event_store_stats", true);
		pset.put("max_event_size_bytes", 0x100000);
		pset.put("shared_memory_key", 0x4EC0F16);
		pset.put("buffer_count",10);

		auto temp = pset.to_string() + " source.waiting_time: 10 source.buffer_count: 10 source.shared_memory_key: 0x4EC0F16 source.fragment_count: " + std::to_string(NUM_FRAGS_PER_EVENT);
		pset = fhicl::ParameterSet();
		fhicl::make_ParameterSet(temp, pset);
		// Eventually, this test should make a mixed-up streams of
		// Fragments; this has too clean a pattern to be an interesting
		// test of the EventStore's ability to deal with multiple events
		// simulatenously.
		GenericFragmentSimulator sim(pset);
		std::unique_ptr<SharedMemoryEventManager> events(new SharedMemoryEventManager(pset, pset.to_string()));
		events->startRun(100);
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
				auto sts = events->AddFragment(std::move(frag), 1000000, tempFrag);
				if (!sts)
				{
					TLOG_ERROR("reconfigure_t") << "Fragment was not added after 1s. Check art thread status!" << TLOG_ENDL;
					exit(1);
				}
			}
		}

		std::cout << "Ending first run..." << std::endl;
		std::vector<int> readerReturnValues;
		bool endSucceeded = events->endOfData(readerReturnValues);
		if (endSucceeded)
		{
			rc = *std::max_element(readerReturnValues.begin(), readerReturnValues.end());

			size_t const NUM_EVENTS2 = 200;
			auto temp = pset.to_string() + " physics.analyzers.frags.num_events_expected: " + std::to_string(NUM_EVENTS2);
			fhicl::ParameterSet sim_config2;
			fhicl::make_ParameterSet(temp, sim_config2);
			GenericFragmentSimulator sim2(sim_config2);
			events->ReconfigureArt(temp);
			event_count = 0;
			while (frags.clear() , event_count++ < NUM_EVENTS2 && sim2.getNext(frags))
			{
				LOG_DEBUG("main") << "Number of fragments: " << frags.size() << '\n';
				assert(frags.size() == NUM_FRAGS_PER_EVENT);
				for (auto&& frag : frags)
				{
					assert(frag != nullptr);
					artdaq::FragmentPtr tempFrag;
					auto sts = events->AddFragment(std::move(frag), 1000000, tempFrag);
					if (!sts)
					{
						TLOG_ERROR("reconfigure_t") << "Fragment was not added after 1s. Check art thread status!" << TLOG_ENDL;
						exit(1);
					}
				}
			}

			bool endSucceeded2 = events->endOfData(readerReturnValues);
			if (endSucceeded2)
			{
				rc = *std::max_element(readerReturnValues.begin(), readerReturnValues.end());
			}
			else
			{
				rc = 16;
			}
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
