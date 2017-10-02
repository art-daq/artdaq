
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/ArtModules/detail/RawEventQueueReader.hh"
#include "artdaq-core/Data/ContainerFragment.hh"

#include "art/Framework/IO/Sources/put_product_in_principal.h"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "canvas/Utilities/Exception.h"
#include "artdaq-core/Data/Fragment.hh"
#include <sys/time.h>
#define TRACE_NAME "RawEventQueueReader"
#include "trace.h"

using std::string;

artdaq::detail::RawEventQueueReader::RawEventQueueReader(fhicl::ParameterSet const& ps,
														 art::ProductRegistryHelper& help,
														 art::SourceHelper const& pm) :
	pmaker(pm)
	, incoming_events(getGlobalQueue())
	, waiting_time(ps.get<double>("waiting_time", 86400.0))
	, resume_after_timeout(ps.get<bool>("resume_after_timeout", true))
	, pretend_module_name(ps.get<std::string>("raw_data_label","daq"))
	, unidentified_instance_name("unidentified")
	, shutdownMsgReceived(false)
	, outputFileCloseNeeded(false)
	, bytesRead(0)
	, fragment_type_map_(Fragment::MakeSystemTypeMap())
	, readNext_calls_(0)
{
	help.reconstitutes<Fragments, art::InEvent>(pretend_module_name,
												unidentified_instance_name);
		help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, "Container_Empty");
	for (auto it = fragment_type_map_.begin(); it != fragment_type_map_.end(); ++it)
	{
		help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, it->second);
	}
}

void artdaq::detail::RawEventQueueReader::closeCurrentFile() {}

void artdaq::detail::RawEventQueueReader::readFile(string const&,
												   art::FileBlock*& fb)
{
	fb = new art::FileBlock(art::FileFormatVersion(1, "RawEvent2011"), "nothing");
}

bool artdaq::detail::RawEventQueueReader::readNext(art::RunPrincipal* const & inR,
												   art::SubRunPrincipal* const & inSR,
												   art::RunPrincipal*& outR,
												   art::SubRunPrincipal*& outSR,
												   art::EventPrincipal*& outE)
{
	/*if (outputFileCloseNeeded) {
		outputFileCloseNeeded = false;
		return false;
	}*/
	if (readNext_calls_++ == 0)
	{
		incoming_events.setReaderIsReady();
		TRACE(50, "RawEventQueueReader::readNext after incoming_events.setReaderIsReady()");
	}
	// Establish default 'results'
	outR = 0;
	outSR = 0;
	outE = 0;
	RawEvent_ptr popped_event;
	// Try to get an event from the queue. We'll continuously loop, either until:
	//   1) we have read a RawEvent off the queue, or
	//   2) we have timed out, AND we are told the when we timeout we
	//      should stop.
	// In any case, if we time out, we emit an informational message.
	bool keep_looping = true;
	bool got_event = false;
	while (keep_looping)
	{
		keep_looping = false;
		got_event = incoming_events.deqTimedWait(popped_event, waiting_time);
		if (!got_event)
		{
			TLOG_INFO("RawEventQueueReader")
				<< "InputFailure: Reading timed out in RawEventQueueReader::readNext()" << TLOG_ENDL;
			keep_looping = resume_after_timeout;
		}
	}
	// We return false, indicating we're done reading, if:
	//   1) we did not obtain an event, because we timed out and were
	//      configured NOT to keep trying after a timeout, or
	//   2) the event we read was the end-of-data marker: a null
	//      pointer
	if (!got_event || !popped_event)
	{
		TLOG_DEBUG("RawEventQueueReader") << "Received shutdown message, returning false" << TLOG_ENDL;
		shutdownMsgReceived = true;
		return false;
	}

	size_t qsize=incoming_events.size(); // save the qsize at this point

	// Check the number of fragments in the RawEvent.  If we have a single
	// fragment and that fragment is marked as EndRun or EndSubrun we'll create
	// the special principals for that.
	art::Timestamp currentTime = time(0);

	// make new run if inR is 0 or if the run has changed
	if (inR == 0 || inR->run() != popped_event->runID())
	{
		outR = pmaker.makeRunPrincipal(popped_event->runID(),
									   currentTime);
	}

	if (popped_event->numFragments() == 1)
	{
		if (popped_event->releaseProduct(Fragment::EndOfRunFragmentType)->size() == 1)
		{
			art::EventID const evid(art::EventID::flushEvent());
			outR = pmaker.makeRunPrincipal(evid.runID(), currentTime);
			outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
			outE = pmaker.makeEventPrincipal(evid, currentTime);
			return true;
		}
		else if (popped_event->releaseProduct(Fragment::EndOfSubrunFragmentType)->size() == 1)
		{
			// Check if inR == 0 or is a new run
			if (inR == 0 || inR->run() != popped_event->runID())
			{
				outSR = pmaker.makeSubRunPrincipal(popped_event->runID(),
												   popped_event->subrunID(),
												   currentTime);
#ifdef ARTDAQ_ART_EVENTID_HAS_EXPLICIT_RUNID /* Old, error-prone interface. */
				art::EventID const evid(art::EventID::flushEvent(outR->id(), outSR->id()));
#else
				art::EventID const evid(art::EventID::flushEvent(outSR->id()));
#endif
				outE = pmaker.makeEventPrincipal(evid, currentTime);
			}
			else
			{
				// If the previous subrun was neither 0 nor flush and was identical with the current
				// subrun, then it must have been associated with a data event.  In that case, we need
				// to generate a flush event with a valid run but flush subrun and event number in order
				// to end the subrun.
				if (inSR != 0 && !inSR->id().isFlush() && inSR->subRun() == popped_event->subrunID())
				{
					art::EventID const evid(art::EventID::flushEvent(inR->id()));
					outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
					outE = pmaker.makeEventPrincipal(evid, currentTime);
					// If this is either a new or another empty subrun, then generate a flush event with
					// valid run and subrun numbers but flush event number
					//} else if(inSR==0 || inSR->id().isFlush()){
				}
				else
				{
					outSR = pmaker.makeSubRunPrincipal(popped_event->runID(),
													   popped_event->subrunID(),
													   currentTime);
#ifdef ARTDAQ_ART_EVENTID_HAS_EXPLICIT_RUNID /* Old, error-prone interface. */
					art::EventID const evid(art::EventID::flushEvent(inR->id(), outSR->id()));
#else
					art::EventID const evid(art::EventID::flushEvent(outSR->id()));
#endif
					outE = pmaker.makeEventPrincipal(evid, currentTime);
					// Possible error condition
					//} else {
				}
				outR = 0;
			}
			//outputFileCloseNeeded = true;
			return true;
		}
	}

	// make new subrun if inSR is 0 or if the subrun has changed
	art::SubRunID subrun_check(popped_event->runID(), popped_event->subrunID());
	if (inSR == 0 || subrun_check != inSR->id())
	{
		outSR = pmaker.makeSubRunPrincipal(popped_event->runID(),
										   popped_event->subrunID(),
										   currentTime);
	}
	outE = pmaker.makeEventPrincipal(popped_event->runID(),
									 popped_event->subrunID(),
									 popped_event->sequenceID(),
									 currentTime);
	// get the list of fragment types that exist in the event
	std::vector<Fragment::type_t> type_list;
	popped_event->fragmentTypes(type_list);
	// insert the Fragments of each type into the EventPrincipal
	std::map<Fragment::type_t, std::string>::const_iterator iter_end =
		fragment_type_map_.end();
	for (size_t idx = 0; idx < type_list.size(); ++idx)
	{
		std::map<Fragment::type_t, std::string>::const_iterator iter =
			fragment_type_map_.find(type_list[idx]);
		auto product = popped_event->releaseProduct(type_list[idx]);
		for (auto &frag : *product)
			bytesRead += frag.sizeBytes();
		if (iter != iter_end)
		{
			if (type_list[idx] == artdaq::Fragment::ContainerFragmentType)
			{
				std::map<std::string, Fragments> derived_fragments;

				for (auto frag : *product)
				{
					ContainerFragment cf(frag);
					auto contained_type = fragment_type_map_.find(cf.fragment_type());
					if (contained_type != iter_end)
					{
						auto label = iter->second + "_" + contained_type->second;
						derived_fragments[label].push_back(std::move(frag));
					}
					else
					{
						derived_fragments[iter->second].push_back(std::move(frag));
					}
				}

				for (auto& type : derived_fragments)
				{
					put_product_in_principal(std::unique_ptr<Fragments>(&type.second),
											 *outE,
											 pretend_module_name,
											 type.first);
				}

			}
			else
			{
				put_product_in_principal(std::move(product),
										 *outE,
										 pretend_module_name,
										 iter->second);
			}
		}
		else
		{
			put_product_in_principal(std::move(product),
									 *outE,
									 pretend_module_name,
									 unidentified_instance_name);
			TLOG_WARNING("RawEventQueueReader")
				<< "UnknownFragmentType: The product instance name mapping for fragment type \""
				<< ((int)type_list[idx]) << "\" is not known. Fragments of this "
				<< "type will be stored in the event with an instance name of \""
				<< unidentified_instance_name << "\"." << TLOG_ENDL;
		}
	}
	TRACE( 10, "readNext: bytesRead=%lu qsize=%zu cap=%zu metricMan=%p", bytesRead, qsize, incoming_events.capacity(), (void*)metricMan );
	if (metricMan) {
		metricMan->sendMetric( "bytesRead", bytesRead>>20, "MB", 5, false, "", true );
		metricMan->sendMetric( "queue%Used", static_cast<unsigned long int>(qsize*100/incoming_events.capacity()), "%", 5, false, "", true );
	}

	return true;
}
