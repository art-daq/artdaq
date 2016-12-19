#include "artdaq/ArtModules/detail/RawEventQueueReader.hh"

#include "art/Framework/IO/Sources/put_product_in_principal.h"
#ifdef CANVAS
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "canvas/Utilities/Exception.h"
#else
#include "art/Persistency/Provenance/FileFormatVersion.h"
#include "art/Utilities/Exception.h"
#endif
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include <sys/time.h>

using std::string;

artdaq::detail::RawEventQueueReader::RawEventQueueReader(fhicl::ParameterSet const & ps,
	art::ProductRegistryHelper & help,
	art::SourceHelper const & pm) :
	pmaker(pm),
	incoming_events(getGlobalQueue()),
	waiting_time(ps.get<double>("waiting_time", 86400.0)),
	resume_after_timeout(ps.get<bool>("resume_after_timeout", true)),
	pretend_module_name("daq"),
	unidentified_instance_name("unidentified"),
	shutdownMsgReceived(false), outputFileCloseNeeded(false),
	fragment_type_map_(Fragment::MakeSystemTypeMap())
{
	help.reconstitutes<Fragments, art::InEvent>(pretend_module_name,
		unidentified_instance_name);
	for(auto it = fragment_type_map_.begin(); it != fragment_type_map_.end(); ++it)
	{
		help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, it->second);
	}
}

void artdaq::detail::RawEventQueueReader::closeCurrentFile()
{
}

void artdaq::detail::RawEventQueueReader::readFile(string const &,
	art::FileBlock *& fb)
{
	fb = new art::FileBlock(art::FileFormatVersion(1, "RawEvent2011"), "nothing");
}

bool artdaq::detail::RawEventQueueReader::readNext(art::RunPrincipal * const & inR,
	art::SubRunPrincipal * const & inSR,
	art::RunPrincipal *& outR,
	art::SubRunPrincipal *& outSR,
	art::EventPrincipal *& outE)
{
	if (outputFileCloseNeeded) {
		outputFileCloseNeeded = false;
		return false;
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
	while (keep_looping) {
		keep_looping = false;
		got_event = incoming_events.deqTimedWait(popped_event, waiting_time);
		if (!got_event) {
			mf::LogInfo("InputFailure")
				<< "Reading timed out in RawEventQueueReader::readNext()";
			keep_looping = resume_after_timeout;
		}
	}
	// We return false, indicating we're done reading, if:
	//   1) we did not obtain an event, because we timed out and were
	//      configured NOT to keep trying after a timeout, or
	//   2) the event we read was the end-of-data marker: a null
	//      pointer
	if (!got_event || !popped_event) {
		shutdownMsgReceived = true;
		return false;
	}

	// Check the number of fragments in the RawEvent.  If we have a single
	// fragment and that fragment is marked as EndRun or EndSubrun we'll create
	// the special principals for that.
	art::Timestamp currentTime = time(0);

	// make new run if inR is 0 or if the run has changed
	if (inR == 0 || inR->run() != popped_event->runID()) {
		outR = pmaker.makeRunPrincipal(popped_event->runID(),
			currentTime);
	}

	if (popped_event->numFragments() == 1) {
		if (popped_event->releaseProduct(Fragment::EndOfRunFragmentType)->size() == 1) {
			art::EventID const evid(art::EventID::flushEvent());
			outR = pmaker.makeRunPrincipal(evid.runID(), currentTime);
			outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
			outE = pmaker.makeEventPrincipal(evid, currentTime);
			return true;
		}
		else if (popped_event->releaseProduct(Fragment::EndOfSubrunFragmentType)->size() == 1) {
			// Check if inR == 0 or is a new run
			if (inR == 0 || inR->run() != popped_event->runID()) {
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
			else {
				// If the previous subrun was neither 0 nor flush and was identical with the current
		  // subrun, then it must have been associated with a data event.  In that case, we need
		  // to generate a flush event with a valid run but flush subrun and event number in order
		  // to end the subrun.
				if (inSR != 0 && !inSR->id().isFlush() && inSR->subRun() == popped_event->subrunID()) {
					art::EventID const evid(art::EventID::flushEvent(inR->id()));
					outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
					outE = pmaker.makeEventPrincipal(evid, currentTime);
					// If this is either a new or another empty subrun, then generate a flush event with
					// valid run and subrun numbers but flush event number
					//} else if(inSR==0 || inSR->id().isFlush()){
				}
				else {
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
			outputFileCloseNeeded = true;
			return true;
		}
	}

	// make new subrun if inSR is 0 or if the subrun has changed
	art::SubRunID subrun_check(popped_event->runID(), popped_event->subrunID());
	if (inSR == 0 || subrun_check != inSR->id()) {
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
	for (size_t idx = 0; idx < type_list.size(); ++idx) {
		std::map<Fragment::type_t, std::string>::const_iterator iter =
			fragment_type_map_.find(type_list[idx]);
		if (iter != iter_end) {
			put_product_in_principal(popped_event->releaseProduct(type_list[idx]),
				*outE,
				pretend_module_name,
				iter->second);
		}
		else {
			put_product_in_principal(popped_event->releaseProduct(type_list[idx]),
				*outE,
				pretend_module_name,
				unidentified_instance_name);
			mf::LogWarning("UnknownFragmentType")
				<< "The product instance name mapping for fragment type \""
				<< ((int)type_list[idx]) << "\" is not known. Fragments of this "
				<< "type will be stored in the event with an instance name of \""
				<< unidentified_instance_name << "\".";
		}
	}

	return true;
}
