
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/ArtModules/detail/SharedMemoryReader.hh"

#include "art/Framework/IO/Sources/put_product_in_principal.h"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "canvas/Utilities/Exception.h"
#include "artdaq-core/Data/Fragment.hh"
#include <sys/time.h>
#define TRACE_NAME "SharedMemoryReader"
#include "trace.h"

using std::string;

artdaq::detail::SharedMemoryEventReceiver::SharedMemoryEventReceiver(int shm_key, size_t buffer_count, size_t max_buffer_size)
	: SharedMemoryManager(shm_key, buffer_count, max_buffer_size)
	, current_read_buffer_(-1)
{

}
std::shared_ptr<artdaq::detail::RawEventHeader> artdaq::detail::SharedMemoryEventReceiver::ReadHeader()
{
	if (current_header_) return current_header_;
	auto buf = GetBufferForReading();
	if (buf == -1) throw cet::exception("OutOfEvents") << "ReadHeader called but no events are ready! (Did you check ReadyForRead()?)";
	current_read_buffer_ = buf;
	ResetReadPos(current_read_buffer_);
	current_header_ = std::shared_ptr<detail::RawEventHeader>(reinterpret_cast<detail::RawEventHeader*>(GetReadPos(buf)));
	return current_header_;
}
std::set<artdaq::Fragment::type_t> artdaq::detail::SharedMemoryEventReceiver::GetFragmentTypes()
{
	if (current_read_buffer_ == -1) throw cet::exception("AccessViolation") << "Cannot call GetFragmentTypes when not currently reading a buffer! Call ReadHeader() first!";
	ResetReadPos(current_read_buffer_);
	IncrementReadPos(current_read_buffer_, sizeof(detail::RawEventHeader));

	auto output = std::set<Fragment::type_t>();

	while (MoreDataInBuffer(current_read_buffer_))
	{
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(current_read_buffer_));
		output.insert(fragHdr->type);
		IncrementReadPos(current_read_buffer_, fragHdr->word_count * sizeof(RawDataType));
	}

	return output;
}

std::unique_ptr<artdaq::Fragments> artdaq::detail::SharedMemoryEventReceiver::GetFragmentsByType(Fragment::type_t type)
{
	if (current_read_buffer_ == -1) throw cet::exception("AccessViolation") << "Cannot call GetFragmentsByType when not currently reading a buffer! Call ReadHeader() first!";
	ResetReadPos(current_read_buffer_);
	IncrementReadPos(current_read_buffer_, sizeof(detail::RawEventHeader));

	Fragments output;

	while (MoreDataInBuffer(current_read_buffer_))
	{
		auto fragHdr = reinterpret_cast<artdaq::detail::RawFragmentHeader*>(GetReadPos(current_read_buffer_));
		if (fragHdr->type != type) continue;

		output.emplace_back(fragHdr->word_count - detail::RawFragmentHeader::num_words());
		Read(current_read_buffer_, output.back().headerAddress(), fragHdr->word_count * sizeof(RawDataType));

		IncrementReadPos(current_read_buffer_, fragHdr->word_count * sizeof(RawDataType));
	}

	return std::unique_ptr<Fragments>(&output);
}

void artdaq::detail::SharedMemoryEventReceiver::ReleaseBuffer()
{
	SharedMemoryManager::ReleaseBuffer(current_read_buffer_);
	current_read_buffer_ = -1;
	current_header_.reset();
}



artdaq::detail::SharedMemoryReader::SharedMemoryReader(fhicl::ParameterSet const& ps,
													   art::ProductRegistryHelper& help,
													   art::SourceHelper const& pm)
	: pmaker(pm)
	, incoming_events(new SharedMemoryEventReceiver(ps.get<int>("shared_memory_key", 0xBEE7),
												   ps.get<size_t>("buffer_count", 20), 
												   ps.get<size_t>("max_buffer_size", 1024)))
	, waiting_time(ps.get<double>("waiting_time", 86400.0))
	, resume_after_timeout(ps.get<bool>("resume_after_timeout", true))
	, pretend_module_name(ps.get<std::string>("raw_data_label", "daq"))
	, unidentified_instance_name("unidentified")
	, shutdownMsgReceived(false)
	, outputFileCloseNeeded(false)
	, bytesRead(0)
	, fragment_type_map_(Fragment::MakeSystemTypeMap())
	, readNext_calls_(0)
{
	help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, unidentified_instance_name);
	for (auto it = fragment_type_map_.begin(); it != fragment_type_map_.end(); ++it)
	{
		help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, it->second);
	}
}

void artdaq::detail::SharedMemoryReader::closeCurrentFile() {}

void artdaq::detail::SharedMemoryReader::readFile(string const&,
												  art::FileBlock*& fb)
{
	fb = new art::FileBlock(art::FileFormatVersion(1, "RawEvent2011"), "nothing");
}

bool artdaq::detail::SharedMemoryReader::readNext(art::RunPrincipal* const & inR,
												  art::SubRunPrincipal* const & inSR,
												  art::RunPrincipal*& outR,
												  art::SubRunPrincipal*& outSR,
												  art::EventPrincipal*& outE)
{
	/*if (outputFileCloseNeeded) {
		outputFileCloseNeeded = false;
		return false;
	}*/
	// Establish default 'results'
	outR = 0;
	outSR = 0;
	outE = 0;
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
		got_event = incoming_events->ReadyForRead();
		if (!got_event)
		{
			TLOG_INFO("SharedMemoryReader")
				<< "InputFailure: Reading timed out in SharedMemoryReader::readNext()" << TLOG_ENDL;
			keep_looping = resume_after_timeout;
		}
	}

	auto evtHeader = incoming_events->ReadHeader();
	auto fragmentTypes = incoming_events->GetFragmentTypes();
	if (fragmentTypes.size() == 0)
	{
		TLOG_ERROR("SharedMemoryReader") << "Event has no Fragments! Aborting!" << TLOG_ENDL;
		incoming_events->ReleaseBuffer();
		return false;
	}
	auto firstFragmentType = *fragmentTypes.begin();

	// We return false, indicating we're done reading, if:
	//   1) we did not obtain an event, because we timed out and were
	//      configured NOT to keep trying after a timeout, or
	//   2) the event we read was the end-of-data marker: a null
	//      pointer
	if (!got_event || firstFragmentType == Fragment::EndOfDataFragmentType)
	{
		TLOG_DEBUG("SharedMemoryReader") << "Received shutdown message, returning false" << TLOG_ENDL;
		shutdownMsgReceived = true;
		incoming_events->ReleaseBuffer();
		return false;
	}

	size_t qsize = incoming_events->ReadReadyCount(); // save the qsize at this point

	// Check the number of fragments in the RawEvent.  If we have a single
	// fragment and that fragment is marked as EndRun or EndSubrun we'll create
	// the special principals for that.
	art::Timestamp currentTime = time(0);

	// make new run if inR is 0 or if the run has changed
	if (inR == 0 || inR->run() != evtHeader->run_id)
	{
		outR = pmaker.makeRunPrincipal(evtHeader->run_id,
									   currentTime);
	}

	if (firstFragmentType == Fragment::EndOfRunFragmentType)
	{
		art::EventID const evid(art::EventID::flushEvent());
		outR = pmaker.makeRunPrincipal(evid.runID(), currentTime);
		outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
		outE = pmaker.makeEventPrincipal(evid, currentTime);
		incoming_events->ReleaseBuffer();
		return true;
	}
	else if (firstFragmentType == Fragment::EndOfSubrunFragmentType)
	{
		// Check if inR == 0 or is a new run
		if (inR == 0 || inR->run() != evtHeader->run_id)
		{
			outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id,
											   evtHeader->subrun_id,
											   currentTime);
			art::EventID const evid(art::EventID::flushEvent(outSR->id()));
			outE = pmaker.makeEventPrincipal(evid, currentTime);
		}
		else
		{
			// If the previous subrun was neither 0 nor flush and was identical with the current
			// subrun, then it must have been associated with a data event.  In that case, we need
			// to generate a flush event with a valid run but flush subrun and event number in order
			// to end the subrun.
			if (inSR != 0 && !inSR->id().isFlush() && inSR->subRun() == evtHeader->subrun_id)
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
				outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id,
												   evtHeader->subrun_id,
												   currentTime);
				art::EventID const evid(art::EventID::flushEvent(outSR->id()));
				outE = pmaker.makeEventPrincipal(evid, currentTime);
				// Possible error condition
				//} else {
			}
			outR = 0;
		}
		//outputFileCloseNeeded = true;
		incoming_events->ReleaseBuffer();
		return true;
	}

	// make new subrun if inSR is 0 or if the subrun has changed
	art::SubRunID subrun_check(evtHeader->run_id, evtHeader->subrun_id);
	if (inSR == 0 || subrun_check != inSR->id())
	{
		outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id,
										   evtHeader->subrun_id,
										   currentTime);
	}
	outE = pmaker.makeEventPrincipal(evtHeader->run_id,
									 evtHeader->subrun_id,
									 evtHeader->sequence_id,
									 currentTime);

	// insert the Fragments of each type into the EventPrincipal
	std::map<Fragment::type_t, std::string>::const_iterator iter_end =
		fragment_type_map_.end();
	for (auto& type_code : fragmentTypes)
	{
		std::map<Fragment::type_t, std::string>::const_iterator iter =
			fragment_type_map_.find(type_code);
		auto product = incoming_events->GetFragmentsByType(type_code);
		for (auto &frag : *product)
			bytesRead += frag.sizeBytes();
		if (iter != iter_end)
		{
			put_product_in_principal(std::move(product),
									 *outE,
									 pretend_module_name,
									 iter->second);
		}
		else
		{
			put_product_in_principal(std::move(product),
									 *outE,
									 pretend_module_name,
									 unidentified_instance_name);
			TLOG_WARNING("SharedMemoryReader")
				<< "UnknownFragmentType: The product instance name mapping for fragment type \""
				<< ((int)type_code) << "\" is not known. Fragments of this "
				<< "type will be stored in the event with an instance name of \""
				<< unidentified_instance_name << "\"." << TLOG_ENDL;
		}
	}
	incoming_events->ReleaseBuffer();
	TRACE(10, "readNext: bytesRead=%lu qsize=%zu cap=%zu metricMan=%p", bytesRead, qsize, incoming_events->size(), (void*)metricMan);
	if (metricMan) {
		metricMan->sendMetric("bytesRead", bytesRead >> 20, "MB", 5, false, "", true);
		metricMan->sendMetric("queue%Used", static_cast<unsigned long int>(qsize * 100 / incoming_events->size()), "%", 5, false, "", true);
	}

	return true;
}
