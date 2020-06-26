#define TRACE_NAME "ShmemBinaryOutput"
#include "artdaq/DAQdata/Globals.hh"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/Selector.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include <unistd.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace art {
class ShmemBinaryOutput;
}

#if ART_HEX_VERSION < 0x30000
#define RUN_ID id
#define SUBRUN_ID id
#define EVENT_ID id
#else
#define RUN_ID runID
#define SUBRUN_ID subRunID
#define EVENT_ID eventID
#endif

using art::ShmemBinaryOutput;
using fhicl::ParameterSet;

/**
 * \brief An art::OutputModule which sends Fragments using DataSenderManager.
 * This module produces output identical to that of a BoardReader, for use in
 * systems which have multiple layers of EventBuilders.
 */
class art::ShmemBinaryOutput final : public OutputModule
{
public:
	/**
	 * \brief ShmemBinaryOutput Constructor
	 * \param ps ParameterSet used to configure ShmemBinaryOutput
	 *
	 * ShmemBinaryOutput forwards its ParameterSet to art::OutputModule,
	 * so any Parameters it requires are also required by ShmemBinaryOutput.
	 * Finally, ShmemBinaryOutput accpets the following parameters:
	 * "module_name" (Default: ShmemBinaryOutput): Friendly name for this module (MessageFacility Category)
	 */
	explicit ShmemBinaryOutput(ParameterSet const& ps);

	/**
	 * \brief ShmemBinaryOutput Destructor
	 */
	virtual ~ShmemBinaryOutput();

private:
	void beginJob() override;

	void endJob() override;

	void write(EventPrincipal&) override;

	void writeRun(RunPrincipal&) override;
	void writeSubRun(SubRunPrincipal&) override;

	bool broadcastFragment_(artdaq::FragmentPtr& frag, uint32_t run_id, uint32_t subrun_id);

private:
	std::string name_ = "ShmemBinaryOutput";
	artdaq::SharedMemoryManager events_;
	artdaq::SharedMemoryManager broadcasts_;
	size_t send_timeout_ms_;
};

art::ShmemBinaryOutput::ShmemBinaryOutput(ParameterSet const& pset)
    : OutputModule(pset)
    , name_(pset.get<std::string>("module_name", "ShmemBinaryOutput"))
    , events_(pset.get<uint32_t>("shared_memory_key", 0xEEEE0000 + pset.get<uint16_t>("shared_memory_id", getpid())),
              pset.get<size_t>("buffer_count", 10),
              pset.get<size_t>("max_event_size_bytes"),
              pset.get<size_t>("stale_buffer_timeout_usec", 5000000),
              true)
    , broadcasts_(pset.get<uint32_t>("broadcast_shared_memory_key", 0xBBBB0000 + pset.get<uint16_t>("shared_memory_id", getpid())),
                  pset.get<size_t>("broadcast_buffer_count", 10),
                  pset.get<size_t>("broadcast_buffer_size", 0x100000),
                  pset.get<size_t>("stale_buffer_timeout_usec", 5000000), false)
    , send_timeout_ms_(pset.get<size_t>("send_timeout_ms", 1000))
{
	TLOG(TLVL_DEBUG) << "Begin: ShmemBinaryOutput::ShmemBinaryOutput(ParameterSet const& ps)\n";
	TLOG(TLVL_DEBUG) << "End: ShmemBinaryOutput::ShmemBinaryOutput(ParameterSet const& ps)\n";
}

art::ShmemBinaryOutput::~ShmemBinaryOutput() { TLOG(TLVL_DEBUG) << "Begin/End: ShmemBinaryOutput::~ShmemBinaryOutput()\n"; }

void art::ShmemBinaryOutput::beginJob()
{
	TLOG(TLVL_DEBUG) << "Begin: ShmemBinaryOutput::beginJob()\n";
	TLOG(TLVL_DEBUG) << "End:   ShmemBinaryOutput::beginJob()\n";
}

void art::ShmemBinaryOutput::endJob()
{
	TLOG(TLVL_DEBUG) << "Begin: ShmemBinaryOutput::endJob()\n";
	auto eodFrag = artdaq::Fragment::eodFrag(0);
	broadcastFragment_(eodFrag, 0, 0);

	TLOG(TLVL_DEBUG) << "End:   ShmemBinaryOutput::endJob()\n";
}

void art::ShmemBinaryOutput::write(EventPrincipal& ep)
{
	TLOG(TLVL_DEBUG) << "Writing Run " << ep.EVENT_ID().run() << ", SubRun " << ep.EVENT_ID().subRun() << ", Event " << ep.EVENT_ID().event();
	auto buffer = events_.GetBufferForWriting(false);
	TLOG(TLVL_DEBUG) << "write(EventPrincipal& ep): after getting buffer " << buffer;
	auto start_time = std::chrono::steady_clock::now();
	while (buffer == -1 && artdaq::TimeUtils::GetElapsedTimeMilliseconds(start_time) < static_cast<size_t>(send_timeout_ms_))
	{
		usleep(10000);
		buffer = events_.GetBufferForWriting(false);
	}
	TLOG(TLVL_DEBUG) << "write(EventPrincipal& ep): after getting buffer w/timeout, buffer=" << buffer << ", elapsed time=" << artdaq::TimeUtils::GetElapsedTime(start_time) << " s.";
	if (buffer == -1)
	{
		TLOG(TLVL_ERROR) << "Writing event failed due to timeout waiting for buffer!";
		return;
	}

	auto result_handles = std::vector<art::GroupQueryResult>();

	artdaq::detail::RawEventHeader hdr;
	auto const& wrapped_hdr = art::WrappedTypeID::make<artdaq::detail::RawEventHeader>();
#if ART_HEX_VERSION >= 0x30000
	ModuleContext const mc{moduleDescription()};
	ProcessTag const processTag{"", mc.moduleDescription().processName()};

	result_handles = ep.getMany(mc, wrapped_hdr, art::MatchAllSelector{}, processTag);
#else
	result_handles = ep.getMany(wrapped_hdr, art::MatchAllSelector{});
#endif

	for (auto const& result_handle : result_handles)
	{
		auto const raw_event_handle = art::Handle<artdaq::detail::RawEventHeader>(result_handle);

		if (!raw_event_handle.isValid()) continue;

		hdr = *raw_event_handle;
		break;
	}

	auto sequence_id = hdr.sequence_id;
	TLOG(TLVL_DEBUG) << "ShmemBinaryOutput::write: Writing Event Header";
	events_.Write(buffer, &hdr, sizeof(artdaq::detail::RawEventHeader));
	TLOG(TLVL_DEBUG) << "ShmemBinaryOutput::write: Done Writing Event Header";

	auto const& wrapped = art::WrappedTypeID::make<artdaq::Fragments>();
#if ART_HEX_VERSION >= 0x30000
	result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);
#else
	result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif

	for (auto const& result_handle : result_handles)
	{
		auto const raw_event_handle = art::Handle<artdaq::Fragments>(result_handle);

		if (!raw_event_handle.isValid()) continue;

		for (auto const& fragment : *raw_event_handle)
		{
			auto fragid_id = fragment.fragmentID();
			auto fragment_copy = fragment;
			TLOG(TLVL_DEBUG) << "ShmemBinaryOutput::write seq=" << sequence_id << " frag=" << fragid_id << " start";
			events_.Write(buffer, fragment_copy.headerAddress(), fragment_copy.size() * sizeof(artdaq::RawDataType));
			TLOG(TLVL_DEBUG) << "ShmemBinaryOutput::write seq=" << sequence_id << " frag=" << fragid_id << " done";
		}
	}

	return;
}

void art::ShmemBinaryOutput::writeRun(RunPrincipal& rp)
{
	auto outputFrag = std::make_unique<artdaq::Fragment>(0, rp.RUN_ID().run(), my_rank, artdaq::Fragment::EndOfRunFragmentType);
	broadcastFragment_(outputFrag, rp.RUN_ID().run(), 0);
}

void art::ShmemBinaryOutput::writeSubRun(SubRunPrincipal& srp)
{
	auto outputFrag = std::make_unique<artdaq::Fragment>(0, srp.RUN_ID().run(), my_rank, artdaq::Fragment::EndOfSubrunFragmentType);
	broadcastFragment_(outputFrag, srp.SUBRUN_ID().run(), srp.SUBRUN_ID().subRun());
}

bool art::ShmemBinaryOutput::broadcastFragment_(artdaq::FragmentPtr& frag, uint32_t run_id, uint32_t subrun_id)
{
	if (frag == nullptr)
	{
		TLOG(TLVL_ERROR) << "Requested broadcast but no Fragment given!";
		return false;
	}
	TLOG(TLVL_DEBUG) << "Broadcasting Fragment with seqID=" << frag->sequenceID()
	                 << ", type " << artdaq::detail::RawFragmentHeader::SystemTypeToString(frag->type())
	                 << ", size=" << frag->sizeBytes() << "B.";
	auto buffer = broadcasts_.GetBufferForWriting(false);
	TLOG(TLVL_DEBUG) << "broadcastFragment_: after getting buffer " << buffer;
	auto start_time = std::chrono::steady_clock::now();
	while (buffer == -1 && artdaq::TimeUtils::GetElapsedTimeMilliseconds(start_time) < static_cast<size_t>(send_timeout_ms_))
	{
		usleep(10000);
		buffer = broadcasts_.GetBufferForWriting(false);
	}
	TLOG(TLVL_DEBUG) << "broadcastFragment_: after getting buffer w/timeout, buffer=" << buffer << ", elapsed time=" << artdaq::TimeUtils::GetElapsedTime(start_time) << " s.";
	if (buffer == -1)
	{
		TLOG(TLVL_ERROR) << "Broadcast of fragment type " << frag->typeString() << " failed due to timeout waiting for buffer!";
		return false;
	}

	TLOG(TLVL_DEBUG) << "broadcastFragment_: Filling in RawEventHeader";
	auto hdr = reinterpret_cast<artdaq::detail::RawEventHeader*>(broadcasts_.GetBufferStart(buffer));
	hdr->run_id = run_id;
	hdr->subrun_id = subrun_id;
	hdr->sequence_id = frag->sequenceID();
	hdr->timestamp = frag->timestamp();
	hdr->is_complete = true;
	broadcasts_.IncrementWritePos(buffer, sizeof(artdaq::detail::RawEventHeader));

	TLOG(TLVL_DEBUG) << "broadcastFragment_ before Write call";
	broadcasts_.Write(buffer, frag->headerAddress(), frag->size() * sizeof(artdaq::RawDataType));

	TLOG(TLVL_DEBUG) << "broadcastFragment_ Marking buffer full";
	broadcasts_.MarkBufferFull(buffer, -1);
	TLOG(TLVL_DEBUG) << "broadcastFragment_ Complete";
	return true;
}

DEFINE_ART_MODULE(art::ShmemBinaryOutput)
