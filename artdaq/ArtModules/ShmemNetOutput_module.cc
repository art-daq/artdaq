#define TRACE_NAME "ShmemNetOutput"
#include "artdaq/ArtModules/ArtdaqOutput.hh"

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"

#include <fhiclcpp/ParameterSet.h>
#include <signal.h>


namespace art {
class ShmemNetOutput;
}

/**
 * \brief An art::OutputModule which sends events using DataSenderManager.
 * This module is designed for transporting Fragment-wrapped art::Events after
 * they have been read into art, for example between the EventBuilder and the Aggregator.
 */
class art::ShmemNetOutput : public ArtdaqOutput
{
public:
	/**
   * \brief ShmemNetOutput Constructor
   * \param ps ParameterSet used to configure ShmemNetOutput
   *
   * ShmemNetOutput accepts no Parameters beyond those which art::OutputModule takes.
   * See the art::OutputModule documentation for more details on those Parameters.
   */
	explicit ShmemNetOutput(fhicl::ParameterSet const& ps);

	/**
   * \brief ShmemNetOutput Destructor
   */
	~ShmemNetOutput();

protected:
	/// <summary>
	/// Send a message using the Transfer Plugin
	/// </summary>
	/// <param name="msg">Fragment to send</param>
	virtual void SendMessage(artdaq::FragmentPtr& msg);

private:
	bool writeFragment_(artdaq::SharedMemoryManager* shm, artdaq::FragmentPtr& frag);

	artdaq::SharedMemoryManager events_;
	artdaq::SharedMemoryManager broadcasts_;
	size_t send_retry_count_;
	size_t send_timeout_ms_;
};

art::ShmemNetOutput::ShmemNetOutput(fhicl::ParameterSet const& pset)
    : ArtdaqOutput(pset)
	, events_(pset.get<uint32_t>("shared_memory_key", 0xEEEE0000 + pset.get<uint16_t>("shared_memory_id", getpid())),
              pset.get<size_t>("buffer_count", 10),
              pset.get<size_t>("max_event_size_bytes"),
              pset.get<size_t>("stale_buffer_timeout_usec", 5000000),
              true)
    , broadcasts_(pset.get<uint32_t>("broadcast_shared_memory_key", 0xBBBB0000 + pset.get<uint16_t>("shared_memory_id", getpid())),
                  pset.get<size_t>("broadcast_buffer_count", 10),
                  pset.get<size_t>("broadcast_buffer_size", 0x100000),
                  pset.get<size_t>("stale_buffer_timeout_usec", 5000000), false)
    , send_retry_count_(pset.get<size_t>("send_retry_count", 5))
    , send_timeout_ms_(pset.get<size_t>("send_timeout_ms", 1000))
{
	TLOG(TLVL_DEBUG) << "Begin: ShmemNetOutput::ShmemNetOutput(ParameterSet const& ps)";
	TLOG(TLVL_DEBUG) << "END: ShmemNetOutput::ShmemNetOutput";
}

art::ShmemNetOutput::~ShmemNetOutput()
{
	TLOG(TLVL_DEBUG) << "Begin: ShmemNetOutput::~ShmemNetOutput()";

	auto eodFrag = artdaq::Fragment::eodFrag(0);
	auto sts = writeFragment_(&broadcasts_, eodFrag);
	if (!sts)
	{
		TLOG(TLVL_ERROR) << "Error broadcasting EndOfData Fragment!";
	}
	TLOG(TLVL_DEBUG) << "End: ShmemNetOutput::~ShmemNetOutput()";
}

void art::ShmemNetOutput::SendMessage(artdaq::FragmentPtr& fragment)
{
	TLOG(TLVL_DEBUG) << "Sending message with sequenceID=" << fragment->sequenceID() << ", type=" << fragment->type()
	                 << ", length=" << fragment->dataSizeBytes();

	auto sts = false;
	size_t retries = 0;

	while (!sts && retries <= send_retry_count_)
	{
		if (artdaq::Fragment::isSystemFragmentType(fragment->type()))
		{
			sts = writeFragment_(&broadcasts_, fragment);
		}
		else
		{
			sts = writeFragment_(&events_, fragment);
		}
		retries++;
	}

	if (retries > send_retry_count_)
	{
		TLOG(TLVL_ERROR) << "Error communicating with remote after " << retries << " tries. Closing art process";
		kill(getpid(), SIGUSR2);
	}

}

bool art::ShmemNetOutput::writeFragment_(artdaq::SharedMemoryManager* shm, artdaq::FragmentPtr& frag)
{
	if (frag == nullptr)
	{
		TLOG(TLVL_ERROR) << "Write requested but no Fragment given!";
		return false;
	}
	TLOG(TLVL_DEBUG) << "Writing Fragment with seqID=" << frag->sequenceID()
	                 << ", type " << artdaq::detail::RawFragmentHeader::SystemTypeToString(frag->type())
	                 << ", size=" << frag->sizeBytes() << "B.";
	auto buffer = shm->GetBufferForWriting(false);
	TLOG(TLVL_DEBUG) << "writeFragment_: after getting buffer " << buffer;
	auto start_time = std::chrono::steady_clock::now();
	while (buffer == -1 && artdaq::TimeUtils::GetElapsedTimeMilliseconds(start_time) < static_cast<size_t>(send_timeout_ms_))
	{
		usleep(10000);
		buffer = shm->GetBufferForWriting(false);
	}
	TLOG(TLVL_DEBUG) << "writeFragment_: after getting buffer w/timeout, buffer=" << buffer << ", elapsed time=" << artdaq::TimeUtils::GetElapsedTime(start_time) << " s.";
	if (buffer == -1)
	{
		TLOG(TLVL_ERROR) << "Write of fragment type " << frag->typeString() << " failed due to timeout waiting for buffer!";
		return false;
	}

	TLOG(TLVL_DEBUG) << "writeFragment_: Filling in RawEventHeader";
	auto hdr = reinterpret_cast<artdaq::detail::RawEventHeader*>(shm->GetBufferStart(buffer));
	hdr->run_id = 0;
	hdr->subrun_id = 0;
	hdr->sequence_id = frag->sequenceID();
	hdr->timestamp = frag->timestamp();
	hdr->is_complete = true;
	shm->IncrementWritePos(buffer, sizeof(artdaq::detail::RawEventHeader));

	TLOG(TLVL_DEBUG) << "writeFragment_ before Write call";
	shm->Write(buffer, frag->headerAddress(), frag->size() * sizeof(artdaq::RawDataType));

	TLOG(TLVL_DEBUG) << "writeFragment_ Marking buffer full";
	shm->MarkBufferFull(buffer, -1);
	TLOG(TLVL_DEBUG) << "writeFragment_ Complete";
	return true;
}
DEFINE_ART_MODULE(art::ShmemNetOutput)
