#include "TRACE/tracemf.h"
#define TRACE_NAME "ShmemWrapper"

#include "artdaq/ArtModules/detail/ShmemWrapper.hh"

#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"
#include "artdaq/DAQdata/NetMonHeader.hh"

art::ShmemWrapper::ShmemWrapper(fhicl::ParameterSet const& ps)
{
	init_timeout_s_ = ps.get<double>("init_fragment_timeout_seconds", 600.0);
	// Make sure the ArtdaqSharedMemoryService is available
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
}

std::shared_ptr<ArtdaqEvent> art::ShmemWrapper::receiveMessages()
{
	TLOG(TLVL_DEBUG + 34) << "Receiving Fragment from NetMonTransportService";
	TLOG(TLVL_DEBUG + 33) << "receiveMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	std::shared_ptr<ArtdaqEvent> output;

	// Do not process data until Init Fragment received!
	auto start = std::chrono::steady_clock::now();
	while (!init_received_ && artdaq::TimeUtils::GetElapsedTime(start) < init_timeout_s_)
	{
		usleep(static_cast<unsigned>(init_timeout_s_ * 1000000 / 100));  // Check 100 times
		if (eod_received_)
		{
			TLOG(TLVL_DEBUG + 32) << "Received EndOfData message while waiting for Init Fragment, returning";
			return nullptr;
		}
	}
	if (!init_received_)
	{
		TLOG(TLVL_ERROR) << "Did not receive Init Fragment after " << init_timeout_s_ << " seconds.";
	}

	output = shm->ReceiveEvent(false);

	if (output == nullptr)
	{
		TLOG(TLVL_DEBUG + 32) << "Did not receive event after timeout, returning from receiveMessage ";
		return output;
	}

	TLOG(TLVL_DEBUG + 33) << "receiveMessage END";

	TLOG(TLVL_DEBUG + 34) << "Done Receiving Fragments from Shared Memory";
	return output;
}

artdaq::FragmentPtrs art::ShmemWrapper::receiveInitMessage()
{
	TLOG(TLVL_DEBUG + 34) << "Receiving Init Fragment from NetMonTransportService";

	TLOG(TLVL_DEBUG + 33) << "receiveInitMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	auto start = std::chrono::steady_clock::now();
	std::shared_ptr<ArtdaqEvent> eventMap;
	while (eventMap == nullptr)
	{
		eventMap = shm->ReceiveEvent(true);

		if (eventMap != nullptr)
		{
			auto type = eventMap->FirstFragmentType();
			if (type == artdaq::Fragment::EndOfDataFragmentType)
			{
				TLOG(TLVL_DEBUG + 32) << "Received shutdown message, returning";
				eod_received_ = true;
				artdaq::FragmentPtrs output;
				for (auto& frag : *eventMap->fragments[artdaq::Fragment::EndOfDataFragmentType])
				{
					output.emplace_back(new artdaq::Fragment(std::move(frag)));
				}
				return output;
			}
			if (type != artdaq::Fragment::InitFragmentType)
			{
				TLOG(TLVL_WARNING) << "Did NOT receive Init Fragment as first broadcast! Type="
				                   << artdaq::detail::RawFragmentHeader::SystemTypeToString(type);
				eventMap = nullptr;
			}
		}
		else if (artdaq::TimeUtils::GetElapsedTime(start) > init_timeout_s_)
		{
			TLOG(TLVL_WARNING) << "Did not receive Init fragment after init_fragment_timeout_seconds (" << artdaq::TimeUtils::GetElapsedTime(start) << ")!";
			eod_received_ = true;
			return artdaq::FragmentPtrs();
		}
	}

	// We return false, indicating we're done reading, if:
	//   1) we did not obtain an event, because we timed out and were
	//      configured NOT to keep trying after a timeout, or
	//   2) the event we read was the end-of-data marker: a null
	//      pointer

	TLOG(TLVL_DEBUG + 33) << "receiveInitMessage: Returning top Fragment";
	artdaq::FragmentPtrs output;
	for (auto& frag : *eventMap->fragments[artdaq::Fragment::InitFragmentType])
	{
		output.emplace_back(new artdaq::Fragment(std::move(frag)));
	}

#if DUMP_RECEIVE_MESSAGE
	std::string fileName = "receiveInitMessage_" + std::to_string(getpid()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif

	TLOG(TLVL_DEBUG + 33) << "receiveInitMessage END";
	init_received_ = true;

	TLOG(TLVL_DEBUG + 34) << "Done Receiving Init Fragment from NetMonTransportService";
	return output;
}
