#include "tracemf.h"
#define TRACE_NAME "ShmemWrapper"

#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryService.h"
#include "artdaq/ArtModules/detail/ShmemWrapper.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"

art::ShmemWrapper::ShmemWrapper(fhicl::ParameterSet const& ps)
{
	init_timeout_s_ = ps.get<double>("init_fragment_timeout_seconds", 600.0);
	// Make sure the ArtdaqSharedMemoryService is available
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
}

artdaq::FragmentPtrs art::ShmemWrapper::receiveMessage()
{
	TLOG(TLVL_DEBUG + 34) << "Receiving Fragment from NetMonTransportService";
	TLOG(TLVL_DEBUG + 33) << "receiveMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	artdaq::FragmentPtrs output;

	// Do not process data until Init Fragment received!
	auto start = std::chrono::steady_clock::now();
	while (!init_received_ && artdaq::TimeUtils::GetElapsedTime(start) < init_timeout_s_)
	{
		usleep(static_cast<unsigned>(init_timeout_s_ * 1000000 / 100));  // Check 100 times
	}
	if (!init_received_)
	{
		TLOG(TLVL_ERROR) << "Did not receive Init Fragment after " << init_timeout_s_ << " seconds.";
		return output;
	}

	artdaq::Fragments recvd_fragments;
	while (recvd_fragments.empty())
	{
		auto eventMap = shm->ReceiveEvent(false);

		if (eventMap.empty())
		{
			TLOG(TLVL_DEBUG + 32) << "Did not receive event after timeout, returning from receiveMessage ";
			return output;
		}

		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)) != 0u)
		{
			TLOG(TLVL_DEBUG + 32) << "Received shutdown message, returning";
			return output;
		}
		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)) != 0u)
		{
			std::move(eventMap[artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)]->begin(), eventMap[artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)]->end(), std::back_inserter(recvd_fragments));
		}
		std::sort(recvd_fragments.begin(), recvd_fragments.end(), artdaq::fragmentSequenceIDCompare);
	}

	TLOG(TLVL_DEBUG + 33) << "receiveMessage: Returning top Fragment";
	for (auto& frag : recvd_fragments)
	{
		TLOG(TLVL_DEBUG + 33) << "receiveMessage: Returning Fragment, length="
		                 << frag.metadata<artdaq::NetMonHeader>()->data_length;
		output.emplace_back(new artdaq::Fragment(std::move(frag)));
	}

#if DUMP_RECEIVE_MESSAGE
	std::string fileName = "receiveMessage_" + std::to_string(my_rank) + "_" + std::to_string(getpid()) + "_" +
	                       std::to_string(topFrag.sequenceID()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif

	TLOG(TLVL_DEBUG + 33) << "receiveMessage END";

	TLOG(TLVL_DEBUG + 34) << "Done Receiving Fragment from NetMonTransportService";
	return output;
}

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> art::ShmemWrapper::receiveMessages()
{
	TLOG(TLVL_DEBUG + 34) << "Receiving Fragment from NetMonTransportService";
	TLOG(TLVL_DEBUG + 33) << "receiveMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> output;

	// Do not process data until Init Fragment received!
	auto start = std::chrono::steady_clock::now();
	while (!init_received_ && artdaq::TimeUtils::GetElapsedTime(start) < init_timeout_s_)
	{
		usleep(static_cast<unsigned>(init_timeout_s_ * 1000000 / 100));  // Check 100 times
	}
	if (!init_received_)
	{
		TLOG(TLVL_ERROR) << "Did not receive Init Fragment after " << init_timeout_s_ << " seconds.";
	}

	output = shm->ReceiveEvent(false);

	if (output.empty())
	{
		TLOG(TLVL_DEBUG + 32) << "Did not receive event after timeout, returning from receiveMessage ";
		return output;
	}

	hdr_ptr_ = shm->GetEventHeader();
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
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> eventMap;
	while (eventMap.empty())
	{
		eventMap = shm->ReceiveEvent(true);

		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)) != 0u)
		{
			TLOG(TLVL_DEBUG + 32) << "Received shutdown message, returning";
			artdaq::FragmentPtrs output;
			for (auto& frag : *eventMap[artdaq::Fragment::EndOfDataFragmentType])
			{
				output.emplace_back(new artdaq::Fragment(std::move(frag)));
			}
			return output;
		}
		if ((eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::InitFragmentType)) == 0u) && !eventMap.empty())
		{
			TLOG(TLVL_WARNING) << "Did NOT receive Init Fragment as first broadcast! Type="
			                   << artdaq::detail::RawFragmentHeader::SystemTypeToString(eventMap.begin()->first);
			eventMap.clear();
		}
		else if (artdaq::TimeUtils::GetElapsedTime(start) > init_timeout_s_)
		{
			TLOG(TLVL_WARNING) << "Did not receive Init fragment after init_fragment_timeout_seconds (" << artdaq::TimeUtils::GetElapsedTime(start) << ")!";
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
	for (auto& frag : *eventMap[artdaq::Fragment::InitFragmentType])
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
