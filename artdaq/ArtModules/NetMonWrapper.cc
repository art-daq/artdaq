#define TRACE_NAME "NetMonWrapper"

#include "artdaq/ArtModules/NetMonWrapper.hh"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryService.h"
#include "artdaq/DAQdata/NetMonHeader.hh"

art::NetMonWrapper::NetMonWrapper(fhicl::ParameterSet const& ps)
{
	init_timeout_s_ = ps.get<double>("init_fragment_timeout_seconds", 1.0);
	// Make sure the ArtdaqSharedMemoryService is available
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
}

artdaq::FragmentPtrs art::NetMonWrapper::receiveMessage()
{
	TLOG(5) << "Receiving Fragment from NetMonTransportService";
	TLOG(TLVL_TRACE) << "receiveMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	artdaq::FragmentPtrs output;

	// Do not process data until Init Fragment received!
	auto start = std::chrono::steady_clock::now();
	while (!init_received_ && artdaq::TimeUtils::GetElapsedTime(start) < init_timeout_s_)
	{
		usleep(init_timeout_s_ * 1000000 / 100);  // Check 100 times
	}
	if (!init_received_)
	{
		TLOG(TLVL_ERROR) << "Did not receive Init Fragment after " << init_timeout_s_ << " seconds. Art will crash.";
		return output;
	}

	artdaq::Fragments recvd_fragments;
	while (recvd_fragments.size() == 0)
	{
		auto eventMap = shm->ReceiveEvent(false);

		if (eventMap.size() == 0)
		{
			TLOG(TLVL_DEBUG) << "Did not receive event after timeout, returning from receiveMessage ";
			return output;
		}

		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)))
		{
			TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
			return output;
		}
		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)))
		{
			std::move(eventMap[artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)]->begin(), eventMap[artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)]->end(), std::back_inserter(recvd_fragments));
		}
		std::sort(recvd_fragments.begin(), recvd_fragments.end(), artdaq::fragmentSequenceIDCompare);
	}

	TLOG(TLVL_TRACE) << "receiveMessage: Returning top Fragment";
	for (auto& frag : recvd_fragments)
	{
		TLOG(TLVL_TRACE) << "receiveMessage: Returning Fragment, length="
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

	TLOG(TLVL_TRACE) << "receiveMessage END";

	TLOG(5) << "Done Receiving Fragment from NetMonTransportService";
	return output;
}

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> art::NetMonWrapper::receiveMessages()
{
	TLOG(5) << "Receiving Fragment from NetMonTransportService";
	TLOG(TLVL_TRACE) << "receiveMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> output;

	// Do not process data until Init Fragment received!
	auto start = std::chrono::steady_clock::now();
	while (!init_received_ && artdaq::TimeUtils::GetElapsedTime(start) < init_timeout_s_)
	{
		usleep(init_timeout_s_ * 1000000 / 100);  // Check 100 times
	}
	if (!init_received_)
	{
		TLOG(TLVL_ERROR) << "Did not receive Init Fragment after " << init_timeout_s_ << " seconds. Art will crash.";
		return output;
	}

	output = shm->ReceiveEvent(false);

	if (output.size() == 0)
	{
		TLOG(TLVL_DEBUG) << "Did not receive event after timeout, returning from receiveMessage ";
		return output;
	}

	if (output.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)))
	{
		TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
		return std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>>();
	}

	TLOG(TLVL_TRACE) << "receiveMessage END";

	TLOG(5) << "Done Receiving Fragments from Shared Memory";
	return output;
}

artdaq::FragmentPtrs art::NetMonWrapper::receiveInitMessage()
{
	TLOG(5) << "Receiving Init Fragment from NetMonTransportService";

	TLOG(TLVL_TRACE) << "receiveInitMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> eventMap;
	while (eventMap.size() == 0)
	{
		eventMap = shm->ReceiveEvent(true);

		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)))
		{
			TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
			return artdaq::FragmentPtrs();
		}
		else if (!eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::InitFragmentType)) && eventMap.size() > 0)
		{
			TLOG(TLVL_WARNING) << "Did NOT receive Init Fragment as first broadcast! Type="
			                   << artdaq::detail::RawFragmentHeader::SystemTypeToString(eventMap.begin()->first);
			eventMap.clear();
		}
	}

	// We return false, indicating we're done reading, if:
	//   1) we did not obtain an event, because we timed out and were
	//      configured NOT to keep trying after a timeout, or
	//   2) the event we read was the end-of-data marker: a null
	//      pointer

	TLOG(TLVL_TRACE) << "receiveInitMessage: Returning top Fragment";
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

	TLOG(TLVL_TRACE) << "receiveInitMessage END";
	init_received_ = true;

	TLOG(5) << "Done Receiving Init Fragment from NetMonTransportService";
	return output;
}
