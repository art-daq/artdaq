#define TRACE_NAME "NetMonWrapper"

#include "artdaq/ArtModules/NetMonWrapper.hh"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryService.h"
#include "artdaq/DAQdata/NetMonHeader.hh"

art::NetMonWrapper::NetMonWrapper(fhicl::ParameterSet const& ps)
{
	init_timeout_s_ = ps.get<double>("init_fragment_timeout_seconds", 1.0);
	// Make sure the ArtdaqSharedMemoryService is available
	art::ServiceHandle<ArtdaqSharedMemoryService> shm;
}

artdaq::FragmentPtr art::NetMonWrapper::receiveMessage()
{
	TLOG(5) << "Receiving Fragment from NetMonTransportService";
	TLOG(TLVL_TRACE) << "receiveMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryService> shm;

	// Do not process data until Init Fragment received!
	auto start = std::chrono::steady_clock::now();
	while (!init_received_ && artdaq::TimeUtils::GetElapsedTime(start) < init_timeout_s_)
	{
		usleep(init_timeout_s_ * 1000000 / 100);  // Check 100 times
	}
	if (!init_received_)
	{
		TLOG(TLVL_ERROR) << "Did not receive Init Fragment after " << init_timeout_s_ << " seconds. Art will crash.";
		return nullptr;
	}

	artdaq::Fragments recvd_fragments;
	while (recvd_fragments.size() == 0)
	{
		auto eventMap = shm->ReceiveEvent(false);

		if (eventMap.size() == 0)
		{
			TLOG(TLVL_DEBUG) << "Did not receive event after timeout, returning from receiveMessage ";
			return nullptr;
		}

		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)))
		{
			TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
			return nullptr;
		}
		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)))
		{
			std::move(eventMap[artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)]->begin(), eventMap[artdaq::Fragment::type_t(artdaq::Fragment::DataFragmentType)]->end(), std::back_inserter(recvd_fragments));
		}
		std::sort(recvd_fragments.begin(), recvd_fragments.end(), artdaq::fragmentSequenceIDCompare);
	}

	TLOG(TLVL_TRACE) << "receiveMessage: Returning top Fragment";
	artdaq::FragmentPtr topFrag = artdaq::FragmentPtr(new artdaq::Fragment(std::move(recvd_fragments.at(0))));
	recvd_fragments.erase(recvd_fragments.begin());

	TLOG(TLVL_TRACE) << "receiveMessage: Returning Fragment, length="
	                 << topFrag->metadata<artdaq::NetMonHeader>()->data_length;

#if DUMP_RECEIVE_MESSAGE
	std::string fileName = "receiveMessage_" + std::to_string(my_rank) + "_" + std::to_string(getpid()) + "_" +
	                       std::to_string(topFrag.sequenceID()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif

	TLOG(TLVL_TRACE) << "receiveMessage END";

	TLOG(5) << "Done Receiving Fragment from NetMonTransportService";
	return topFrag;
}

artdaq::FragmentPtr art::NetMonWrapper::receiveInitMessage()
{
	TLOG(5) << "Receiving Init Fragment from NetMonTransportService";

	TLOG(TLVL_TRACE) << "receiveInitMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryService> shm;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> eventMap;
	while (eventMap.size() == 0)
	{
		eventMap = shm->ReceiveEvent(true);

		if (eventMap.count(artdaq::Fragment::type_t(artdaq::Fragment::EndOfDataFragmentType)))
		{
			TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
			return nullptr;
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
	artdaq::FragmentPtr topFrag = artdaq::FragmentPtr(new artdaq::Fragment(std::move(eventMap[artdaq::Fragment::InitFragmentType]->at(0))));


#if DUMP_RECEIVE_MESSAGE
	std::string fileName = "receiveInitMessage_" + std::to_string(getpid()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif


	TLOG(TLVL_TRACE) << "receiveInitMessage END";
	init_received_ = true;

	TLOG(5) << "Done Receiving Init Fragment from NetMonTransportService";
	return topFrag;
}
