#define TRACE_NAME "NetMonWrapper"

#include "artdaq/ArtModules/NetMonWrapper.hh"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryService.h"
#include "artdaq/DAQdata/NetMonHeader.hh"

#include <TBufferFile.h>

art::NetMonWrapper::NetMonWrapper(fhicl::ParameterSet const& ps)
{
	init_timeout_s_ = ps.get<double>("init_fragment_timeout_seconds", 1.0);
	// Make sure the ArtdaqSharedMemoryService is available
	art::ServiceHandle<ArtdaqSharedMemoryService> shm;
}

void art::NetMonWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg_ptr)
{
	TLOG(5) << "Receiving Fragment from NetMonTransportService";
	TBufferFile* msg(nullptr);
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
		msg = nullptr;
		return;
	}

	artdaq::Fragments recvd_fragments;
	while (recvd_fragments.size() == 0)
	{
		auto eventMap = shm->ReceiveEvent(false);

		if (eventMap.size() == 0)
		{
			TLOG(TLVL_DEBUG) << "Did not receive event after timeout, returning from receiveMessage ";
			return;
		}

		if (eventMap.count(artdaq::Fragment::EndOfDataFragmentType))
		{
			TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
			msg = nullptr;
			return;
		}
		if (eventMap.count(artdaq::Fragment::DataFragmentType))
		{
			std::move(eventMap[artdaq::Fragment::DataFragmentType]->begin(), eventMap[artdaq::Fragment::DataFragmentType]->end(), std::back_inserter(recvd_fragments));
		}
		std::sort(recvd_fragments.begin(), recvd_fragments.end(), artdaq::fragmentSequenceIDCompare);
	}

	TLOG(TLVL_TRACE) << "receiveMessage: Returning top Fragment";
	artdaq::Fragment topFrag = std::move(recvd_fragments.at(0));
	recvd_fragments.erase(recvd_fragments.begin());

	TLOG(TLVL_TRACE) << "receiveMessage: Copying Fragment into TBufferFile, length="
	                 << topFrag.metadata<artdaq::NetMonHeader>()->data_length;
	auto header = topFrag.metadata<artdaq::NetMonHeader>();
	auto buffer = static_cast<char*>(malloc(header->data_length));
	memcpy(buffer, &*topFrag.dataBegin(), header->data_length);
	msg = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);

#if DUMP_RECEIVE_MESSAGE
	std::string fileName = "receiveMessage_" + std::to_string(my_rank) + "_" + std::to_string(getpid()) + "_" +
	                       std::to_string(topFrag.sequenceID()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif

	TLOG(TLVL_TRACE) << "receiveMessage END";

	msg_ptr.reset(msg);
	TLOG(5) << "Done Receiving Fragment from NetMonTransportService";
}

void art::NetMonWrapper::receiveInitMessage(std::unique_ptr<TBufferFile>& msg_ptr)
{
	TLOG(5) << "Receiving Init Fragment from NetMonTransportService";
	TBufferFile* msg(nullptr);

	TLOG(TLVL_TRACE) << "receiveInitMessage BEGIN";
	art::ServiceHandle<ArtdaqSharedMemoryService> shm;
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> eventMap;
	while (eventMap.size() == 0)
	{
		eventMap = shm->ReceiveEvent(true);

		if (eventMap.count(artdaq::Fragment::EndOfDataFragmentType))
		{
			TLOG(TLVL_DEBUG) << "Received shutdown message, returning";
			msg = nullptr;
			return;
		}
		else if (!eventMap.count(artdaq::Fragment::InitFragmentType) && eventMap.size() > 0)
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
	artdaq::Fragment topFrag = std::move(eventMap[artdaq::Fragment::InitFragmentType]->at(0));

	auto header = topFrag.metadata<artdaq::NetMonHeader>();
	TLOG(TLVL_TRACE) << "receiveInitMessage: Copying Fragment into TBufferFile: message length: " << header->data_length;
	auto buffer = new char[header->data_length];
	// auto buffer = static_cast<char *>(malloc(header->data_length)); // Fix alloc-dealloc-mismatch
	memcpy(buffer, &*topFrag.dataBegin(), header->data_length);

#if DUMP_RECEIVE_MESSAGE
	std::string fileName = "receiveInitMessage_" + std::to_string(getpid()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif

	msg = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);

	TLOG(TLVL_TRACE) << "receiveInitMessage END";
	init_received_ = true;

	msg_ptr.reset(msg);
	TLOG(5) << "Done Receiving Init Fragment from NetMonTransportService";
}
