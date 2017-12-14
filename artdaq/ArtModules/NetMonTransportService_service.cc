#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "art/Framework/Services/Registry/ActivityRegistry.h"
#include "canvas/Utilities/Exception.h"
#include "cetlib/container_algorithms.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetRegistry.h"

#include <TClass.h>
#include <TBufferFile.h>

#include <iomanip>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>

static fhicl::ParameterSet empty_pset;


NetMonTransportService::
NetMonTransportService(fhicl::ParameterSet const& pset, art::ActivityRegistry&)
	: NetMonTransportServiceInterface()
	, data_pset_(pset)
	, sender_ptr_(nullptr)
	, incoming_events_(new artdaq::SharedMemoryEventReceiver(pset.get<int>("shared_memory_key", 0xBEE70000 + getppid()), pset.get<int>("broadcast_shared_memory_key", 0xCEE70000 + getppid())))
	, recvd_fragments_(nullptr)
{
	TLOG_TRACE("NetMonTransportService") << "NetMonTransportService CONSTRUCTOR" << TLOG_ENDL;
	if (pset.has_key("rank")) my_rank = pset.get<int>("rank");
	else my_rank = incoming_events_->GetRank();
}

NetMonTransportService::
~NetMonTransportService()
{
	NetMonTransportService::disconnect();
}

void
NetMonTransportService::
connect()
{
	sender_ptr_.reset(new artdaq::DataSenderManager(data_pset_));
}

void
NetMonTransportService::
listen()
{
	return;
}

void
NetMonTransportService::
disconnect()
{
	if (sender_ptr_) sender_ptr_.reset(nullptr);
}

void
NetMonTransportService::
sendMessage(uint64_t sequenceId, uint8_t messageType, TBufferFile& msg)
{
	if (sender_ptr_ == nullptr)
	{
		TLOG_DEBUG("NetMonTransportService") << "Reconnecting DataSenderManager" << TLOG_ENDL;
		connect();
	}

#if 0
	if (messageType == artdaq::Fragment::InitFragmentType) {
		std::fstream ostream("sendInitMessage.bin", std::ios::out | std::ios::binary);
		ostream.write(msg.Buffer(), msg.Length());
		ostream.close();
}
#endif

	TLOG_DEBUG("NetMonTransportService") << "Sending message with sequenceID=" << std::to_string(sequenceId) << ", type=" << std::to_string(messageType) << ", length=" << std::to_string(msg.Length()) << TLOG_ENDL;
	artdaq::NetMonHeader header;
	header.data_length = static_cast<uint64_t>(msg.Length());
	artdaq::Fragment
		fragment(std::ceil(msg.Length() /
						   static_cast<double>(sizeof(artdaq::RawDataType))),
				 sequenceId, 0, messageType, header);

	memcpy(&*fragment.dataBegin(), msg.Buffer(), msg.Length());
	sender_ptr_->sendFragment(std::move(fragment));
}

void
NetMonTransportService::
receiveMessage(TBufferFile*& msg)
{
	TLOG_TRACE("NetMonTransportService") << "receiveMessage BEGIN" << TLOG_ENDL;
	while (recvd_fragments_ == nullptr)
	{
		TLOG_TRACE("NetMonTransportService") << "receiveMessage: Waiting for available buffer" << TLOG_ENDL;
		bool keep_looping = true;
		bool got_event = false;
		while (keep_looping)
		{
			keep_looping = false;
			got_event = incoming_events_->ReadyForRead();
			if (!got_event)
			{
				keep_looping = true;
			}
		}

		TLOG_TRACE("NetMonTransportService") << "receiveMessage: Reading buffer header" << TLOG_ENDL;
		auto errflag = false;
		incoming_events_->ReadHeader(errflag);
		if (errflag) { // Buffer was changed out from under reader!
			msg = nullptr;
			return; 
		}
		TLOG_TRACE("NetMonTransportService") << "receiveMessage: Getting Fragment types" << TLOG_ENDL;
		auto fragmentTypes = incoming_events_->GetFragmentTypes(errflag);
		if (errflag) { // Buffer was changed out from under reader!
			msg = nullptr;
			return;
		}
		if (fragmentTypes.size() == 0)
		{
			TLOG_ERROR("NetMonTransportService") << "Event has no Fragments! Aborting!" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			msg = nullptr;
			return;
		}
		TLOG_TRACE("NetMonTransportService") << "receiveMessage: Checking first Fragment type" << TLOG_ENDL;
		auto firstFragmentType = *fragmentTypes.begin();

		// We return false, indicating we're done reading, if:
		//   1) we did not obtain an event, because we timed out and were
		//      configured NOT to keep trying after a timeout, or
		//   2) the event we read was the end-of-data marker: a null
		//      pointer
		if (!got_event || firstFragmentType == artdaq::Fragment::EndOfDataFragmentType)
		{
			TLOG_DEBUG("NetMonTransportService") << "Received shutdown message, returning" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			msg = nullptr;
			return;
		}
		if (firstFragmentType == artdaq::Fragment::InitFragmentType)
		{
			TLOG_DEBUG("NetMonTransportService") << "Cannot receive InitFragments here, retrying" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			continue;
		}
		// EndOfRun and EndOfSubrun Fragments are ignored in NetMonTransportService
		else if (firstFragmentType == artdaq::Fragment::EndOfRunFragmentType || firstFragmentType == artdaq::Fragment::EndOfSubrunFragmentType)
		{
			TLOG_DEBUG("NetMonTransportService") << "Ignoring EndOfRun or EndOfSubrun Fragment" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			continue;
		}

		TLOG_TRACE("NetMonTransportService") << "receiveMessage: Getting all Fragments" << TLOG_ENDL;
		recvd_fragments_ = incoming_events_->GetFragmentsByType(errflag, artdaq::Fragment::InvalidFragmentType);
		/* Events coming out of the EventStore are not sorted but need to be
		   sorted by sequence ID before they can be passed to art.
		*/
		std::sort(recvd_fragments_->begin(), recvd_fragments_->end(),
				  artdaq::fragmentSequenceIDCompare);			
		
		TLOG_TRACE("NetMonTransportService") << "receiveMessage: Releasing buffer" << TLOG_ENDL;
		incoming_events_->ReleaseBuffer();
	}

	TLOG_TRACE("NetMonTransportService") << "receiveMessage: Returning top Fragment" << TLOG_ENDL;
	artdaq::Fragment topFrag = std::move(recvd_fragments_->at(0));
	recvd_fragments_->erase(recvd_fragments_->begin());
	if (recvd_fragments_->size() == 0)
	{
		recvd_fragments_.reset(nullptr);
	}

	TLOG_TRACE("NetMonTransportService") << "receiveMessage: Copying Fragment into TBufferFile" << TLOG_ENDL;
	auto header = topFrag.metadata<artdaq::NetMonHeader>();
	auto buffer = static_cast<char *>(malloc(header->data_length));
	memcpy(buffer, &*topFrag.dataBegin(), header->data_length);
	msg = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);

	TLOG_TRACE("NetMonTransportService") << "receiveMessage END" << TLOG_ENDL;
}

void
NetMonTransportService::
receiveInitMessage(TBufferFile*& msg)
{
	TLOG_TRACE("NetMonTransportService") << "receiveInitMessage BEGIN" << TLOG_ENDL;
	if (recvd_fragments_ == nullptr)
	{
		TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Waiting for available buffer" << TLOG_ENDL;

		bool got_event = false;
		while (!got_event)
		{
			got_event = incoming_events_->ReadyForRead(true);
		}

		TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Reading buffer header" << TLOG_ENDL;
		auto errflag = false;
		incoming_events_->ReadHeader(errflag);
		if (errflag) { // Buffer was changed out from under reader!
			TLOG_ERROR("NetMonTransportService") << "receiveInitMessage: Error receiving message!" << TLOG_ENDL;
			msg = nullptr;
			return;
		}
		TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Getting Fragment types" << TLOG_ENDL;
		auto fragmentTypes = incoming_events_->GetFragmentTypes(errflag);
		if (errflag) { // Buffer was changed out from under reader!
			msg = nullptr;
			TLOG_ERROR("NetMonTransportService") << "receiveInitMessage: Error receiving message!" << TLOG_ENDL;
			return;
		}
		if (fragmentTypes.size() == 0)
		{
			TLOG_ERROR("NetMonTransportService") << "Event has no Fragments! Aborting!" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			msg = nullptr;
			return;
		}
		TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Checking first Fragment type" << TLOG_ENDL;
		auto firstFragmentType = *fragmentTypes.begin();

		// We return false, indicating we're done reading, if:
		//   1) we did not obtain an event, because we timed out and were
		//      configured NOT to keep trying after a timeout, or
		//   2) the event we read was the end-of-data marker: a null
		//      pointer
		if (!got_event || firstFragmentType == artdaq::Fragment::EndOfDataFragmentType)
		{
			TLOG_DEBUG("NetMonTransportService") << "Received shutdown message, returning" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			msg = nullptr;
			return;
		}


		TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Getting all Fragments" << TLOG_ENDL;
		recvd_fragments_ = incoming_events_->GetFragmentsByType(errflag, artdaq::Fragment::InvalidFragmentType);
		/* Events coming out of the EventStore are not sorted but need to be
		sorted by sequence ID before they can be passed to art.
		*/
		std::sort(recvd_fragments_->begin(), recvd_fragments_->end(),
				  artdaq::fragmentSequenceIDCompare);
	}

	TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Returning top Fragment" << TLOG_ENDL;
	artdaq::Fragment topFrag = std::move(recvd_fragments_->at(0));
	recvd_fragments_->erase(recvd_fragments_->begin());
	if (recvd_fragments_->size() == 0)
	{
		recvd_fragments_.reset(nullptr);
	}

	auto header = topFrag.metadata<artdaq::NetMonHeader>();
	TLOG_TRACE("NetMonTransportService") << "receiveInitMessage: Copying Fragment into TBufferFile: message length: " << std::to_string(header->data_length) << TLOG_ENDL;
	auto buffer = static_cast<char *>(malloc(header->data_length));
	memcpy(buffer, &*topFrag.dataBegin(), header->data_length);

#if 0
	std::string fileName = "receiveInitMessage_" + std::to_string(getpid()) + ".bin";
	std::fstream ostream(fileName.c_str(), std::ios::out | std::ios::binary);
	ostream.write(buffer, header->data_length);
	ostream.close();
#endif

	msg = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);

	TLOG_TRACE("NetMonTransportService") << "receiveInitMessage END" << TLOG_ENDL;
}
DEFINE_ART_SERVICE_INTERFACE_IMPL(NetMonTransportService, NetMonTransportServiceInterface)
