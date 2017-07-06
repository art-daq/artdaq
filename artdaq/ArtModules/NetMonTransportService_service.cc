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
#include <string>
#include <vector>

static fhicl::ParameterSet empty_pset;

NetMonTransportService::
~NetMonTransportService()
{
	NetMonTransportService::disconnect();
}

NetMonTransportService::
NetMonTransportService(fhicl::ParameterSet const& pset, art::ActivityRegistry&)
	: NetMonTransportServiceInterface()
	, data_pset_(pset)
	, sender_ptr_(nullptr)
	, incoming_events_(new artdaq::SharedMemoryEventReceiver(pset.get<int>("shared_memory_key", 0xBEE7),
													 pset.get<size_t>("buffer_count", 20),
													 pset.get<size_t>("max_buffer_size", 1024)))
	, recvd_fragments_(nullptr)
{
	if (pset.has_key("rank")) my_rank = pset.get<int>("rank");
	else my_rank = incoming_events_->GetRank();
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

	TLOG_DEBUG("NetMonTransportService") << "Sending message" << TLOG_ENDL;
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
	if (recvd_fragments_ == nullptr)
	{
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

		auto errflag = false;
		incoming_events_->ReadHeader(errflag);
		if (errflag) return; // Buffer was changed out from under reader!
		auto fragmentTypes = incoming_events_->GetFragmentTypes(errflag);
		if (errflag) return; // Buffer was changed out from under reader!
		if (fragmentTypes.size() == 0)
		{
			TLOG_ERROR("NetMonTransportService") << "Event has no Fragments! Aborting!" << TLOG_ENDL;
			incoming_events_->ReleaseBuffer();
			msg = nullptr;
			return;
		}
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



		recvd_fragments_ = incoming_events_->GetFragmentsByType(errflag, artdaq::Fragment::InvalidFragmentType);
		/* Events coming out of the EventStore are not sorted but need to be
		   sorted by sequence ID before they can be passed to art.
		*/
		std::sort(recvd_fragments_->begin(), recvd_fragments_->end(),
				  artdaq::fragmentSequenceIDCompare);
	}

	artdaq::Fragment topFrag = std::move(recvd_fragments_->at(0));
	recvd_fragments_->erase(recvd_fragments_->begin());
	if (recvd_fragments_->size() == 0)
	{
		recvd_fragments_.reset(nullptr);
	}

	auto header = topFrag.metadata<artdaq::NetMonHeader>();
	auto buffer = static_cast<char *>(malloc(header->data_length));
	memcpy(buffer, &*topFrag.dataBegin(), header->data_length);
	msg = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(NetMonTransportService, NetMonTransportServiceInterface)
