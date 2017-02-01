#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq-core/Core/GlobalQueue.hh"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "art/Framework/Services/Registry/ActivityRegistry.h"
#include "canvas/Utilities/Exception.h"
#include "cetlib/container_algorithms.h"
#include "cetlib/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetRegistry.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "TClass.h"
#include "TBufferFile.h"

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

using namespace cet;
using namespace fhicl;
using namespace std;

static ParameterSet empty_pset;

NetMonTransportService::
~NetMonTransportService()
{
    disconnect();
}

NetMonTransportService::
NetMonTransportService(ParameterSet const& pset, art::ActivityRegistry&)
  : NetMonTransportServiceInterface(),
	data_pset_(pset),
    sender_ptr_(nullptr),
    incoming_events_(artdaq::getGlobalQueue()),
    recvd_fragments_(nullptr) { }

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
sendMessage(uint64_t sequenceId, uint8_t messageType, TBufferFile & msg)
{
  if (sender_ptr_ == nullptr) {
	mf::LogDebug("NetMonTransportService") << "Reconnecting DataSenderManager";
    connect();
  }

  mf::LogDebug("NetMonTransportService") << "Sending message";
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
receiveMessage(TBufferFile *&msg)
{
  if (recvd_fragments_ == nullptr) {
    std::shared_ptr<artdaq::RawEvent> popped_event;
    incoming_events_.deqWait(popped_event);

    if (popped_event == nullptr) {
      msg = nullptr;
      return;
    }

    recvd_fragments_ = popped_event->releaseProduct();
    /* Events coming out of the EventStore are not sorted but need to be
       sorted by sequence ID before they can be passed to art.
    */
    std::sort (recvd_fragments_->begin(), recvd_fragments_->end(),
         artdaq::fragmentSequenceIDCompare);
  }

  artdaq::Fragment topFrag = std::move(recvd_fragments_->at(0));
  recvd_fragments_->erase(recvd_fragments_->begin());
  if (recvd_fragments_->size() == 0) {
    recvd_fragments_.reset(nullptr);
  }

  artdaq::NetMonHeader *header = topFrag.metadata<artdaq::NetMonHeader>();
  char *buffer = (char *)malloc(header->data_length);
  memcpy(buffer, &*topFrag.dataBegin(), header->data_length);
  msg = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(NetMonTransportService,
                                  NetMonTransportServiceInterface)
