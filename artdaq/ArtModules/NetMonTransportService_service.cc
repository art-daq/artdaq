#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq/DAQrate/SHandles.hh"
#include "artdaq-core/Core/GlobalQueue.hh"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "art/Framework/Services/Registry/ActivityRegistry.h"
#ifdef CANVAS
#include "canvas/Utilities/Exception.h"
#else
#include "art/Utilities/Exception.h"
#endif
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
    mpi_buffer_count_(pset.get<size_t>("mpi_buffer_count", 5)),
    max_fragment_size_words_(pset.get<uint64_t>("max_fragment_size_words", 512 * 1024)),
    first_data_receiver_rank_(pset.get<size_t>("first_data_receiver_rank", 0)),
    data_receiver_count_(pset.get<size_t>("data_receiver_count", 1)),
    broadcast_sends_(pset.get<bool>("broadcast_sends", false)),
    synchronous_sends_(pset.get<bool>("synchronous_sends", true)),
    sender_ptr_(nullptr),
    incoming_events_(artdaq::getGlobalQueue()),
    recvd_fragments_(nullptr) { }

void
NetMonTransportService::
connect()
{
  sender_ptr_.reset(new artdaq::SHandles(mpi_buffer_count_,
					 max_fragment_size_words_,
					 data_receiver_count_,
					 first_data_receiver_rank_,
					 broadcast_sends_,
                                         synchronous_sends_));
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
    connect();
  }

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
