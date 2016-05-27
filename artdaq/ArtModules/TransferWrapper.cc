
#include "artdaq/ArtModules/TransferWrapper.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "cetlib/BasicPluginFactory.h"
#include "fhiclcpp/ParameterSet.h"

#include "TBufferFile.h"

#include <limits>
#include <iostream>

artdaq::TransferWrapper::TransferWrapper(const std::string transferPluginName)
{

  static cet::BasicPluginFactory bpf("transfer", "make");
  
  fhicl::ParameterSet dummyset;

  try {
    transfer_ = bpf.makePlugin<std::unique_ptr<TransferInterface>,const fhicl::ParameterSet&>(transferPluginName, dummyset);
  } catch (...) {
    ExceptionHandler(ExceptionHandlerRethrow::yes, 
		     "Problem creating instance of TransferInterface in TransferWrapper");
  }
}

void artdaq::TransferWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg) {

  artdaq::Fragment frag;

  std::size_t timeout = 1000000;

  std::cout << "About to call transfer_->receiveFragmentFrom with timeout of " << timeout << std::endl;

  try { 
    transfer_->receiveFragmentFrom(frag, timeout);
  } catch (...) {
    ExceptionHandler(ExceptionHandlerRethrow::yes, 
		     "Problem receiving data in TransferWrapper::receiveMessage");
  }

  std::cout << "Done with call to transfer_->receiveFragmentFrom" << std::endl;

  try {
    extractTBufferFile(frag, msg);
  } catch (...) {
    ExceptionHandler(ExceptionHandlerRethrow::yes, 
		     "Problem extracting TBufferFile from artdaq::Fragment in TransferWrapper::receiveMessage");
  }
}


void 
artdaq::TransferWrapper::extractTBufferFile(const artdaq::Fragment& fragment, 
					    std::unique_ptr<TBufferFile>& tbuffer) {

  const artdaq::NetMonHeader *header = fragment.metadata<artdaq::NetMonHeader>();
  char *buffer = (char *)malloc(header->data_length);
  memcpy(buffer, fragment.dataBeginBytes(), header->data_length);

  // TBufferFile takes ownership of the contents of memory passed to it
  tbuffer.reset( new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0) );
}


