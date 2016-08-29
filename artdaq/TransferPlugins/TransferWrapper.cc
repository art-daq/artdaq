
#include "artdaq/TransferPlugins/TransferWrapper.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "cetlib/BasicPluginFactory.h"
#include "fhiclcpp/ParameterSet.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include <xmlrpc-c/girerr.hpp>
#include <xmlrpc-c/base.hpp>
#include <xmlrpc-c/client_simple.hpp>

#include "TBufferFile.h"

#include <limits>
#include <iostream>
#include <string>

artdaq::TransferWrapper::TransferWrapper(const fhicl::ParameterSet& pset) :
  timeoutInUsecs_(pset.get<std::size_t>("timeoutInUsecs", 100000))
{

  static cet::BasicPluginFactory bpf("transfer", "make");
  
  try {
    transfer_ =  
      bpf.makePlugin<std::unique_ptr<TransferInterface>,
      const fhicl::ParameterSet&,
      TransferInterface::Role>(
			       pset.get<std::string>("transferPluginType"), 
			       pset, 
			       TransferInterface::Role::receive);

  } catch (...) {
    ExceptionHandler(ExceptionHandlerRethrow::yes, 
		     "Problem creating instance of TransferInterface in TransferWrapper");
  }

  auto dispatcherHost = pset.get<std::string>("dispatcherHost");
  auto dispatcherPort = pset.get<std::string>("dispatcherPort");
  std::string serverUrl = "http://" + dispatcherHost + ":" + dispatcherPort + "/RPC2";

  xmlrpc_c::clientSimple myClient;
  xmlrpc_c::value result;
        
  myClient.call(serverUrl, "daq.register_monitor", "s", &result, "TransferWrapper passed argument to register_monitor");

  const std::string status = xmlrpc_c::value_string(result);

  mf::LogInfo("TransferWrapper") << "Response from dispatcher is " << status;
}

void artdaq::TransferWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg) {

  std::unique_ptr<artdaq::Fragment> fragmentPtr;
  bool receivedFragment = false;
  static bool initialized = false;

  while (true) {

    fragmentPtr = std::make_unique<artdaq::Fragment>();

    while (!receivedFragment) {

      try { 
	auto result = transfer_->receiveFragmentFrom(*fragmentPtr, timeoutInUsecs_);
      
	if (result != artdaq::RHandles::RECV_TIMEOUT) {
	  receivedFragment = true;
	  continue; 
	} else {
	  mf::LogWarning("TransferWrapper") << "Timeout occurred in call to transfer_->receiveFragmentFrom; will try again";
	}

      } catch (...) {
	ExceptionHandler(ExceptionHandlerRethrow::yes, 
			 "Problem receiving data in TransferWrapper::receiveMessage");
      }
    }

    try {
      extractTBufferFile(*fragmentPtr, msg);
    } catch (...) {
      ExceptionHandler(ExceptionHandlerRethrow::yes, 
		       "Problem extracting TBufferFile from artdaq::Fragment in TransferWrapper::receiveMessage");
    }

    if (initialized || fragmentPtr->type() == artdaq::Fragment::InitFragmentType) {
      initialized = true;
      break;
    } else {
      receivedFragment = false;
    }
  }

  static size_t cntr = 1;

  mf::LogInfo("TransferWrapper") << "Received " << cntr++ << "-th event, "
				 << "seqID == " << fragmentPtr->sequenceID() 
				 << ", type == " << static_cast<int>(fragmentPtr->type());
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


