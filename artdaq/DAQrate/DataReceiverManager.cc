#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataReceiverManager::DataReceiverManager(fhicl::ParameterSet pset)
  : sources_()
  , current_source_(0)
  , recv_frag_count_(0)
{
  auto srcs = pset.get<fhicl::ParameterSet>("sources");
  for(auto& s : srcs.get_pset_names()) {
	try { 
	  auto ss = std::stoi(s);
      sources_.emplace(ss, MakeTransferPlugin(srcs.get<fhicl::ParameterSet>(s), s, TransferInterface::Role::kReceive));
	}
	catch(std::invalid_argument) {
	  TRACE(3, "Invalid source specification: " + s);
	}
  }
  current_source_ = (*(sources_.begin())).first;
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
}

size_t artdaq::DataReceiverManager::recvFragment( Fragment& frag, size_t timeout_usec)
{
  TRACE( 6,"recvFragment entered tmo=%lu us, frag.sizeofdata=%zu",timeout_usec, frag.size()  );
  size_t source = current_source_;
  auto next_iter = ++(sources_.find(source));
  if(next_iter == sources_.end()) next_iter = sources_.begin();
  current_source_ = (*next_iter).first;
  sources_[source]->receiveFragmentFrom(frag, timeout_usec);
  return source;
}
