#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "trace.h"

artdaq::DataReceiverManager::DataReceiverManager(fhicl::ParameterSet pset)
  : sources_()
  , current_source_(0)
  , recv_frag_count_()
{
  auto srcs = pset.get<fhicl::ParameterSet>("sources", fhicl::ParameterSet());
  for(auto& s : srcs.get_pset_names()) {
	try { 
	  auto ss = std::stoi(s.substr(1));
      sources_.emplace(ss, MakeTransferPlugin(srcs, s, TransferInterface::Role::kReceive));
	}
	catch(std::invalid_argument) {
	  TRACE(3, "Invalid source specification: " + s);
	}
  }
  if(sources_.size() == 0) {
	mf::LogError("DataReceiverManager") << "No sources configured!";
  }
  else current_source_ = (*(sources_.begin())).first;
}

artdaq::DataReceiverManager::~DataReceiverManager()
{
}

size_t artdaq::DataReceiverManager::calcSource() {
  size_t source = current_source_;
  auto next_iter = ++(sources_.find(source));
  if(next_iter == sources_.end()) next_iter = sources_.begin();
  if(next_iter == sources_.end()) return 0;
  return (*next_iter).first;
}

size_t artdaq::DataReceiverManager::recvFragment( Fragment& frag, size_t timeout_usec)
{
  TRACE( 6,"recvFragment entered tmo=%lu us, frag.sizeofdata=%zu",timeout_usec, frag.size()  );
  current_source_ = calcSource();
  if(sources_.count(current_source_)) {
	sources_[current_source_]->receiveFragmentFrom(frag, timeout_usec);
	recv_frag_count_.incSlot(current_source_);
  }
  return current_source_;
}
