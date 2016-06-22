
#include "artdaq/TransferPlugins/TransferInterface.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "cetlib/BasicPluginFactory.h"
#include "cetlib/exception.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <boost/asio.hpp>
#include "boost/bind.hpp"

#include <iostream>
#include <string>


// DUPLICATED CODE: also found in sender.cpp. Not as egregious as
// normal in that this function is unlikely to be changed, and this is
// a standalone app (not part of artdaq)

fhicl::ParameterSet ReadParameterSet() {
 
  if (std::getenv("FHICL_FILE_PATH") == nullptr) {
    std::cerr
      << "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
    setenv("FHICL_FILE_PATH", ".", 0);
  }

  fhicl::ParameterSet pset;
  cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
  fhicl::make_ParameterSet("multicast.fcl", lookup_policy, pset);

  return pset.get<fhicl::ParameterSet>("multicast");
}

int do_check(const artdaq::Fragment& frag);

int main()
{

  std::unique_ptr<artdaq::TransferInterface> transfer;
  auto pset = ReadParameterSet();

  try {
    static cet::BasicPluginFactory bpf("transfer", "make");

    transfer =  
      bpf.makePlugin<std::unique_ptr<artdaq::TransferInterface>,
      const fhicl::ParameterSet&,
      artdaq::TransferInterface::Role>(
				       "multicast", 
				       pset, 
				       artdaq::TransferInterface::Role::receive);
  } catch(...) {
    artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::yes,
			     "Error creating transfer plugin");
  }


  while (true) {

    artdaq::Fragment myfrag;
    size_t dummy_timeout = 0;

    transfer->receiveFragmentFrom(myfrag, dummy_timeout);

    std::cout << "Returned from call to transfer_->receiveFragmentFrom; fragment with seqID == " <<
      myfrag.sequenceID() << ", fragID == " << myfrag.fragmentID() << " has size " << 
      myfrag.sizeBytes() << " bytes" << std::endl;

    if (do_check(myfrag) != 0) {
      std::cerr << "Error: do_check indicates fragment failed to transmit correctly" << std::endl;
    } else {
      std::cerr << "Success: do_check indicates fragment transmitted correctly" << std::endl;
    }

  }

  return 0;
}

// JCF, Jun-22-2016

// do_check assumes std::iota was used to fill the sent fragment with
// monotonically incrementing 64-bit unsigned integers

int do_check(const artdaq::Fragment& frag) {
  
  uint64_t variable_to_compare = 0;

  for (auto ptr_into_frag = reinterpret_cast<const uint64_t*>(frag.dataBeginBytes());
       ptr_into_frag != reinterpret_cast<const uint64_t*>(frag.dataEndBytes());
       ++ptr_into_frag, ++variable_to_compare) {
  
    if (variable_to_compare != *ptr_into_frag) {
      std::cerr << "ERROR for fragment with sequence ID " << frag.sequenceID() << ", fragment ID " <<
	frag.fragmentID() << ": expected " << variable_to_compare << ", got " << *ptr_into_frag << std::endl;
      return 1;
    }
  }

  return 0;
}
