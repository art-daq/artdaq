
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

const short multicast_port = 30001;
const bool gVerbose = true;


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


class receiver
{
public:
  receiver()
  {

    auto pset = ReadParameterSet();

    try {
      static cet::BasicPluginFactory bpf("transfer", "make");

      transfer_ =  
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

      transfer_->receiveFragmentFrom(myfrag, dummy_timeout);

      std::cout << "Returned from call to transfer_->receiveFragmentFrom; fragment is " << 
	myfrag.sizeBytes() << " bytes" << std::endl;
    }
 
    
  }


private:

  std::unique_ptr<artdaq::TransferInterface> transfer_;

};


int main()
{
  try
    {
      receiver r;
    }
  catch (std::exception& e)
    {
      std::cerr << "Exception: " << e.what() << "\n";
    }

  return 0;
}
