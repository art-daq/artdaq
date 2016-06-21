
#include "artdaq/TransferPlugins/TransferInterface.h"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "cetlib/BasicPluginFactory.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <boost/asio.hpp>
#include "boost/bind.hpp"
#include <boost/lexical_cast.hpp>

#include <iostream>
#include <sstream>
#include <string>
#include <cstdlib>
#include <memory>
#include <limits>


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



int main(int argc, char* argv[])
{

  if (argc > 3)
    {
      std::cerr << "Usage: sender (number of sends - default is 1) (fragment size)\n";
      return 1;
    }

  size_t num_sends = (argc >= 2) ? boost::lexical_cast<size_t>( argv[1] ) : 1;
  size_t fragment_size = (argc == 3) ? boost::lexical_cast<size_t>( argv[2] ) : 1000000;

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
				       artdaq::TransferInterface::Role::send);
  } catch(...) {
    artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::yes,
			     "Error creating transfer plugin");
  }

  std::unique_ptr<artdaq::Fragment> frag = artdaq::Fragment::FragmentBytes( fragment_size );

  for (size_t i_i = 0; i_i < num_sends; ++i_i) {

    //    frag.reset( std::move( artdaq::Fragment::FragmentBytes( fragment_size ) ) );

    bool dummy_bool = true;
    size_t dummy_size_t = std::numeric_limits<size_t>::max();

    transfer->copyFragmentTo(dummy_bool, dummy_bool, dummy_bool, *frag, dummy_size_t);
  }

  std::cout << "# of sent fragments attempted == " << num_sends << std::endl;

  return 0;
}
