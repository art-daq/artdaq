
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

const short multicast_port = 30001;
const bool gVerbose = false;

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


class sender
{
public:

  sender(const size_t num_sends)
    : transfer_(nullptr),
      fragment_size_(1e6)
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
					 artdaq::TransferInterface::Role::send);
    } catch(...) {
      artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::yes,
			       "Error creating transfer plugin");
    }

    for (size_t i_i = 0; i_i < num_sends; ++i_i) {
      create_and_send_fragment();
    }
  }

  void create_and_send_fragment() 
  {
	
    frag_.reset(nullptr);
    frag_ = std::move( artdaq::Fragment::FragmentBytes( fragment_size_ ) );

    bool dummy_bool = true;
    size_t dummy_size_t = std::numeric_limits<size_t>::max();

    transfer_->copyFragmentTo(dummy_bool, dummy_bool, dummy_bool, *frag_, dummy_size_t);
  }

private:

  std::unique_ptr<artdaq::Fragment> frag_;
  std::unique_ptr<artdaq::TransferInterface> transfer_;

  const size_t fragment_size_;
};


int main(int argc, char* argv[])
{
  try
    {
      if (argc > 2)
	{
	  std::cerr << "Usage: sender (number of sends - default is 1)\n";
	  return 1;
	}

      size_t num_sends = (argc == 2) ? boost::lexical_cast<size_t>( argv[1] ) : 1;

      sender s(num_sends);

      std::cout << "# of sent fragments attempted == " << num_sends << std::endl;
    }
  catch (std::exception& e)
    {
      std::cerr << "Exception: " << e.what() << "\n";
    }

  return 0;
}
