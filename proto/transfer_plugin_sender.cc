#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

#include "cetlib/BasicPluginFactory.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>

// DUPLICATED CODE: also found in transfer_plugin_receiver.cpp. Not as
// egregious as normal in that this function is unlikely to be
// changed, and this is a standalone app (not part of artdaq)

fhicl::ParameterSet ReadParameterSet(const std::string& fhicl_filename)
{
	if (std::getenv("FHICL_FILE_PATH") == nullptr)
	{
		std::cerr
		    << "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
		setenv("FHICL_FILE_PATH", ".", 0);
	}

	fhicl::ParameterSet pset;
	cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
	fhicl::make_ParameterSet(fhicl_filename, lookup_policy, pset);

	return pset;
}

int main(int argc, char* argv[]) try
{
	if (argc != 4)
	{
		std::cerr << "Usage: <fhicl document> <number of sends (should be greater than 1) > <fragment payload size>" << std::endl;
		return 1;
	}

	auto fhicl_filename = boost::lexical_cast<std::string>(argv[1]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	auto num_sends = boost::lexical_cast<size_t>(argv[2]);            // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	auto fragment_size = boost::lexical_cast<size_t>(argv[3]);        // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)

	if (num_sends <= 1)
	{
		std::cerr << "Logic in the program requires requested # of sends to be greater than 1" << std::endl;
		return 1;
	}

	std::unique_ptr<artdaq::TransferInterface> transfer;

	auto pset = ReadParameterSet(fhicl_filename);

	try
	{
		static cet::BasicPluginFactory bpf("transfer", "make");

		transfer =
		    bpf.makePlugin<std::unique_ptr<artdaq::TransferInterface>,
		                   const fhicl::ParameterSet&,
		                   artdaq::TransferInterface::Role>(
		        pset.get<std::string>("transfer_plugin_type"),
		        pset,
		        artdaq::TransferInterface::Role::kSend);
	}
	catch (...)
	{
		artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no,
		                         "Error creating transfer plugin");
		return 1;
	}

	std::unique_ptr<artdaq::Fragment> frag = artdaq::Fragment::FragmentBytes(fragment_size);

	struct ArbitraryMetadata
	{
		const uint64_t val1 = 0;
		const uint32_t val2 = 0;
	};

	ArbitraryMetadata arbitraryMetadata;

	frag->setMetadata(arbitraryMetadata);

	// Fill the fragment with monotonically increasing 64-bit integers
	// to be checked on the other end

	std::iota(reinterpret_cast<uint64_t*>(frag->dataBeginBytes()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	          reinterpret_cast<uint64_t*>(frag->dataEndBytes()),    // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
	          0);

	auto timeout = pset.get<size_t>("send_timeout_usecs", std::numeric_limits<size_t>::max());

	for (size_t i_i = 0; i_i < num_sends; ++i_i)
	{
		frag->setSequenceID(i_i + 1);
		frag->setFragmentID(0);
		frag->setUserType(artdaq::Fragment::FirstUserFragmentType);

		transfer->transfer_fragment_min_blocking_mode(*frag, timeout);
	}

	std::cout << "# of sent fragments attempted == " << num_sends << std::endl;

	return 0;
}
catch (...)
{
	return -1;
}
