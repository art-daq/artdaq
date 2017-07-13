
#include <sstream>
#include <boost/program_options.hpp>
namespace bpo = boost::program_options;

#include "fhiclcpp/make_ParameterSet.h"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"

int main(int argc, char* argv[])
{

	artdaq::configureMessageFacility("PrintSharedMemory");

	std::ostringstream descstr;
	descstr << argv[0]
		<< " <-c <config-file>> <other-options> [<source-file>]+";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("config,c", bpo::value<std::string>(), "Configuration file.")
		("help,h", "produce help message");
	bpo::variables_map vm;
	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const & e) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": " << e.what() << "\n";
		return -1;
	}
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 1;
	}
	if (!vm.count("config")) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": no configuration file given.\n"
			<< "For usage and an options list, please do '"
			<< argv[0] << " --help"
			<< "'.\n";
		return 2;
	}
	fhicl::ParameterSet pset;
	if (getenv("FHICL_FILE_PATH") == nullptr) {
		std::cerr
			<< "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
		setenv("FHICL_FILE_PATH", ".", 0);
	}
	cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
	fhicl::make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);

	if(pset.get<bool>("ReadEventInfo", false))
	{
		artdaq::SharedMemoryEventManager t(pset, pset.to_string());
		std::cout << t.toString() << std::endl;
	}
	else
	{
		artdaq::SharedMemoryManager t(pset.get<int>("shared_memory_key"),
									   pset.get<size_t>("buffer_count"),
									   pset.get<size_t>("max_event_size_bytes"),
									   pset.get<size_t>("stale_buffer_timeout_usec", 1000000));
		std::cout << t.toString() << std::endl;
	}
}
