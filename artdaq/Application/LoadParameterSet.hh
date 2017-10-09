#ifndef artdaq_proto_LoadParameterSet_hh
#define artdaq_proto_LoadParameterSet_hh 1

#include "fhiclcpp/make_ParameterSet.h"
#include <boost/program_options.hpp>
#include <iostream>
namespace bpo = boost::program_options;

inline fhicl::ParameterSet LoadParameterSet(int argc, char* argv[]) {
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
		exit(-1);
	}
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		exit(1);
	}
	if (!vm.count("config")) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": no configuration file given.\n"
			<< "For usage and an options list, please do '"
			<< argv[0] << " --help"
			<< "'.\n";
		exit(2);
	}
	fhicl::ParameterSet pset;
	if (getenv("FHICL_FILE_PATH") == nullptr) {
		std::cerr
			<< "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
		setenv("FHICL_FILE_PATH", ".", 0);
	}
	cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
	make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);
	return pset;
}
#endif //artdaq_proto_LoadParameterSet_hh