//
// artdaqDriver is a program for testing the behavior of the generic
// RawInput source. Run 'artdaqDriver --help' to get a description of the
// expected command-line parameters.
//
//
// The current version generates simple data fragments, for testing
// that data are transmitted without corruption from the
// artdaq::EventStore through to the artdaq::RawInput source.
//

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "cetlib/container_algorithms.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"
#include <boost/program_options.hpp>

#include <signal.h>
#include <iostream>
#include <memory>
#include <utility>
#include "artdaq/Application/DataLoggerApp.hh"
#include "artdaq/ExternalComms/xmlrpc_commander.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"

int main(int argc, char * argv[])
{
	std::ostringstream descstr;
	descstr << argv[0]
		<< " <-p <port-number>> <-r <rank>> [-n <name>] [-c <config-file>]";
	boost::program_options::options_description desc(descstr.str());
	desc.add_options()
		("config,c", boost::program_options::value<std::string>(), "Configuration file.")
		("rank,r", boost::program_options::value<int>(), "Process Rank")
		("port,p", boost::program_options::value<unsigned short>(), "Port number")
		("name,n", boost::program_options::value<std::string>(), "Application Nickname")
		("help,h", "produce help message");
	boost::program_options::variables_map vm;
	try {
		boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(desc).run(), vm);
		boost::program_options::notify(vm);
	}
	catch (boost::program_options::error const & e) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": " << e.what() << "\n";
		return -1;
	}
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 1;
	}

	if (!vm.count("port"))
	{
		TLOG_ERROR("Option") << argv[0] << " port number not supplied" << std::endl << "For usage and an options list, please do '" << argv[0] << " --help'" << TLOG_ENDL;
		return 1;
	}

	if (!vm.count("rank"))
	{
		TLOG_ERROR("Option") << argv[0] << " rank not supplied" << std::endl << "For usage and an options list, please do '" << argv[0] << " --help'" << TLOG_ENDL;
		return 2;
	}
	auto rank = vm["rank"].as<int>();

	std::string name = "DataLogger";
	if (vm.count("name"))
	{
		name = vm["name"].as<std::string>();
		TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;
	}

	artdaq::setMsgFacAppName(name, vm["port"].as<unsigned short>());
	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	// create the AggregatorApp
	artdaq::DataLoggerApp agg_app(rank, name);

	if (vm.count("config")) {
		fhicl::ParameterSet pset;
		if (getenv("FHICL_FILE_PATH") == nullptr) {
			std::cerr
				<< "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
			setenv("FHICL_FILE_PATH", ".", 0);
		}
		cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
		make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);

		int run = pset.get<int>("run_number", 101);
		uint64_t timeout = pset.get<uint64_t>("transition_timeout", 30);
		uint64_t timestamp = 0;

		agg_app.do_initialize(pset, timeout, timestamp);
		agg_app.do_start(art::RunID(run), timeout, timestamp);

		TLOG_INFO(name) << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
			<< "xmlrpc http://`hostname`:" << vm["port"].as<unsigned short>() << "/RPC2 daq.stop" << std::endl
			<< "xmlrpc http://`hostname`:" << vm["port"].as<unsigned short>() << "/RPC2 daq.shutdown" << TLOG_ENDL;
	}

	// create the xmlrpc_commander and run it
	artdaq::xmlrpc_commander commander(vm["port"].as<unsigned short>(), agg_app);
	commander.run();

}
