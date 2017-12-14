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
#include "artdaq/Application/DispatcherApp.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"

int main(int argc, char * argv[])
{
	std::ostringstream descstr;
	descstr << argv[0]
		<< "<-r <rank>> <-c <config-file>>";
	boost::program_options::options_description desc(descstr.str());
	desc.add_options()
		("config,c", boost::program_options::value<std::string>(), "Configuration file.")
		("rank,r", boost::program_options::value<int>(), "Process Rank")
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

	if (!vm.count("rank"))
	{
		TLOG_ERROR("Option") << argv[0] << " rank not supplied" << std::endl << "For usage and an options list, please do '" << argv[0] << " --help'" << TLOG_ENDL;
		return 2;
	}
	auto rank = vm["rank"].as<int>();


	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "Dispatcher");
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));

	// create the DispatcherApp
	artdaq::DispatcherApp dl_app(rank, name);


	int run = config.get<int>("run_number", 101);
	uint64_t timeout = config.get<uint64_t>("transition_timeout", 30);
	uint64_t timestamp = 0;

	dl_app.do_initialize(config, timeout, timestamp);
	dl_app.do_start(art::RunID(run), timeout, timestamp);

	TLOG_INFO(name) << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
		<< "xmlrpc http://`hostname`:" << vm["port"].as<unsigned short>() << "/RPC2 daq.stop" << std::endl
		<< "xmlrpc http://`hostname`:" << vm["port"].as<unsigned short>() << "/RPC2 daq.shutdown" << TLOG_ENDL;

}
