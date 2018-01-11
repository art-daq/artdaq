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

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

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

	artdaq::configureMessageFacility("datalogger");

	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "Dispatcher");
	auto rank = config.get<int>("rank", 0);
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));
	
	// create the DispatcherApp
	artdaq::DispatcherApp dl_app(rank, name);


	auto auto_run = config.get<bool>("auto_run", false);
	if (auto_run) {
		int run = config.get<int>("run_number", 101);
		uint64_t timeout = config.get<uint64_t>("transition_timeout", 30);
		uint64_t timestamp = 0;

		dl_app.do_initialize(config, timeout, timestamp);
		dl_app.do_start(art::RunID(run), timeout, timestamp);

		TLOG_INFO(name) << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
			<< "xmlrpc http://`hostname`:" << config.get<int>("id") << "/RPC2 daq.stop" << std::endl
			<< "xmlrpc http://`hostname`:" << config.get<int>("id") << "/RPC2 daq.shutdown" << TLOG_ENDL;
	}

	auto commander = artdaq::MakeCommanderPlugin(config, dl_app);
	commander->run_server();
}
