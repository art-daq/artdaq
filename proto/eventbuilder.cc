#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/EventBuilderApp.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "cetlib_except/exception.h"

#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>

#include <iostream>
#include <memory>

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("EventBuilder");

	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "EventBuilder");
	auto rank = config.get<int>("rank", 0);
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));

	// create the EventBuilderApp
	artdaq::EventBuilderApp eb_app(rank, name);


	auto auto_run = config.get<bool>("auto_run", false);
	if (auto_run) {
		int run = config.get<int>("run_number", 101);
		uint64_t timeout = config.get<uint64_t>("transition_timeout", 30);
		uint64_t timestamp = 0;

		eb_app.do_initialize(config, timeout, timestamp);
		eb_app.do_start(art::RunID(run), timeout, timestamp);

		TLOG_INFO(name) << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
			<< "xmlrpc http://`hostname`:" << config.get<int>("id") << "/RPC2 daq.stop" << std::endl
			<< "xmlrpc http://`hostname`:" << config.get<int>("id") << "/RPC2 daq.shutdown" << TLOG_ENDL;
	}

	auto commander = artdaq::MakeCommanderPlugin(config, eb_app);
	commander->run_server();
}
