#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/BoardReaderApp.hh"
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
	fhicl::ParameterSet config_ps = LoadParameterSet(argc, argv);
	app_name = config_ps.get<std::string>("application_name", "BoardReader");
	std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));
	artdaq::configureMessageFacility(mf_app_name.c_str());

	auto rank = config_ps.get<int>("rank", 0);
	TLOG_DEBUG(app_name + "Main") << "Setting application name to " << app_name << TLOG_ENDL;

	TLOG_DEBUG(app_name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));

	// create the BoardReaderApp
	artdaq::BoardReaderApp br_app(rank, app_name);

	auto auto_run = config_ps.get<bool>("auto_run", false);
	if (auto_run) {
		int run = config_ps.get<int>("run_number", 101);
		uint64_t timeout = config_ps.get<uint64_t>("transition_timeout", 30);
		uint64_t timestamp = 0;

		br_app.do_initialize(config_ps, timeout, timestamp);
		br_app.do_start(art::RunID(run), timeout, timestamp);

		TLOG_INFO(app_name) << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
			<< "xmlrpc http://`hostname`:" << config_ps.get<int>("id") << "/RPC2 daq.stop" << std::endl
			<< "xmlrpc http://`hostname`:" << config_ps.get<int>("id") << "/RPC2 daq.shutdown" << TLOG_ENDL;
	}

	auto commander = artdaq::MakeCommanderPlugin(config_ps, br_app);
	commander->run_server();
}
