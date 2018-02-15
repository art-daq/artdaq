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
#include "artdaq/Application/RoutingMasterApp.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"

int main(int argc, char * argv[])
{
	fhicl::ParameterSet config_ps = LoadParameterSet(argc, argv);
	app_name = config_ps.get<std::string>("application_name", "RoutingMaster");
	std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));
	artdaq::configureMessageFacility(mf_app_name.c_str());

	auto rank = config_ps.get<int>("rank", 0);
	TLOG_DEBUG(app_name + "Main") << "Setting application name to " << app_name << TLOG_ENDL;

	TLOG_DEBUG(app_name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));
	
	// create the RoutingMasterApp
	artdaq::RoutingMasterApp dl_app(rank, app_name);

	auto auto_run = config_ps.get<bool>("auto_run", false);
	if (auto_run) {
		int run = config_ps.get<int>("run_number", 101);
		uint64_t timeout = config_ps.get<uint64_t>("transition_timeout", 30);
		uint64_t timestamp = 0;

		dl_app.do_initialize(config_ps, timeout, timestamp);
		dl_app.do_start(art::RunID(run), timeout, timestamp);

		TLOG_INFO(app_name) << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
			<< "xmlrpc http://`hostname`:" << config_ps.get<int>("id") << "/RPC2 daq.stop" << std::endl
			<< "xmlrpc http://`hostname`:" << config_ps.get<int>("id") << "/RPC2 daq.shutdown" << TLOG_ENDL;
	}

	auto commander = artdaq::MakeCommanderPlugin(config_ps, dl_app);
	commander->run_server();
}
