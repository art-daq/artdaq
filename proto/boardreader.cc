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
	artdaq::configureMessageFacility("boardreader");
	
	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "BoardReader");
	auto rank = config.get<int>("rank", 0);
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));

	// create the BoardReaderApp
	artdaq::BoardReaderApp br_app(rank, name);

	auto commander = artdaq::MakeCommanderPlugin(config, br_app);
	commander->run_server();
}
