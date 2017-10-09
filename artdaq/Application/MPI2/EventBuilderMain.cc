#include <iostream>
#include <memory>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/EventBuilderApp.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "cetlib_except/exception.h"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("eventbuilder");

	// initialization

	int const wanted_threading_level{MPI_THREAD_MULTIPLE};
	MPI_Comm local_group_comm;
	std::unique_ptr<artdaq::MPISentry> mpiSentry;

	try
	{
		mpiSentry.reset(new artdaq::MPISentry(&argc, &argv, wanted_threading_level, artdaq::TaskType::EventBuilderTask, local_group_comm));
	}
	catch (cet::exception& errormsg)
	{
		TLOG_ERROR("EventBuilderMain") << errormsg << TLOG_ENDL;
		TLOG_ERROR("EventBuilderMain") << "MPISentry error encountered in EventBuilderMain; exiting..." << TLOG_ENDL;
		throw errormsg;
	}

	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "EventBuilder");
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));

	// create the EventBuilderApp
	artdaq::EventBuilderApp evb_app(mpiSentry->rank(), name);

	auto commander = artdaq::MakeCommanderPlugin(config, evb_app);
	commander->run_server();
}
