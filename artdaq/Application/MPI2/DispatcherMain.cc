#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "artdaq/Application/TaskType.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/DispatcherApp.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "cetlib_except/exception.h"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("dispatcher");

	// initialization

	int const wanted_threading_level{MPI_THREAD_FUNNELED};

	MPI_Comm local_group_comm;
	std::unique_ptr<artdaq::MPISentry> mpiSentry;

	try
	{
		mpiSentry.reset(new artdaq::MPISentry(&argc, &argv, wanted_threading_level, artdaq::TaskType::AggregatorTask, local_group_comm));
	}
	catch (cet::exception& errormsg)
	{
		TLOG_ERROR("DispatcherMain") << errormsg << TLOG_ENDL;
		TLOG_ERROR("DispatcherMain") << "MPISentry error encountered in DispatcherMain; exiting..." << TLOG_ENDL;
		throw errormsg;
	}


	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "Dispatcher");
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_INFO(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));

	// create the DispatcherApp
	artdaq::DispatcherApp disp_app(mpiSentry->rank(), name);

	auto commander = artdaq::MakeCommanderPlugin(config, disp_app);
	TLOG_INFO(name + "Main") << "Running Commmander Server" << TLOG_ENDL;
	commander->run_server();
	TLOG_INFO(name + "Main") << "Commandable Server ended, exiting..." << TLOG_ENDL;
}
