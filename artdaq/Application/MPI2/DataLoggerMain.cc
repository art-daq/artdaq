#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "artdaq/Application/TaskType.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/DataLoggerApp.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "cetlib_except/exception.h"

int main(int argc, char* argv[])
{
	// initialization

	int const wanted_threading_level{ MPI_THREAD_FUNNELED };

	MPI_Comm local_group_comm;
	std::unique_ptr<artdaq::MPISentry> mpiSentry;

	try
	{
		mpiSentry.reset(new artdaq::MPISentry(&argc, &argv, wanted_threading_level, artdaq::TaskType::AggregatorTask, local_group_comm));
	}
	catch (cet::exception& errormsg)
	{
		TLOG_ERROR("DataLoggerMain") << errormsg << TLOG_ENDL;
		TLOG_ERROR("DataLoggerMain") << "MPISentry error encountered in DataLoggerMain; exiting..." << TLOG_ENDL;
		throw errormsg;
	}

	fhicl::ParameterSet config = LoadParameterSet(argc, argv);
	app_name = config.get<std::string>("application_name", "DataLogger");
	std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config.get<int>("id"));
	artdaq::configureMessageFacility(mf_app_name.c_str());
	TLOG_DEBUG(app_name + "Main") << "Setting application name to " << mf_app_name << TLOG_ENDL;


	TLOG_INFO(app_name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	// create the DataLoggerApp
	artdaq::DataLoggerApp dl_app(mpiSentry->rank(), app_name);

	auto commander = artdaq::MakeCommanderPlugin(config, dl_app);
	TLOG_INFO(app_name + "Main") << "Running Commmander Server" << TLOG_ENDL;
	commander->run_server();
	TLOG_INFO(app_name + "Main") << "Commandable Server ended, exiting..." << TLOG_ENDL;
}
