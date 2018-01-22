#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/BoardReaderApp.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "cetlib_except/exception.h"

#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>

#include <iostream>
#include <memory>

int main(int argc, char* argv[])
{

	// initialization
	int const wanted_threading_level{ MPI_THREAD_FUNNELED };

	MPI_Comm local_group_comm;
	std::unique_ptr<artdaq::MPISentry> mpiSentry;

	try
	{
		mpiSentry.reset(new artdaq::MPISentry(&argc, &argv, wanted_threading_level, artdaq::TaskType::BoardReaderTask, local_group_comm));
	}
	catch (cet::exception& errormsg)
	{
		TLOG_ERROR("BoardReaderMain") << errormsg << TLOG_ENDL;
		TLOG_ERROR("BoardReaderMain") << "MPISentry error encountered in BoardReaderMain; exiting..." << TLOG_ENDL;
		throw errormsg;
	}

	fhicl::ParameterSet config_ps = LoadParameterSet(argc, argv);
	app_name = config_ps.get<std::string>("application_name", "BoardReader");
	std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));
	artdaq::configureMessageFacility(mf_app_name.c_str());
	TLOG_DEBUG(app_name + "Main") << "Setting application name to " << mf_app_name << TLOG_ENDL;


	TLOG_INFO(app_name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	// create the BoardReaderApp
	artdaq::BoardReaderApp br_app(mpiSentry->rank(), app_name);

	auto commander = artdaq::MakeCommanderPlugin(config_ps, br_app);
	TLOG_INFO(app_name + "Main") << "Running Commmander Server" << TLOG_ENDL;
	commander->run_server();
	TLOG_INFO(app_name + "Main") << "Commandable Server ended, exiting..." << TLOG_ENDL;
}
