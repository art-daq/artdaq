#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/RoutingMasterApp.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "cetlib_except/exception.h"
#include "artdaq/DAQdata/Globals.hh"

int main(int argc, char* argv[])
{
	fhicl::ParameterSet config_ps = LoadParameterSet(argc, argv);
	std::string fac_name_part = config_ps.get<std::string>("application_name", "RoutingMaster");
	std::string app_name = artdaq::setMsgFacAppName(fac_name_part, config_ps.get<int>("id"));
	artdaq::configureMessageFacility(app_name.c_str());
	TLOG_DEBUG(fac_name_part + "Main") << "Setting application name to " << app_name << TLOG_ENDL;

	// initialization

	int const wanted_threading_level{ MPI_THREAD_FUNNELED };

	MPI_Comm local_group_comm;
	std::unique_ptr<artdaq::MPISentry> mpiSentry;

	try
	{
		mpiSentry.reset(new artdaq::MPISentry(&argc, &argv, wanted_threading_level, artdaq::TaskType::RoutingMasterTask, local_group_comm));
	}
	catch (cet::exception& errormsg)
	{
		TLOG_ERROR("RoutingMasterMain") << errormsg << TLOG_ENDL;
		TLOG_ERROR("RoutingMasterMain") << "MPISentry error encountered in RoutingMasterMain; exiting..." << TLOG_ENDL;
		throw errormsg;
	}

	TLOG_INFO(fac_name_part + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	// create the RoutingMasterApp
	artdaq::RoutingMasterApp rm_app(mpiSentry->rank(), fac_name_part);

	auto commander = artdaq::MakeCommanderPlugin(config_ps, rm_app);
	TLOG_INFO(fac_name_part + "Main") << "Running Commmander Server" << TLOG_ENDL;
	commander->run_server();
	TLOG_INFO(fac_name_part + "Main") << "Commandable Server ended, exiting..." << TLOG_ENDL;
}
