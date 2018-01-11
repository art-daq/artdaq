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
	fhicl::ParameterSet config = LoadParameterSet(argc, argv);
	std::string fac_name_part = config.get<std::string>("application_name", "EventBuilder");
	std::string app_name = artdaq::setMsgFacAppName(fac_name_part, config.get<int>("id"));
	artdaq::configureMessageFacility(app_name.c_str());
	TLOG_DEBUG(fac_name_part + "Main") << "Setting application name to " << app_name << TLOG_ENDL;

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

	TLOG_INFO(fac_name_part + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	// create the EventBuilderApp
	artdaq::EventBuilderApp evb_app(mpiSentry->rank(), fac_name_part);

	TLOG_DEBUG(fac_name_part + "Main") << "Creating EventBuilder Commander plugin" << TLOG_ENDL;
	auto commander = artdaq::MakeCommanderPlugin(config, evb_app);

	TLOG_INFO(fac_name_part + "Main") << "Running Commmander Server" << TLOG_ENDL;
	commander->run_server();
	TLOG_INFO(fac_name_part + "Main") << "Commandable Server ended, exiting..." << TLOG_ENDL;
}
