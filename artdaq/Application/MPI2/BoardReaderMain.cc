#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/BoardReaderApp.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
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


	fhicl::ParameterSet config = LoadParameterSet(argc, argv);

	std::string name = config.get<std::string>("application_name", "BoardReader");
	TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;

	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	artdaq::setMsgFacAppName(name, config.get<int>("id"));

	// create the BoardReaderApp
	artdaq::BoardReaderApp br_app(mpiSentry->rank(), name);

	auto commander = artdaq::MakeCommanderPlugin(config, br_app);
	commander->run_server();
}