#include "artdaq/Application/Commandable.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/Application/LoadParameterSet.hh"

#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>

#include <iostream>

int main(int argc, char* argv[])
{
	// initialization
	int const wanted_threading_level{MPI_THREAD_FUNNELED};
	artdaq::MPISentry mpiSentry(&argc, &argv, wanted_threading_level);
	artdaq::configureMessageFacility("commandable");
	TLOG_DEBUG("Commandable::main")
		<< "MPI initialized with requested thread support level of "
		<< wanted_threading_level << ", actual support level = "
		<< mpiSentry.threading_level() << "." << TLOG_ENDL;
	TLOG_DEBUG("Commandable::main")
		<< "size = "
		<< mpiSentry.procs()
		<< ", rank = "
		<< mpiSentry.rank() << TLOG_ENDL;


	fhicl::ParameterSet config = LoadParameterSet(argc, argv);


	artdaq::setMsgFacAppName("Commandable", config.get<int>("id"));

	// create the Commandable object
	artdaq::Commandable commandable;
	
	auto commander = artdaq::MakeCommanderPlugin(config, commandable);
	commander->run_server();
}
