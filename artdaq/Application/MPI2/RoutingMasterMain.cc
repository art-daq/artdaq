#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "artdaq/Application/TaskType.hh"
#include "artdaq/Application/RoutingMasterApp.hh"
#include "artdaq/ExternalComms/xmlrpc_commander.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "cetlib/exception.h"
#include "artdaq/DAQdata/Globals.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("routingmaster");

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


	// handle the command-line arguments
	std::string usage = std::string(argv[0]) + " -p port_number -n name <other-options>";
	boost::program_options::options_description desc(usage);

	desc.add_options()
		("port,p", boost::program_options::value<unsigned short>(), "Port number")
		("name,n", boost::program_options::value<std::string>(), "Application Nickname")
		("help,h", "produce help message");

	boost::program_options::variables_map vm;
	try
	{
		boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(desc).run(), vm);
		boost::program_options::notify(vm);
	}
	catch (boost::program_options::error const& e)
	{
		TLOG_ERROR("Option") << "exception from command line processing in " << argv[0] << ": " << e.what() << TLOG_ENDL;
		return 1;
	}

	if (vm.count("help"))
	{
		std::cout << desc << std::endl;
		return 0;
	}

	if (!vm.count("port"))
	{
		TLOG_ERROR("Option") << argv[0] << " port number not supplied" << std::endl << "For usage and an options list, please do '" << argv[0] << " --help'" << TLOG_ENDL;
		return 1;
	}

	std::string name = "RoutingMaster";
	if (vm.count("name"))
	{
		name = vm["name"].as<std::string>();
		TLOG_DEBUG(name + "Main") << "Setting application name to " << name << TLOG_ENDL;
	}

	artdaq::setMsgFacAppName(name, vm["port"].as<unsigned short>());
	TLOG_DEBUG(name + "Main") << "artdaq version " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
		<< ", built " <<
		artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() << TLOG_ENDL;

	// create the AggregatorApp
	artdaq::RoutingMasterApp rm_app(mpiSentry->rank(), name);

	// create the xmlrpc_commander and run it
	artdaq::xmlrpc_commander commander(vm["port"].as<unsigned short>(), rm_app);
	commander.run();
}
