#include <iostream>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
#include "artdaq/Application/TaskType.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "artdaq/Application/configureMessageFacility.hh"
#include "artdaq/Application/MPI2/AggregatorApp.hh"
#include "artdaq/ExternalComms/xmlrpc_commander.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "cetlib/exception.h"

int main(int argc, char *argv[])
{
  artdaq::configureMessageFacility("aggregator");

  // initialization

  int const wanted_threading_level { MPI_THREAD_FUNNELED };

  MPI_Comm local_group_comm;
  std::unique_ptr<artdaq::MPISentry> mpiSentry;

  try {

    mpiSentry.reset( new artdaq::MPISentry(&argc, &argv, wanted_threading_level, artdaq::TaskType::AggregatorTask, local_group_comm) );

  } catch (cet::exception& errormsg) {
    mf::LogError("AggregatorMain") << errormsg ;
    mf::LogError("AggregatorMain") << "MPISentry error encountered in AggregatorMain; exiting...";
    throw errormsg;
  }


  // handle the command-line arguments
  std::string usage = std::string(argv[0]) + " -p port_number <other-options>";
  boost::program_options::options_description desc(usage);

  desc.add_options ()
    ("port,p", boost::program_options::value<unsigned short>(), "Port number")
    ("help,h", "produce help message");
  
  boost::program_options::variables_map vm;
  try {
    boost::program_options::store (boost::program_options::command_line_parser(argc, argv).options(desc).run(), vm);
    boost::program_options::notify (vm);
  } catch (boost::program_options::error const& e) {
    mf::LogError ("Option") << "exception from command line processing in " << argv[0] << ": " << e.what() << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  if (!vm.count("port")) {
    mf::LogError ("Option") << argv[0] << " port number not suplied" << std::endl << "For usage and an options list, please do '" << argv[0] <<  " --help'" << std::endl;
    return 1;
  }

  artdaq::setMsgFacAppName("Aggregator", vm["port"].as<unsigned short> ()); 

  // create the AggregatorApp
  artdaq::AggregatorApp agg_app(mpiSentry->rank(), local_group_comm );

  // create the xmlrpc_commander and run it
  xmlrpc_commander commander(vm["port"].as<unsigned short> (), agg_app);
  commander.run();

}
