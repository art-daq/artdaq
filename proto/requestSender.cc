#define TRACE_NAME "RequestSender"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include "artdaq/DAQrate/RequestSender.hh"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("RequestSender");

	auto pset = LoadParameterSet<artdaq::RequestSender::Config>(argc, argv, "sender", "This test application sends Data Request messages and optionally receives them to detect issues in the network transport");

	int rc = 0;

	artdaq::RequestSender sender(pset);


	return rc;
}
