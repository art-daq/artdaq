#define TRACE_NAME "requestReceiver"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include "artdaq/DAQrate/RequestReceiver.hh"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("requestReceiver");

	auto pset = LoadParameterSet<artdaq::RequestReceiver::RequestReceiverConfig>(argc, argv, "receiver");

	int rc = 0;

	artdaq::RequestReceiver recvr(pset);
	recvr.startRequestReceiverThread();

	while (true)
	{
		for (auto req : recvr.GetRequests())
		{
			TLOG(TLVL_INFO) << "Received Request for Sequence ID " << std::to_string(req.first) << ", timestamp " << std::to_string(req.second) ;
		}
		recvr.ClearRequests();
		usleep(10000);
	}

	return rc;
}
