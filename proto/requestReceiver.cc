#define TRACE_NAME "requestReceiver"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQrate/RequestBuffer.hh"
#include "artdaq/DAQrate/RequestReceiver.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("requestReceiver");

	auto pset = LoadParameterSet<artdaq::RequestReceiver::Config>(argc, argv, "receiver", "This is a simple application which listens for Data Request messages and prints their contents");

	int rc = 0;

	fhicl::ParameterSet tempPset;
	if (pset.has_key("daq"))	{
		fhicl::ParameterSet daqPset = pset.get<fhicl::ParameterSet>("daq");
		for (auto& name : daqPset.get_pset_names())
		{
			auto thisPset = daqPset.get<fhicl::ParameterSet>(name);
			if (thisPset.has_key("receive_requests"))
			{
				tempPset = thisPset;
			}
		}
	}
	else if (pset.has_key("request_receiver"))
	{
		tempPset = pset.get<fhicl::ParameterSet>("request_receiver");
	}
	else
	{
		tempPset = pset;
	}

	auto buffer = std::make_shared<artdaq::RequestBuffer>(tempPset.get<artdaq::Fragment::sequence_id_t>("request_increment", 1));
	artdaq::RequestReceiver recvr(tempPset, buffer);
	recvr.startRequestReception();

	while (true)
	{
		for (auto req : buffer->GetAndClearRequests())
		{
			TLOG(TLVL_INFO) << "Received Request for Sequence ID " << req.first << ", timestamp " << req.second;
		}
		usleep(10000);
	}

	return rc;
}