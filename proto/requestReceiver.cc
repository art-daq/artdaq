#define TRACE_NAME "requestReceiverApp"
#include "artdaq/DAQdata/Globals.hh"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "artdaq/DAQrate/RequestReceiver.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("requestReceiverApp");
	metricMan->initialize(fhicl::ParameterSet());
	metricMan->do_start();

	struct Config
	{
		fhicl::TableFragment<artdaq::RequestReceiver::Config> receiverConfig;
		fhicl::Atom<int> request_receiver_app_rank{fhicl::Name{"request_receiver_app_rank"}, fhicl::Comment{"Rank of this requestReceiver app"}, -1};
		fhicl::Atom<int> request_count{fhicl::Name{"request_count"}, fhicl::Comment{"Number of requests to receive before exiting. -1 to go forever."}, -1};
	};
	auto pset = LoadParameterSet<Config>(argc, argv, "request_receiver", "This is a simple application which listens for Data Request messages and prints their contents");

	int rc = 0;

	fhicl::ParameterSet tempPset;
	if (pset.has_key("request_receiver"))
	{
		tempPset = pset.get<fhicl::ParameterSet>("request_receiver");
	}
	else
	{
		tempPset = pset;
	}
	my_rank = tempPset.get<int>("request_receiver_app_rank", -1);
	auto count = tempPset.get<int>("request_count", -1);

	artdaq::RequestReceiver recvr(tempPset);
	recvr.startRequestReception();

	while (count != 0)
	{
		if (my_rank == -1)
		{
			for (auto req : recvr.GetAndClearRequests())
			{
				TLOG(TLVL_INFO) << "Received Request for Sequence ID " << req.first << ": " << req.second;
				count--;
			}
		}
		else
		{
			for (auto req : recvr.GetRequests())
			{
				TLOG(TLVL_INFO) << "Received Request for Sequence ID " << req.first << ": " << req.second;
				recvr.RemoveRequest(req.first);
				count--;
			}
		}
		usleep(10000);
	}

	artdaq::Globals::CleanUpGlobals();
	return rc;
}
