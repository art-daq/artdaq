#define TRACE_NAME "RequestSenderApp"
#include "artdaq/DAQdata/Globals.hh"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQrate/RequestReceiver.hh"
#include "artdaq/DAQrate/RequestSender.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("RequestSenderApp");
	metricMan->initialize(fhicl::ParameterSet());
	metricMan->do_start();

	struct Config
	{
		fhicl::TableFragment<artdaq::RequestSender::Config> senderConfig;
		fhicl::Atom<bool> use_receiver{fhicl::Name{"use_receiver"}, fhicl::Comment{"Whether to setup a RequestReceiver to verify that requests are being sent"}, false};
		fhicl::Atom<size_t> receiver_timeout_ms{fhicl::Name{"recevier_timeout_ms"}, fhicl::Comment{"Amount of time to wait for the receiver to receive a request message"}, 1000};
		fhicl::Atom<size_t> request_sending_delay{fhicl::Name{"request_sending_delay"}, fhicl::Comment{"Amount of time to wait between each request message"}, 10000};
		fhicl::Table<artdaq::RequestReceiver::Config> receiver_config{fhicl::Name{"receiver_config"}, fhicl::Comment{"Configuration for RequestReceiver, if used"}};
		fhicl::Atom<int> num_requests{fhicl::Name{"num_requests"}, fhicl::Comment{"Number of requests to send"}};
		fhicl::Atom<artdaq::Fragment::sequence_id_t> starting_sequence_id{fhicl::Name{"starting_sequence_id"}, fhicl::Comment{"Sequence ID of first request"}, 1};
		fhicl::Atom<artdaq::Fragment::sequence_id_t> sequence_id_scale{fhicl::Name{"sequence_id_scale"}, fhicl::Comment{"Amount to increment Sequence ID for each request"}, 1};
		fhicl::Atom<artdaq::Fragment::timestamp_t> starting_timestamp{fhicl::Name{"starting_timestamp"}, fhicl::Comment{"Timestamp of first request"}, 1};
		fhicl::Atom<artdaq::Fragment::timestamp_t> timestamp_scale{fhicl::Name{"timestamp_scale"}, fhicl::Comment{"Amount to increment timestamp for each request"}, 1};
		fhicl::Atom<int> request_sender_app_rank{fhicl::Name{"request_sender_app_rank"}, fhicl::Comment{"Rank of the requestSender app"}, -1};
	};

	auto tempPset = LoadParameterSet<Config>(argc, argv, "sender", "This test application sends Data Request messages and optionally receives them to detect issues in the network transport");
	fhicl::ParameterSet pset;
	if (tempPset.has_key("sender"))
	{
		pset = tempPset.get<fhicl::ParameterSet>("sender");
	}
	else
	{
		pset = tempPset;
	}

	my_rank = pset.get<int>("request_sender_app_rank", -1);

	if (!pset.has_key("request_timeout_s"))
	{
		pset.put<int>("request_timeout_s", 3);
	}

	int rc = 0;

	artdaq::RequestSender sender(pset);

	std::unique_ptr<artdaq::RequestReceiver> receiver(nullptr);
	int num_requests = pset.get<int>("num_requests", 1);
	if (pset.get<bool>("use_receiver", false))
	{
		receiver.reset(new artdaq::RequestReceiver(pset.get<fhicl::ParameterSet>("receiver_config")));
		receiver->startRequestReception();
	}

	auto seq = pset.get<artdaq::Fragment::sequence_id_t>("starting_sequence_id", 1);
	auto seq_scale = pset.get<artdaq::Fragment::sequence_id_t>("sequence_id_scale", 1);
	auto ts = pset.get<artdaq::Fragment::timestamp_t>("starting_timestamp", 1);
	auto ts_scale = pset.get<artdaq::Fragment::timestamp_t>("timestamp_scale", 1);
	auto tmo = pset.get<size_t>("recevier_timeout_ms", 1000);
	auto delay = pset.get<size_t>("request_sending_delay", 10000);

	for (auto ii = 0; ii < num_requests; ++ii)
	{
		sender.AddRequest(seq, ts);
		sender.SendRequest();

		if (receiver)
		{
			auto start_time = std::chrono::steady_clock::now();
			bool recvd = false;
			while (!recvd && artdaq::TimeUtils::GetElapsedTimeMilliseconds(start_time) < tmo)
			{
				auto reqs = receiver->GetRequests();
				if (reqs.count(seq))
				{
					TLOG(TLVL_INFO) << "Received Request for Sequence ID " << seq << ": " << reqs[seq];
					receiver->RemoveRequest(seq);
					if (!pset.get<bool>("request_acknowledgements", false))
					{
						sender.RemoveRequest(seq);
					}
					recvd = true;
				}
				else
				{
					usleep(10000);
				}
			}
		}

		seq += seq_scale;
		ts += ts_scale;
		usleep(delay);
	}

	TLOG(TLVL_INFO) << "Waiting for all requests to be acknowledged. Sent " << sender.GetRequestCount() << " requests and received " << sender.GetAckCount() << " acks.";

	auto start_wait = std::chrono::steady_clock::now();
	while (sender.GetActiveRequests().size())
	{
		if (artdaq::TimeUtils::GetElapsedTime(start_wait) > 5.0)
		{
			TLOG(TLVL_WARNING) << "Outstanding requests were not acknowledged after 5s. Shutting down.";
			auto reqs = sender.GetActiveRequests();
			for (auto& req : reqs)
			{
				TLOG(21) << "Request " << req.first << ": " << req.second << ".";
			}
			break;
		}
		sender.ClearCompletedRequests();
		usleep(100000);
	}

	TLOG(TLVL_INFO) << "requestSender shutting down. Sent " << sender.GetRequestCount() << " requests and received " << sender.GetAckCount() << " acks.";
	artdaq::Globals::CleanUpGlobals();
	return rc;
}
