#define TRACE_NAME "RequestSender"

#include <boost/program_options.hpp>
#include <memory>
#include "fhiclcpp/make_ParameterSet.h"

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQrate/RequestReceiver.hh"
#include "artdaq/DAQrate/RequestSender.hh"

int main(int argc, char* argv[])
try
{
	artdaq::configureMessageFacility("RequestSender");

	struct Config
	{
		fhicl::TableFragment<artdaq::RequestSender::Config> senderConfig;
		fhicl::Atom<bool> use_receiver{fhicl::Name{"use_receiver"}, fhicl::Comment{"Whether to setup a RequestReceiver to verify that requests are being sent"}, false};
		fhicl::Atom<size_t> receiver_timeout_ms{fhicl::Name{"recevier_timeout_ms"}, fhicl::Comment{"Amount of time to wait for the receiver to receive a request message"}, 1000};
		fhicl::Table<artdaq::RequestReceiver::Config> receiver_config{fhicl::Name{"receiver_config"}, fhicl::Comment{"Configuration for RequestReceiver, if used"}};
		fhicl::Atom<int> num_requests{fhicl::Name{"num_requests"}, fhicl::Comment{"Number of requests to send"}};
		fhicl::Atom<artdaq::Fragment::sequence_id_t> starting_sequence_id{fhicl::Name{"starting_sequence_id"}, fhicl::Comment{"Sequence ID of first request"}, 1};
		fhicl::Atom<artdaq::Fragment::sequence_id_t> sequence_id_scale{fhicl::Name{"sequence_id_scale"}, fhicl::Comment{"Amount to increment Sequence ID for each request"}, 1};
		fhicl::Atom<artdaq::Fragment::timestamp_t> starting_timestamp{fhicl::Name{"starting_timestamp"}, fhicl::Comment{"Timestamp of first request"}, 1};
		fhicl::Atom<artdaq::Fragment::timestamp_t> timestamp_scale{fhicl::Name{"timestamp_scale"}, fhicl::Comment{"Amount to increment timestamp for each request"}, 1};
	};

	auto pset = LoadParameterSet<Config>(argc, argv, "sender", "This test application sends Data Request messages and optionally receives them to detect issues in the network transport");

	int rc = 0;

	artdaq::RequestSender sender(pset);

	std::unique_ptr<artdaq::RequestReceiver> receiver(nullptr);
	std::shared_ptr<artdaq::RequestBuffer> request_buffer(nullptr);
	int num_requests = pset.get<int>("num_requests", 1);
	if (pset.get<bool>("use_receiver", false))
	{
		auto receiver_pset = pset.get<fhicl::ParameterSet>("receiver_config");
		request_buffer = std::make_shared<artdaq::RequestBuffer>(receiver_pset.get<artdaq::Fragment::sequence_id_t>("request_increment", 1));
		receiver = std::make_unique<artdaq::RequestReceiver>(receiver_pset, request_buffer);
		receiver->startRequestReception();
	}

	auto seq = pset.get<artdaq::Fragment::sequence_id_t>("starting_sequence_id", 1);
	auto seq_scale = pset.get<artdaq::Fragment::sequence_id_t>("sequence_id_scale", 1);
	auto ts = pset.get<artdaq::Fragment::timestamp_t>("starting_timestamp", 1);
	auto ts_scale = pset.get<artdaq::Fragment::timestamp_t>("timestamp_scale", 1);
	auto tmo = pset.get<size_t>("recevier_timeout_ms", 1000);

	for (auto ii = 0; ii < num_requests; ++ii)
	{
		TLOG(TLVL_INFO) << "Sending request " << ii << " of " << num_requests << " with sequence id " << seq;
		sender.AddRequest(seq, ts);
		sender.SendRequest();

		if (request_buffer)
		{
			auto start_time = std::chrono::steady_clock::now();
			bool recvd = false;
			TLOG(TLVL_INFO) << "Starting receive loop for request " << ii;
			while (!recvd && artdaq::TimeUtils::GetElapsedTimeMilliseconds(start_time) < tmo)
			{
				auto reqs = request_buffer->GetRequests();
				if (reqs.count(seq) != 0u)
				{
					TLOG(TLVL_INFO) << "Received Request for Sequence ID " << seq << ", timestamp " << reqs[seq];
					request_buffer->RemoveRequest(seq);
					sender.RemoveRequest(seq);
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
	}

	return rc;
}
catch (...)
{
	return -1;
}
