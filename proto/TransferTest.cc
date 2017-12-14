#include "proto/TransferTest.hh"

#include "artdaq-core/Data/Fragment.hh"
#include "proto/FragmentReceiverManager.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"

#include "artdaq/DAQdata/Globals.hh"

#include "fhiclcpp/make_ParameterSet.h"

artdaq::TransferTest::TransferTest(fhicl::ParameterSet psi)
	: senders_(psi.get<int>("num_senders"))
	, receivers_(psi.get<int>("num_receivers"))
	, sends_each_sender_(psi.get<int>("sends_per_sender"))
	, receives_each_receiver_(senders_ * sends_each_sender_ / receivers_)
	, buffer_count_(psi.get<int>("buffer_count", 10))
	, max_payload_size_(psi.get<size_t>("fragment_size", 0x100000))
	, ps_()
	, validate_mode_(psi.get<bool>("validate_data_mode", false))
{
	TLOG_ARB(10, "TransferTest") << "CONSTRUCTOR" << TLOG_ENDL;
	metricMan = &metricMan_;

	fhicl::ParameterSet metric_pset;

	try
	{
		metric_pset = psi.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL                                    

	try
	{
		std::string name = "TransferTest" + std::to_string(my_rank);
		metricMan_.initialize(metric_pset, name);
		metricMan_.do_start();
	}
	catch (...) {}

	std::string type(psi.get<std::string>("transfer_plugin_type", "Shmem"));

	if (receivers_ > 0)
	{
		if (senders_ * sends_each_sender_ % receivers_ != 0)
		{
			std::cout << "Adding sends so that sends_each_sender * num_sending_ranks is a multiple of num_receiving_ranks" << std::endl;
			while (senders_ * sends_each_sender_ % receivers_ != 0)
			{
				sends_each_sender_++;
			}
			receives_each_receiver_ = senders_ * sends_each_sender_ / receivers_;
			std::cout << "sends_each_sender is now " << sends_each_sender_ << std::endl;
			psi.put_or_replace("sends_per_sender", sends_each_sender_);
		}
	}

	std::string hostmap = "";
	if (psi.has_key("hostmap"))
	{
		hostmap = " host_map: @local::hostmap";
	}

	std::stringstream ss;
	ss << psi.to_string();
	ss << " sources: {";
	for (int ii = 0; ii < senders_; ++ii)
	{
		ss << "s" << ii << ": { transferPluginType: " << type << " source_rank: " << ii << " max_fragment_size_words: " << max_payload_size_ << " buffer_count: " << buffer_count_ << hostmap << "}";
	}
	ss << "} destinations: {";
	for (int jj = senders_; jj < senders_ + receivers_; ++jj)
	{
		ss << "d" << jj << ": { transferPluginType: " << type << " destination_rank: " << jj << " max_fragment_size_words: " << max_payload_size_ << " buffer_count: " << buffer_count_ << hostmap << "}";
	}
	ss << "}";

	make_ParameterSet(ss.str(), ps_);


	std::cout << "Going to configure with ParameterSet: " << ps_.to_string() << std::endl;
}

int artdaq::TransferTest::runTest()
{
	TLOG_ARB(11, "TransferTest") << "runTest BEGIN" << TLOG_ENDL;
	start_time_ = std::chrono::steady_clock::now();
	std::pair<size_t, double> result;
	if (my_rank >= senders_ + receivers_) return 0;
	if (my_rank < senders_)
	{
		result = do_sending();
	}
	else
	{
		result = do_receiving();
	}
	auto duration = std::chrono::duration_cast<artdaq::TimeUtils::seconds>(std::chrono::steady_clock::now() - start_time_).count();
	std::cout << (my_rank < senders_ ? "Sent " : "Received ") << result.first << " bytes in " << duration << " seconds ( " << formatBytes(result.first / duration) << "/s )." << std::endl;
	std::cout << "Rate of " << (my_rank < senders_ ? "sending" : "receiving") << ": " << formatBytes(result.first / result.second) << "/s." << std::endl;
	metricMan_.do_stop();
	metricMan_.shutdown();
	TLOG_ARB(11, "TransferTest") << "runTest DONE" << TLOG_ENDL;
	return 0;
}

std::pair<size_t, double> artdaq::TransferTest::do_sending()
{
	TLOG_ARB(7, "TransferTest") << "do_sending entered RawFragmentHeader::num_words()=" << std::to_string(artdaq::detail::RawFragmentHeader::num_words()) << TLOG_ENDL;

	size_t totalSize = 0;
	double totalTime = 0;
	artdaq::DataSenderManager sender(ps_);

	unsigned data_size_wrds = max_payload_size_ / sizeof(artdaq::RawDataType) - artdaq::detail::RawFragmentHeader::num_words();
	if (data_size_wrds < 8) data_size_wrds = 8; // min size
	artdaq::Fragment frag(data_size_wrds);

	if (validate_mode_)
	{
		artdaq::RawDataType gen_seed = 0;

		std::generate_n(frag.dataBegin(), data_size_wrds, [&]() {	return ++gen_seed; });
		for (size_t ii = 0; ii < frag.dataSize(); ++ii)
		{
			if (*(frag.dataBegin() + ii) != ii + 1)
			{
				TLOG_ERROR("TransferTest") << "Data corruption detected! (" << std::to_string(*(frag.dataBegin() + ii)) << " != " << std::to_string(ii + 1) << ") Aborting!" << TLOG_ENDL;
				exit(1);
			}
		}
	}

	int metric_send_interval = sends_each_sender_ / 1000 > 1 ? sends_each_sender_ / 1000 : 1;
	auto init_time_metric = 0.0;
	auto send_time_metric = 0.0;
	auto after_time_metric = 0.0;
	auto send_size_metric = 0.0;

	for (int ii = 0; ii < sends_each_sender_; ++ii)
	{
		auto loop_start = std::chrono::steady_clock::now();
		TLOG_ARB(7, "TransferTest") << "sender rank " << my_rank << " #" << ii << " resized bytes=" << std::to_string(frag.sizeBytes()) << TLOG_ENDL;
		totalSize += frag.sizeBytes();

		//unsigned sndDatSz = data_size_wrds;
		frag.setSequenceID(ii);
		frag.setFragmentID(my_rank);
		frag.setSystemType(artdaq::Fragment::DataFragmentType);
		/*
				artdaq::Fragment::iterator it = frag.dataBegin();
				*it = my_rank;
				*++it = ii;
				*++it = sndDatSz;*/

		auto send_start = std::chrono::steady_clock::now();
		sender.sendFragment(std::move(frag));
		auto after_send = std::chrono::steady_clock::now();
		TLOG_TRACE("TransferTest") << "Sender " << my_rank << " sent fragment " << ii << TLOG_ENDL;
		//usleep( (data_size_wrds*sizeof(artdaq::RawDataType))/233 );

		frag = artdaq::Fragment(data_size_wrds); // replace/renew
		if (validate_mode_)
		{
			artdaq::RawDataType gen_seed = ii + 1;

			std::generate_n(frag.dataBegin(), data_size_wrds, [&]() {	return ++gen_seed; });
			for (size_t jj = 0; jj < frag.dataSize(); ++jj)
			{
				if (*(frag.dataBegin() + jj) != (ii + 1) + jj + 1)
				{
					TLOG_ERROR("TransferTest") << "Input Data corruption detected! (" << std::to_string(*(frag.dataBegin() + jj)) << " != " << std::to_string(ii + jj + 2) << " at position " << ii << ") Aborting!" << TLOG_ENDL;
					exit(1);
				}
			}
		}
		TLOG_ARB(9, "TransferTest") << "sender rank " << my_rank << " frag replaced" << TLOG_ENDL;

		auto total_send_time = std::chrono::duration_cast<artdaq::TimeUtils::seconds>(after_send - send_start).count();
		totalTime += total_send_time;
		send_time_metric += total_send_time;
		send_size_metric += data_size_wrds * sizeof(artdaq::RawDataType);
		after_time_metric += std::chrono::duration_cast<artdaq::TimeUtils::seconds>(std::chrono::steady_clock::now() - after_send).count();
		init_time_metric += std::chrono::duration_cast<artdaq::TimeUtils::seconds>(send_start - loop_start).count();

		if (metricMan && ii % metric_send_interval == 0)
		{
			metricMan->sendMetric("send_init_time", init_time_metric, "seconds", 3, MetricMode::Accumulate);
			metricMan->sendMetric("total_send_time", send_time_metric, "seconds", 3, MetricMode::Accumulate);
			metricMan->sendMetric("after_send_time", after_time_metric, "seconds", 3, MetricMode::Accumulate);
			metricMan->sendMetric("send_rate", send_size_metric / send_time_metric, "B/s", 3, MetricMode::Average);
			init_time_metric = 0.0;
			send_time_metric = 0.0;
			after_time_metric = 0.0;
			send_size_metric = 0.0;
		}
	}

	return std::make_pair(totalSize, totalTime);
} // do_sending

std::pair<size_t, double> artdaq::TransferTest::do_receiving()
{
	TLOG_ARB(7, "TransferTest") << "do_receiving entered" << TLOG_ENDL;

	artdaq::FragmentReceiverManager receiver(ps_);
	receiver.start_threads();
	int counter = receives_each_receiver_;
	size_t totalSize = 0;
	double totalTime = 0;
	bool first = true;
	int activeSenders = senders_;
	auto end_loop = std::chrono::steady_clock::now();

	auto recv_size_metric = 0.0;
	auto recv_time_metric = 0.0;
	auto input_wait_metric = 0.0;
	auto init_wait_metric = 0.0;
	int metric_send_interval = receives_each_receiver_ / 1000 > 1 ? receives_each_receiver_ : 1;

	while (activeSenders > 0)
	{
		auto start_loop = std::chrono::steady_clock::now();
		TLOG_ARB(7, "TransferTest") << "do_receiving: Counter is " << counter << ", calling recvFragment" << TLOG_ENDL;
		int senderSlot = artdaq::TransferInterface::RECV_TIMEOUT;
		auto before_receive = std::chrono::steady_clock::now();
		init_wait_metric += std::chrono::duration_cast<artdaq::TimeUtils::seconds>(before_receive - start_loop).count();

		auto ignoreFragPtr = receiver.recvFragment(senderSlot);
		auto after_receive = std::chrono::steady_clock::now();
		size_t thisSize = 0;
		if (senderSlot != artdaq::TransferInterface::RECV_TIMEOUT && ignoreFragPtr)
		{
			if (ignoreFragPtr->type() == artdaq::Fragment::EndOfDataFragmentType)
			{
				std::cout << "Receiver " << my_rank << " received EndOfData Fragment from Sender " << senderSlot << std::endl;
				activeSenders--;
			}
			else
			{
				if (first)
				{
					start_time_ = std::chrono::steady_clock::now();
					first = false;
				}
				counter--;
				TLOG_INFO("TransferTest") << "Receiver " << my_rank << " received fragment " << receives_each_receiver_ - counter
					<< " with seqID " << std::to_string(ignoreFragPtr->sequenceID()) << " from Sender " << senderSlot << " (Expecting " << counter << " more)" << TLOG_ENDL;
				thisSize = ignoreFragPtr->size() * sizeof(artdaq::RawDataType);
				totalSize += thisSize;
				if (validate_mode_)
				{
					for (size_t ii = 0; ii < ignoreFragPtr->dataSize(); ++ii)
					{
						if (*(ignoreFragPtr->dataBegin() + ii) != ignoreFragPtr->sequenceID() + ii + 1)
						{
							TLOG_ERROR("TransferTest") << "Output Data corruption detected! (" << std::to_string(*(ignoreFragPtr->dataBegin() + ii)) << " != " << std::to_string(ignoreFragPtr->sequenceID() + ii + 1) << " at position " << ii << ") Aborting!" << TLOG_ENDL;
							exit(1);
						}
					}
				}
			}
			input_wait_metric += std::chrono::duration_cast<artdaq::TimeUtils::seconds>(after_receive - end_loop).count();
		}
		TLOG_ARB(7, "TransferTest") << "do_receiving: Recv Loop end, counter is " << counter << TLOG_ENDL;
		auto total_recv_time = std::chrono::duration_cast<artdaq::TimeUtils::seconds>(after_receive - before_receive).count();
		recv_time_metric += total_recv_time;
		totalTime += total_recv_time;
		recv_size_metric += thisSize;

		if (metricMan && counter % metric_send_interval == 0)
		{
			metricMan->sendMetric("input_wait", input_wait_metric, "seconds", 3, MetricMode::Accumulate);
			metricMan->sendMetric("recv_init_time", init_wait_metric, "seconds", 3, MetricMode::Accumulate);
			metricMan->sendMetric("total_recv_time", recv_time_metric, "seconds", 3, MetricMode::Accumulate);
			metricMan->sendMetric("recv_rate", recv_size_metric / recv_time_metric, "B/s", 3, MetricMode::Average);

			input_wait_metric = 0.0;
			init_wait_metric = 0.0;
			recv_time_metric = 0.0;
			recv_size_metric = 0.0;
		}
		end_loop = std::chrono::steady_clock::now();
	}

	return std::make_pair(totalSize, totalTime);
}
