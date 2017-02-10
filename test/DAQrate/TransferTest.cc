#include "test/DAQrate/TransferTest.hh"

#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"

#include "artdaq/DAQdata/Globals.hh"
#include "trace.h"

#include <fhiclcpp/make_ParameterSet.h>

size_t artdaq::TransferTest::do_sending()
{
	TRACE(7, "do_sending entered RawFragmentHeader::num_words()=%lu"
		, artdaq::detail::RawFragmentHeader::num_words());

	size_t totalSize = 0;
	artdaq::DataSenderManager sender(ps_);

	std::vector<artdaq::Fragment> frags(buffer_count_, artdaq::Fragment());
	unsigned data_size_wrds = max_payload_size_ / sizeof(artdaq::RawDataType) - artdaq::detail::RawFragmentHeader::num_words();
	if (data_size_wrds < 8) data_size_wrds = 8;  // min size

	for (int ii = 0; ii < sends_each_sender_; ++ii)
	{
		TRACE(6, "sender rank %d #%u resize datsz=%u", my_rank, ii, data_size_wrds);
		frags[ii%buffer_count_].resize(data_size_wrds);
		TRACE(7, "sender rank %d #%u resized bytes=%ld", my_rank, ii, frags[ii%buffer_count_].sizeBytes());
		totalSize += frags[ii%buffer_count_].sizeBytes();

		unsigned sndDatSz = data_size_wrds;
		frags[ii%buffer_count_].setSequenceID(ii);
		frags[ii%buffer_count_].setFragmentID(my_rank);
		frags[ii%buffer_count_].setSystemType(artdaq::Fragment::DataFragmentType);

		artdaq::Fragment::iterator it = frags[ii%buffer_count_].dataBegin();
		*it = my_rank;
		*++it = ii;
		*++it = sndDatSz;

		sender.sendFragment(std::move(frags[ii%buffer_count_]));
		TRACE( 1,"Sender %d sent fragment %d", my_rank, ii );
		//usleep( (data_size_wrds*sizeof(artdaq::RawDataType))/233 );

		frags[ii%buffer_count_] = artdaq::Fragment(); // replace/renew
		TRACE(9, "sender rank %d frag replaced", my_rank);
	}
	
	return totalSize;
} // do_sending

size_t artdaq::TransferTest::do_receiving()
{
	TRACE(7, "do_receiving entered");
	artdaq::DataReceiverManager receiver(ps_);
	receiver.start_threads();
	int counter = receives_each_receiver_;
	size_t totalSize = 0;
	bool first = true;
	int activeSenders = senders_;
	while (activeSenders > 0) {
		TRACE(7, "TransferTest::do_receiving: Counter is %d, calling recvFragment", counter);
		int senderSlot = artdaq::TransferInterface::RECV_TIMEOUT;
		auto ignoreFragPtr = receiver.recvFragment(senderSlot);
		if (senderSlot != artdaq::TransferInterface::RECV_TIMEOUT && ignoreFragPtr) {
			if (ignoreFragPtr->type() == artdaq::Fragment::EndOfDataFragmentType) {
				std::cout << "Receiver " << my_rank << " received EndOfData Fragment from Sender " << senderSlot << std::endl;
				activeSenders--;
			}
			else {
				if (first) {
					start_time_ = std::chrono::steady_clock::now();
					first = false;
				}
				counter--;
				TRACE( 1, "Receiver %d received fragment %d with seqID %lu from Sender %d (Expecting %d more)"
				      , my_rank, receives_each_receiver_-counter, ignoreFragPtr->sequenceID(), senderSlot, counter );
				totalSize += ignoreFragPtr->size() * sizeof(artdaq::RawDataType);
			}
		}
		TRACE(7, "TransferTest::do_receiving: Recv Loop end, counter is %d", counter);
	}
	
	return totalSize;
}

artdaq::TransferTest::TransferTest(fhicl::ParameterSet psi)
	: senders_(psi.get<int>("num_senders"))
	, receivers_(psi.get<int>("num_receivers"))
	, sends_each_sender_(psi.get<int>("sends_per_sender"))
	, receives_each_receiver_(senders_ * sends_each_sender_ / receivers_)
	, buffer_count_(psi.get<int>("buffer_count", 10))
	, max_payload_size_(psi.get<size_t>("fragment_size", 0x100000))
	, ps_()
{
	TRACE(10, "TransferTest CONSTRUCTOR");
	std::string type(psi.get<std::string>("transfer_plugin_type", "Shmem"));

	if (receivers_ > 0) {
		if (senders_ * sends_each_sender_ % receivers_ != 0) {
			std::cout << "Adding sends so that sends_each_sender * num_sending_ranks is a multiple of num_receiving_ranks" << std::endl;
			while (senders_ * sends_each_sender_ % receivers_ != 0) {
				sends_each_sender_++;
			}
			receives_each_receiver_ = senders_ * sends_each_sender_ / receivers_;
			std::cout << "sends_each_sender is now " << sends_each_sender_ << std::endl;
			psi.put_or_replace("sends_per_sender", sends_each_sender_);
		}
	}

	std::string hostmap = "";
	if(psi.has_key("hostmap")) {
	  hostmap = " host_map: @local::hostmap";
	}

	std::stringstream ss;
	ss << psi.to_string();
	ss << " sources: {";
	for (int ii = 0; ii < senders_; ++ii) {
	  ss << "s" << ii << ": { transferPluginType: " << type << " source_rank: " << ii << " max_fragment_size_words: " << max_payload_size_ << " buffer_count: " << buffer_count_ << hostmap << "}";
	}
	ss << "} destinations: {";
	for (int jj = senders_; jj < senders_ + receivers_; ++jj) {
	  ss << "d" << jj << ": { transferPluginType: " << type << " destination_rank: " << jj << " max_fragment_size_words: " << max_payload_size_ << " buffer_count: " << buffer_count_ << hostmap << "}";
	}
	ss << "}";

	make_ParameterSet(ss.str(), ps_);


	std::cout << "Going to configure with ParameterSet: " << ps_.to_string() << std::endl;
}

int artdaq::TransferTest::runTest() {
	TRACE(11, "TransferTest::runTest BEGIN");
	start_time_ = std::chrono::steady_clock::now();
	size_t size = 0;
	if (my_rank < senders_) {
		size = do_sending();
	}
	else {
		size = do_receiving();
	}
	auto duration = std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1>>>(std::chrono::steady_clock::now() - start_time_).count();
	std::cout << (my_rank < senders_ ? "Sent " : "Received ") << size << " bytes in " << duration << " seconds ( " << formatBytes(size / duration) << "/s )." << std::endl;

	TRACE(11, "TransferTest::runTest DONE");
	return 0;
}
