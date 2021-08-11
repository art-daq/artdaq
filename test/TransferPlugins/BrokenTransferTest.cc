#include "BrokenTransferTest.hh"

#include "artdaq-core/Data/detail/RawFragmentHeader.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

#include <memory>
#include <thread>
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME "BrokenTransferTest"

artdaqtest::BrokenTransferTest::BrokenTransferTest(const fhicl::ParameterSet& ps)
    : sender_ready_()
    , receiver_ready_()
    , sender_current_fragment_()
    , ps_(ps)
    , test_start_time_(std::chrono::steady_clock::now())
    , test_end_time_(std::chrono::steady_clock::now())
    , test_end_requested_(false)
    , fragment_rate_hz_(ps.get<size_t>("fragment_rate_hz", 10))
    , pause_first_sender_(false)
    , pause_receiver_(false)
    , kill_first_sender_(false)
    , kill_receiver_(false)
    , reliable_mode_(ps.get<bool>("reliable_mode", true))
    , fragment_size_(ps.get<size_t>("fragment_size", 0x10000))
    , send_timeout_us_(ps.get<size_t>("send_timeout_us", 100000))
    , transfer_buffer_count_(ps.get<size_t>("transfer_buffer_count", 10))
    , event_buffer_count_(ps.get<size_t>("event_buffer_count", 20))
    , event_buffer_timeout_us_(ps.get<size_t>("event_buffer_timeout_us", 1000000))
    , send_throttle_us_(0)
{
	if (fragment_rate_hz_ == 0 || fragment_rate_hz_ > 100000)
	{
		TLOG(TLVL_WARNING) << "Invalid rate " << fragment_rate_hz_ << " Hz specified, setting to " << (fragment_rate_hz_ == 0 ? 1 : 1000) << " Hz";
		fragment_rate_hz_ = (fragment_rate_hz_ == 0 ? 1 : 1000);
	}
}

void artdaqtest::BrokenTransferTest::TestSenderPause()
{
	TLOG(TLVL_INFO) << "TestSenderPause BEGIN";
	start_test_();
	usleep_for_n_buffer_epochs_(2);

	TLOG(TLVL_INFO) << "Pausing First Sender";
	pause_first_sender_ = true;
	usleep_for_n_buffer_epochs_(2);
	usleep(2 * event_buffer_timeout_us_);

	TLOG(TLVL_INFO) << "Resuming First Sender";
	pause_first_sender_ = false;
	usleep_for_n_buffer_epochs_(2);

	stop_test_();
	TLOG(TLVL_INFO) << "TestSenderPause END";
}

void artdaqtest::BrokenTransferTest::TestReceiverPause()
{
	TLOG(TLVL_INFO) << "TestReceiverPause BEGIN";
	start_test_();
	usleep_for_n_buffer_epochs_(2);

	TLOG(TLVL_INFO) << "Pausing Recevier";
	pause_receiver_ = true;
	usleep_for_n_buffer_epochs_(2);
	usleep(2 * event_buffer_timeout_us_);

	TLOG(TLVL_INFO) << "Resuming Receiver";
	pause_receiver_ = false;
	usleep_for_n_buffer_epochs_(2);

	stop_test_();
	TLOG(TLVL_INFO) << "TestReceiverPause END";
}

void artdaqtest::BrokenTransferTest::TestSenderReconnect()
{
	TLOG(TLVL_INFO) << "TestSenderReconnect BEGIN";
	start_test_();
	usleep_for_n_buffer_epochs_(2);

	TLOG(TLVL_INFO) << "Killing first Sender";
	kill_first_sender_ = true;
	if (sender_threads_[0].joinable())
	{
		sender_threads_[0].join();
	}
	kill_first_sender_ = false;

	usleep_for_n_buffer_epochs_(2);
	usleep(2 * event_buffer_timeout_us_);

	TLOG(TLVL_INFO) << "Restarting First Sender";
	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 2000 KB
	try
	{
		sender_threads_[0] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_sending_, this, 0));
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Sender thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Sender thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}

	usleep_for_n_buffer_epochs_(2);

	stop_test_();
	TLOG(TLVL_INFO) << "TestSenderReconnect END";
}

void artdaqtest::BrokenTransferTest::TestReceiverReconnect(int send_throttle_factor)
{
	TLOG(TLVL_INFO) << "TestReceiverReconnect BEGIN";
	send_throttle_us_ = send_throttle_factor * 1000000 / fragment_rate_hz_;
	start_test_();
	usleep_for_n_buffer_epochs_(2);

	TLOG(TLVL_INFO) << "Killing Receiver";
	kill_receiver_ = true;
	if (receiver_threads_[0].joinable())
	{
		receiver_threads_[0].join();
	}
	if (receiver_threads_[1].joinable())
	{
		receiver_threads_[1].join();
	}
	kill_receiver_ = false;

	usleep_for_n_buffer_epochs_(2);
	usleep(2 * event_buffer_timeout_us_);

	TLOG(TLVL_INFO) << "Restarting Receiver";
	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 2000 KB
	try
	{
		receiver_threads_[0] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_receiving_, this, 0, 2));
		receiver_threads_[1] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_receiving_, this, 1, 2));
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}

	usleep_for_n_buffer_epochs_(2);

	stop_test_();
	TLOG(TLVL_INFO) << "TestReceiverReconnect END";
}

fhicl::ParameterSet artdaqtest::BrokenTransferTest::make_transfer_ps_(int sender_rank, int receiver_rank, const std::string& name)
{
	auto thePs = ps_.get<fhicl::ParameterSet>("default_transfer_ps", fhicl::ParameterSet());

	thePs.put_or_replace("transferPluginType", ps_.get<std::string>("transfer_to_use", "Shmem"));
	thePs.put_or_replace("destination_rank", receiver_rank);
	thePs.put_or_replace("source_rank", sender_rank);
	thePs.put_or_replace("buffer_count", transfer_buffer_count_);
	if (!thePs.has_key("max_fragment_size_words"))
	{
		thePs.put("max_fragment_size_words", fragment_size_ + artdaq::detail::RawFragmentHeader::num_words() + 1);
	}
	fhicl::ParameterSet outputPs;

	TLOG(TLVL_INFO) << "Configuring transfer between " << sender_rank << " and " << receiver_rank << " with ParameterSet: " << thePs.to_string();

	outputPs.put(name, thePs);
	return outputPs;
}

void artdaqtest::BrokenTransferTest::start_test_()
{
	TLOG(TLVL_DEBUG) << "start_test_ BEGIN";

	sender_ready_[0] = false;
	sender_ready_[1] = false;

	receiver_ready_[0] = false;
	receiver_ready_[1] = false;

	sender_current_fragment_[0] = 0;
	sender_current_fragment_[1] = 0;

	test_start_time_ = std::chrono::steady_clock::now();
	test_end_time_ = std::chrono::steady_clock::now();

	test_end_requested_ = false;
	pause_first_sender_ = false;
	pause_receiver_ = false;
	kill_first_sender_ = false;
	kill_receiver_ = false;

	event_buffer_.clear();
	complete_events_.clear();
	timeout_events_.clear();

	TLOG(TLVL_DEBUG) << "start_test_: Starting receiver threads";
	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 2000 KB
	try
	{
		receiver_threads_[0] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_receiving_, this, 0, 2));
		receiver_threads_[1] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_receiving_, this, 1, 2));
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Receiver thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}

	TLOG(TLVL_DEBUG) << "start_test_: Waiting for receiver_ready_";
	while (!receiver_ready_[0] || !receiver_ready_[1])
	{
		usleep(10000);
	}

	TLOG(TLVL_DEBUG) << "start_test_: Starting sender threads";
	try
	{
		sender_threads_[0] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_sending_, this, 0));
		sender_threads_[1] = boost::thread(attrs, boost::bind(&BrokenTransferTest::do_sending_, this, 1));
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Sender thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Sender thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}

	TLOG(TLVL_DEBUG) << "start_test_: Waiting for sender_ready_";
	while (!sender_ready_[0] || !sender_ready_[1])
	{
		usleep(1000);
	}

	TLOG(TLVL_DEBUG) << "start_test_ DONE";
}

void artdaqtest::BrokenTransferTest::stop_test_()
{
	TLOG(TLVL_DEBUG) << "stop_test_ BEGIN";
	test_end_time_ = std::chrono::steady_clock::now();
	test_end_requested_ = true;

	TLOG(TLVL_DEBUG) << "stop_test_: Waiting for sender threads to shut down";
	while (sender_ready_[0] || sender_ready_[1])
	{
		usleep(1000);
	}

	TLOG(TLVL_DEBUG) << "stop_test_: Joining sender threads";
	if (sender_threads_[0].joinable())
	{
		sender_threads_[0].join();
	}
	if (sender_threads_[1].joinable())
	{
		sender_threads_[1].join();
	}

	TLOG(TLVL_DEBUG) << "stop_test_: Waiting for receiver threads to shut down";
	while (receiver_ready_[0] || receiver_ready_[1])
	{
		usleep(1000);
	}

	TLOG(TLVL_DEBUG) << "stop_test_: Joining receiver threads";
	if (receiver_threads_[0].joinable())
	{
		receiver_threads_[0].join();
	}
	if (receiver_threads_[1].joinable())
	{
		receiver_threads_[1].join();
	}

	TLOG(TLVL_INFO) << "Sent " << sender_current_fragment_[0] << " events from rank 0 and " << sender_current_fragment_[1] << " events from rank 1.";

	artdaq::Fragment::sequence_id_t expected_events = sender_current_fragment_[0];
	if (sender_current_fragment_[1] > expected_events)
	{
		expected_events = sender_current_fragment_[1];
	}

	auto complete_events = complete_events_.size();
	auto incomplete_events = timeout_events_.size();
	auto missing_events = expected_events - complete_events - incomplete_events;

	TLOG(TLVL_INFO) << "Received " << complete_events << " complete events in " << fm_(artdaq::TimeUtils::GetElapsedTime(test_start_time_), "s")
	                << ", Incomplete: " << incomplete_events << ", Missing: " << missing_events;
	TLOG(TLVL_DEBUG) << "stop_test_ END";
}

void artdaqtest::BrokenTransferTest::do_sending_(int sender_rank)
{
	std::unique_ptr<artdaq::TransferInterface> theTransfer = artdaq::MakeTransferPlugin(make_transfer_ps_(sender_rank, 2, "d2"),
	                                                                                    "d2", artdaq::TransferInterface::Role::kSend);

	TLOG(TLVL_DEBUG) << "Sender " << sender_rank << " setting sender_ready_";
	sender_ready_[sender_rank] = true;

	while (sender_current_fragment_[sender_rank] < sequence_id_target_() || !test_end_requested_)
	{
		if (sender_rank == 0 && kill_first_sender_)
		{
			break;
		}
		while (sender_rank == 0 && pause_first_sender_)
		{
			std::this_thread::yield();
			usleep(10000);
		}

		artdaq::Fragment frag(fragment_size_);
		frag.setSequenceID(sender_current_fragment_[sender_rank]);
		frag.setFragmentID(sender_rank);
		frag.setSystemType(artdaq::Fragment::DataFragmentType);

		auto start_time = std::chrono::steady_clock::now();
		auto sts = artdaq::TransferInterface::CopyStatus::kErrorNotRequiringException;

		if (sender_tokens_[sender_rank].load() == 0)
		{
			TLOG(TLVL_INFO) << "Sender " << sender_rank << " waiting for token from receiver";
			while (sender_tokens_[sender_rank].load() == 0 && !test_end_requested_) { usleep(10000); }
			if (test_end_requested_)
			{
				continue;
			}
			TLOG(TLVL_INFO) << "Sender " << sender_rank << " waited " << fm_(artdaq::TimeUtils::GetElapsedTime(start_time), "s") << " for token from receiver";
		}

		if (reliable_mode_)
		{
			sts = theTransfer->transfer_fragment_reliable_mode(std::move(frag));
		}
		else
		{
			sts = theTransfer->transfer_fragment_min_blocking_mode(frag, send_timeout_us_);
		}

		if (sts != artdaq::TransferInterface::CopyStatus::kSuccess)
		{
			TLOG(TLVL_ERROR) << "Error sending Fragment " << sender_current_fragment_[sender_rank] << " from sender rank " << sender_rank << ": "
			                 << artdaq::TransferInterface::CopyStatusToString(sts);
		}
		auto duration = artdaq::TimeUtils::GetElapsedTime(start_time);
		TLOG(TLVL_TRACE) << "Sender " << sender_rank << " Transferred Fragment " << sender_current_fragment_[sender_rank]
		                 << " with size " << fragment_size_ << " words in " << fm_(duration, "s")
		                 << " (approx " << fm_(static_cast<double>(fragment_size_ * sizeof(artdaq::detail::RawFragmentHeader::RawDataType)) / duration, "B/s")
		                 << ") throttle " << send_throttle_us_;
		++sender_current_fragment_[sender_rank];
		sender_tokens_[sender_rank]--;
		if (send_throttle_us_ != 0)
		{
			usleep(send_throttle_us_);
		}
	}

	TLOG(TLVL_DEBUG) << "Sender " << sender_rank << " shutting down...";
	theTransfer.reset(nullptr);
	sender_ready_[sender_rank] = false;
	TLOG(TLVL_DEBUG) << "Sender " << sender_rank << " DONE";
}

void artdaqtest::BrokenTransferTest::do_receiving_(int sender_rank, int receiver_rank)
{
	std::unique_ptr<artdaq::TransferInterface> theTransfer =
	    artdaq::MakeTransferPlugin(make_transfer_ps_(sender_rank, receiver_rank, "s" + std::to_string(sender_rank)),
	                               "s" + std::to_string(sender_rank), artdaq::TransferInterface::Role::kReceive);
	artdaq::FragmentPtr dropFrag = nullptr;

	TLOG(TLVL_DEBUG) << "Receiver " << sender_rank << "->" << receiver_rank << " setting receiver_ready_";
	receiver_ready_[sender_rank] = true;
	sender_tokens_[sender_rank] = event_buffer_count_;

	while (!event_buffer_.empty() || !test_end_requested_ || sender_ready_[0] || sender_ready_[1])
	{
		if (kill_receiver_)
		{
			break;
		}
		while (pause_receiver_)
		{
			std::this_thread::yield();
			usleep(10000);
		}

		artdaq::detail::RawFragmentHeader hdr;
		auto rank = theTransfer->receiveFragmentHeader(hdr, 100000);

		if (rank == artdaq::TransferInterface::RECV_TIMEOUT || event_buffer_.count(hdr.sequence_id) == 0)
		{
			std::unique_lock<std::mutex> lk(event_buffer_mutex_);
			do
			{
				event_buffer_cv_.wait_for(lk, std::chrono::microseconds(10000));

				auto it = event_buffer_.begin();
				while (it != event_buffer_.end())
				{
					if (artdaq::TimeUtils::GetElapsedTimeMicroseconds(it->second.open_time) > event_buffer_timeout_us_)
					{
						TLOG(TLVL_WARNING) << "Receiver " << sender_rank << "->" << receiver_rank << ": Event " << it->first
						                   << " has timed out after " << artdaq::TimeUtils::GetElapsedTime(it->second.open_time) << " s, removing...";
						timeout_events_.insert(it->first);
						it = event_buffer_.erase(it);
						sender_tokens_[0]++;
						sender_tokens_[1]++;
					}
					else
					{
						++it;
					}
				}
			} while (event_buffer_.size() > event_buffer_count_);
		}

		if (rank != sender_rank)
		{
			continue;
		}

		artdaq::RawDataType* ptr = nullptr;
		bool first = true;
		{
			std::unique_lock<std::mutex> lk(event_buffer_mutex_);
			if (timeout_events_.count(hdr.sequence_id) != 0u)
			{
				TLOG(TLVL_WARNING) << "Event " << hdr.sequence_id << " has timed out, discarding";
				if (!dropFrag || dropFrag->size() < hdr.word_count)
				{
					dropFrag = std::make_unique<artdaq::Fragment>(hdr.word_count - hdr.num_words());
				}
				ptr = dropFrag->headerAddress() + hdr.num_words();  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
			}
			else
			{
				if (event_buffer_.count(hdr.sequence_id) == 0u)
				{
					event_buffer_[hdr.sequence_id].open_time = std::chrono::steady_clock::now();
					event_buffer_[hdr.sequence_id].first_frag.reset(new artdaq::Fragment(hdr.word_count - hdr.num_words()));
					ptr = event_buffer_[hdr.sequence_id].first_frag->headerAddress() + hdr.num_words();  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
					TLOG(TLVL_TRACE) << "Receiver " << sender_rank << "->" << receiver_rank << " opened event " << hdr.sequence_id
					                 << " with Fragment from rank " << sender_rank;
				}
				else
				{
					event_buffer_[hdr.sequence_id].second_frag.reset(new artdaq::Fragment(hdr.word_count - hdr.num_words()));
					ptr = event_buffer_[hdr.sequence_id].second_frag->headerAddress() + hdr.num_words();  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
					first = false;
				}
			}
		}

		rank = theTransfer->receiveFragmentData(ptr, hdr.word_count - hdr.num_words());
		if (rank != sender_rank)
		{
			TLOG(TLVL_ERROR) << "Error receiving Fragment data after header received successfully!";
			exit(1);
		}

		if (!first)
		{
			TLOG(TLVL_TRACE) << "Receiver " << sender_rank << "->" << receiver_rank << " completed event " << hdr.sequence_id
			                 << " in " << fm_(artdaq::TimeUtils::GetElapsedTime(event_buffer_[hdr.sequence_id].open_time), "s") << ".";

			std::unique_lock<std::mutex> lk(event_buffer_mutex_);
			complete_events_.insert(hdr.sequence_id);
			event_buffer_.erase(hdr.sequence_id);
			event_buffer_cv_.notify_one();
			sender_tokens_[0]++;
			sender_tokens_[1]++;
		}
	}

	TLOG(TLVL_DEBUG) << "Receiver " << sender_rank << "->" << receiver_rank << " shutting down...";
	theTransfer->flush_buffers();

	std::lock_guard<std::mutex> lk(event_buffer_mutex_);
	theTransfer.reset(nullptr);
	receiver_ready_[sender_rank] = false;
	TLOG(TLVL_DEBUG) << "Receiver " << sender_rank << "->" << receiver_rank << " DONE";
}

artdaq::Fragment::sequence_id_t artdaqtest::BrokenTransferTest::sequence_id_target_()
{
	auto ret = 1 + (artdaq::TimeUtils::GetElapsedTimeMicroseconds(test_start_time_) * fragment_rate_hz_ / 1000000);
	if (test_end_requested_)
	{
		ret = 1 + (artdaq::TimeUtils::GetElapsedTimeMicroseconds(test_start_time_, test_end_time_) * fragment_rate_hz_ / 1000000);
	}
	//TLOG(TLVL_DEBUG) << "sequence_id_target_ is " << ret;
	return ret;
}

std::string artdaqtest::BrokenTransferTest::fm_(double data, const std::string& units, int logt)
{
	if (data < 1 && logt > -3)
	{
		return fm_(data * 1000, units, logt - 1);
	}
	if (data > 1000 && logt < 3)
	{
		return fm_(data / 1000, units, logt + 1);
	}

	std::stringstream o;
	o << std::fixed << std::setprecision(2) << data << " ";
	switch (logt)
	{
		case -3:
			o << "n";
			break;
		case -2:
			o << "u";
			break;
		case -1:
			o << "m";
			break;
		case 0:
		default:
			break;
		case 1:
			o << "K";
			break;
		case 2:
			o << "M";
			break;
		case 3:
			o << "G";
			break;
	}
	o << units;
	return o.str();
}
