#ifndef artdaq_test_TransferPlugins_BrokenTransferTest_hh
#define artdaq_test_TransferPlugins_BrokenTransferTest_hh

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

#include <fhiclcpp/ParameterSet.h>
#include <fhiclcpp/types/Atom.h>
#include <fhiclcpp/types/Table.h>
#include <boost/thread.hpp>
#include <condition_variable>
#include <mutex>
#include <unordered_set>

namespace artdaqtest {
class BrokenTransferTest
{
public:
	struct Config
	{
		fhicl::Atom<size_t> fragment_rate_hz{fhicl::Name{"fragment_rate_hz"}, fhicl::Comment{"The rate at which to generate Fragments, in Hz"}, 10};
		fhicl::Atom<bool> reliable_mode{fhicl::Name{"reliable_mode"}, fhicl::Comment{"Whether to use reliable-mode transfers (true) or min-blocking (false)"}, true};
		fhicl::Atom<size_t> fragment_size{fhicl::Name{"fragment_size"}, fhicl::Comment{"The size of generated Fragments, in Fragment words"}, 0x10000};
		fhicl::Atom<size_t> send_timeout_us{fhicl::Name{"send_timeout_us"}, fhicl::Comment{"The timeout for min-blocking mode sends"}, 100000};
		fhicl::Atom<size_t> transfer_buffer_count{fhicl::Name{"transfer_buffer_count"}, fhicl::Comment{"The number of buffers in the Transfer Plugins"}, 10};
		fhicl::Atom<size_t> event_buffer_count{fhicl::Name{"event_buffer_count"}, fhicl::Comment{"The number of \"EventBuilder\" buffers on the receiver end"}, 20};
		fhicl::Atom<size_t> event_buffer_timeout_us{fhicl::Name{"event_buffer_timeout_us"}, fhicl::Comment{"The timeout for \"EventBuilder\" buffers to be marked incomplete and abandoned"}, 1000000};
		fhicl::Table<artdaq::TransferInterface::Config> default_transfer_ps{fhicl::Name{"default_transfer_ps"}, fhicl::Comment{"The default ParameterSet to use for transfers. Will have transferPluginType, destination_rank, buffer_count and source_rank overridden. If max_fragment_size_words is unspecified, will be set using fragment_size"}};
		fhicl::Atom<std::string> transfer_to_use{fhicl::Name{"transfer_to_use"}, fhicl::Comment{"The name of the Transfer Plugin to use"}, "Shmem"};
	};

	BrokenTransferTest(fhicl::ParameterSet ps);

	void TestSenderPause();
	void TestReceiverPause();
	void TestSenderReconnect();
	void TestReceiverReconnect( int send_throttle_us=0 );

private:
	struct received_event
	{
		artdaq::Fragment first_frag;
		artdaq::Fragment second_frag;
		std::chrono::steady_clock::time_point open_time;
	};

	fhicl::ParameterSet make_transfer_ps_(int sender_rank, int receiver_rank, std::string name);

	void start_test_();
	void stop_test_();

	void do_sending_(int sender_rank);
	void do_receiving_(int sender_rank, int receiver_rank);

	artdaq::Fragment::sequence_id_t sequence_id_target_();
	void usleep_for_n_fragments_(size_t n)
	{
		// n Fragments * 1000000 us/s / fragment_rate_hz_ fragments/s ==> usecs for n Fragments
		usleep(n * 1000000 / fragment_rate_hz_);
	}
	void usleep_for_n_buffer_epochs_(size_t n)
	{
		usleep_for_n_fragments_(n * (event_buffer_count_ + transfer_buffer_count_));
	}

	std::string fm_(double data, std::string units, int logt = 0);

	boost::thread sender_threads_[2];
	boost::thread receiver_threads_[2];

	std::atomic<bool> sender_ready_[2];
	std::atomic<bool> receiver_ready_[2];

	std::atomic<artdaq::Fragment::sequence_id_t> sender_current_fragment_[2];
	std::atomic<int> sender_tokens_[2];

	fhicl::ParameterSet ps_;

	std::chrono::steady_clock::time_point test_start_time_;  ///< Tests are synchronized by time/rate
	std::chrono::steady_clock::time_point test_end_time_;
	std::atomic<bool> test_end_requested_;
	std::atomic<size_t> fragment_rate_hz_;
	std::atomic<bool> pause_first_sender_;
	std::atomic<bool> pause_receiver_;
	std::atomic<bool> kill_first_sender_;
	std::atomic<bool> kill_receiver_;

	std::atomic<bool> reliable_mode_;
	size_t fragment_size_;
	size_t send_timeout_us_;

	std::map<artdaq::Fragment::sequence_id_t, received_event> event_buffer_;
	std::set<artdaq::Fragment::sequence_id_t> timeout_events_;
	std::set<artdaq::Fragment::sequence_id_t> complete_events_;
	size_t transfer_buffer_count_;
	size_t event_buffer_count_;
	size_t event_buffer_timeout_us_;
	int send_throttle_us_;
	std::mutex event_buffer_mutex_;
	std::condition_variable event_buffer_cv_;
};
}  // namespace artdaqtest

#endif  // artdaq_test_TransferPlugins_BrokenTransferTest_hh
