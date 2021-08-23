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
/// <summary>
/// A class which simulates several failure modes for TransferPlugins such as sender pause/restart and receiver pause/restart
/// </summary>
class BrokenTransferTest
{
public:
	/// <summary>
	/// Configuration parameters for BrokenTransferTest
	/// </summary>
	struct Config
	{
		/// "fragment_rate_hz" (Default: 10): The rate at which to generate Fragments, in Hz
		fhicl::Atom<size_t> fragment_rate_hz{fhicl::Name{"fragment_rate_hz"}, fhicl::Comment{"The rate at which to generate Fragments, in Hz"}, 10};
		/// "reliable_mode" (Default: true): Whether to use reliable-mode transfers (true) or min-blocking (false)
		fhicl::Atom<bool> reliable_mode{fhicl::Name{"reliable_mode"}, fhicl::Comment{"Whether to use reliable-mode transfers (true) or min-blocking (false)"}, true};
		/// "fragment_size" (Default: 0x10000): The size of generated Fragments, in Fragment words
		fhicl::Atom<size_t> fragment_size{fhicl::Name{"fragment_size"}, fhicl::Comment{"The size of generated Fragments, in Fragment words"}, 0x10000};
		/// "send_timeout_us" (Default: 100000): The timeout for min-blocking mode sends
		fhicl::Atom<size_t> send_timeout_us{fhicl::Name{"send_timeout_us"}, fhicl::Comment{"The timeout for min-blocking mode sends"}, 100000};
		/// "transfer_buffer_count" (Default: 10): The number of buffers in the Transfer Plugins
		fhicl::Atom<size_t> transfer_buffer_count{fhicl::Name{"transfer_buffer_count"}, fhicl::Comment{"The number of buffers in the Transfer Plugins"}, 10};
		/// "event_buffer_count" (Default: 20): The number of "EventBuilder" buffers on the receiver end
		fhicl::Atom<size_t> event_buffer_count{fhicl::Name{"event_buffer_count"}, fhicl::Comment{"The number of \"EventBuilder\" buffers on the receiver end"}, 20};
		/// "event_buffer_timeout_us" (Default: 1000000): The timeout for "EventBuilder" buffers to be marked incomplete and abandoned
		fhicl::Atom<size_t> event_buffer_timeout_us{fhicl::Name{"event_buffer_timeout_us"}, fhicl::Comment{"The timeout for \"EventBuilder\" buffers to be marked incomplete and abandoned"}, 1000000};
		/// "default_transfer_ps" (Default: {}): The default ParameterSet to use for transfers. Will have transferPluginType, destination_rank, buffer_count and source_rank overridden. If max_fragment_size_words is unspecified, will be set using fragment_size
		fhicl::Table<artdaq::TransferInterface::Config> default_transfer_ps{fhicl::Name{"default_transfer_ps"}, fhicl::Comment{"The default ParameterSet to use for transfers. Will have transferPluginType, destination_rank, buffer_count and source_rank overridden. If max_fragment_size_words is unspecified, will be set using fragment_size"}};
		/// "transfer_to_use" (Default: "Shmem"): The name of the Transfer Plugin to use
		fhicl::Atom<std::string> transfer_to_use{fhicl::Name{"transfer_to_use"}, fhicl::Comment{"The name of the Transfer Plugin to use"}, "Shmem"};
	};

	/// <summary>
	/// BrokenTransferTest Constructor
	/// </summary>
	/// <param name="ps">ParameterSet containing BrokenTransferTest configuration</param>
	BrokenTransferTest(const fhicl::ParameterSet& ps);

	/// <summary>
	/// Run the "Sender Paused" test
	/// </summary>
	void TestSenderPause();
	/// <summary>
	/// Run the "Receiver Paused" test
	/// </summary>
	void TestReceiverPause();
	/// <summary>
	/// Run the "Sender Reconnect" test
	/// </summary>
	void TestSenderReconnect();
	/// <summary>
	/// Run the "Receiver Reconnect" test
	/// </summary>
	/// <param name="send_throttle_factor">Amount of time Sender should wait, in units of 1/fragment_rate</param>
	void TestReceiverReconnect(int send_throttle_factor = 0);

private:
	struct received_event
	{
		artdaq::FragmentPtr first_frag;
		artdaq::FragmentPtr second_frag;
		std::chrono::steady_clock::time_point open_time;
	};

	fhicl::ParameterSet make_transfer_ps_(int sender_rank, int receiver_rank, const std::string& name);

	void start_test_();
	void stop_test_();

	void do_sending_(int sender_rank);
	void do_receiving_(int sender_rank, int receiver_rank);

	void throttle_sender_(int sender_rank);
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

	std::string fm_(double data, const std::string& units, int logt = 0);

	boost::thread sender_threads_[2];
	boost::thread receiver_threads_[2];

	std::array<std::atomic<bool>, 2> sender_ready_;
	std::array<std::atomic<bool>, 2> receiver_ready_;

	std::array<std::atomic<artdaq::Fragment::sequence_id_t>, 2> sender_current_fragment_;
	std::array<std::atomic<int>, 2> sender_tokens_;

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
