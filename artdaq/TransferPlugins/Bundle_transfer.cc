#include <memory>

#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_BundleTransfer").c_str()

#include "artdaq-core/Data/ContainerFragmentLoader.hh"
#include "artdaq/TransferPlugins/TCPSocketTransfer.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

#include <boost/thread.hpp>

namespace artdaq {
/**
 * \brief The BundleTransfer TransferInterface plugin sets up a
 * Shmem_transfer plugin or TCPSocket_transfer plugin depending if
 * the source and destination are on the same host, to maximize
 * throughput.
 */
class BundleTransfer : public TransferInterface
{
public:
	/**
	 * \brief BundleTransfer Constructor
	 * \param pset ParameterSet used to configure BundleTransfer
	 * \param role Role of this TransferInterface, either kReceive or kSend
	 */
	BundleTransfer(const fhicl::ParameterSet& pset, Role role);

	/**
	 * \brief BundleTransfer default Destructor
	 */
	~BundleTransfer() override;

	/**
	 * \brief Receive a Fragment, using the underlying transfer plugin
	 * \param fragment Output Fragment
	 * \param receiveTimeout Time to wait before returning TransferInterface::RECV_TIMEOUT
	 * \return Rank of sender
	 */
	int receiveFragment(artdaq::Fragment& fragment,
	                    size_t receiveTimeout) override
	{
		if (bundle_fragment_ == nullptr)
		{
			receive_bundle_fragment_(receiveTimeout);
			if (current_rank_ < RECV_SUCCESS) return current_rank_;
		}

		ContainerFragment cf(*bundle_fragment_);
		TLOG(TLVL_DEBUG + 32) << GetTraceName() << "Retrieving Fragment " << (current_block_index_ + 1) << " of " << cf.block_count();
		fragment.resizeBytes(cf.fragSize(current_block_index_) - sizeof(detail::RawFragmentHeader));
		memcpy(fragment.headerAddress(), static_cast<const uint8_t*>(cf.dataBegin()) + cf.fragmentIndex(current_block_index_), cf.fragSize(current_block_index_));
		current_block_index_++;
		if (current_block_index_ >= cf.block_count())  // Index vs. count!
		{
			bundle_fragment_.reset(nullptr);
		}
		return current_rank_;
	}

	/**
	 * \brief Receive a Fragment Header from the transport mechanism
	 * \param[out] header Received Fragment Header
	 * \param receiveTimeout Timeout for receive
	 * \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
	 */
	int receiveFragmentHeader(detail::RawFragmentHeader& header, size_t receiveTimeout) override
	{
		if (bundle_fragment_ == nullptr)
		{
			receive_bundle_fragment_(receiveTimeout);
			if (current_rank_ < RECV_SUCCESS) return current_rank_;
		}
		ContainerFragment cf(*bundle_fragment_);
		TLOG(TLVL_DEBUG + 32) << GetTraceName() << "Retrieving Fragment Header " << (current_block_index_ + 1) << " of " << cf.block_count();
		memcpy(&header, static_cast<const uint8_t*>(cf.dataBegin()) + cf.fragmentIndex(current_block_index_), sizeof(detail::RawFragmentHeader));
		return current_rank_;
	}

	/**
	 * \brief Receive the body of a Fragment to the given destination pointer
	 * \param destination Pointer to memory region where Fragment data should be stored
	 * \param wordCount Number of words of Fragment data to receive
	 * \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
	 */
	int receiveFragmentData(RawDataType* destination, size_t /*wordCount*/) override
	{
		if (bundle_fragment_ == nullptr)  // Should be impossible!
		{
			return RECV_TIMEOUT;
		}
		ContainerFragment cf(*bundle_fragment_);
		TLOG(TLVL_DEBUG + 32) << GetTraceName() << "Retrieving Fragment Data " << (current_block_index_ + 1) << " of " << cf.block_count();
		memcpy(destination, static_cast<const uint8_t*>(cf.dataBegin()) + cf.fragmentIndex(current_block_index_) + sizeof(detail::RawFragmentHeader), cf.fragSize(current_block_index_) - sizeof(detail::RawFragmentHeader));
		current_block_index_++;
		if (current_block_index_ >= cf.block_count())  // Index vs. count!
		{
			bundle_fragment_.reset(nullptr);
		}
		return current_rank_;
	}

	/**
	 * \brief Send a Fragment in non-reliable mode, using the underlying transfer plugin
	 * \param fragment The Fragment to send
	 * \param send_timeout_usec How long to wait before aborting. Defaults to size_t::MAX_VALUE
	 * \return A TransferInterface::CopyStatus result variable
	 */
	CopyStatus transfer_fragment_min_blocking_mode(artdaq::Fragment const& fragment, size_t send_timeout_usec) override
	{
		TLOG(TLVL_DEBUG + 35) << GetTraceName() << "transfer_fragment_min_blocking_mode START";
		last_send_call_reliable_ = false;
		last_send_timeout_usec_ = send_timeout_usec;
		{
			std::unique_lock<std::mutex> lk(fragment_mutex_);
			if (current_buffer_size_bytes_ > max_hold_size_bytes_)
			{
				fragment_cv_.wait_for(lk, std::chrono::microseconds(send_timeout_usec), [&] { return current_buffer_size_bytes_ < max_hold_size_bytes_; });
			}

			if (current_buffer_size_bytes_ > max_hold_size_bytes_)
			{
				TLOG(TLVL_WARNING) << GetTraceName() << "Dropping data due to timeout in min_blocking_mode";
				return CopyStatus::kTimeout;
			}

			TLOG(TLVL_DEBUG + 35) << GetTraceName() << "transfer_fragment_min_blocking_mode after wait for buffer";
			// Always send along System Fragments immediately
			if (Fragment::isSystemFragmentType(fragment.type()))
			{
				system_fragment_cached_ = true;
			}

			current_buffer_size_bytes_ += fragment.sizeBytes();
			// Eww, we have to copy
			fragment_buffer_.emplace_back(fragment);
		}
		TLOG(TLVL_DEBUG + 35) << GetTraceName() << "transfer_fragment_min_blocking_mode END";
		return CopyStatus::kSuccess;  // Might be a lie, but we're going to send from the thread proc
	}

	/**
	 * \brief Send a Fragment in reliable mode, using the underlying transfer plugin
	 * \param fragment The Fragment to send
	 * \return A TransferInterface::CopyStatus result variable
	 */
	CopyStatus transfer_fragment_reliable_mode(artdaq::Fragment&& fragment) override
	{
		TLOG(TLVL_DEBUG + 36) << GetTraceName() << "transfer_fragment_reliable_mode START";
		last_send_call_reliable_ = true;
		{
			std::unique_lock<std::mutex> lk(fragment_mutex_);
			if (current_buffer_size_bytes_ > max_hold_size_bytes_)
			{
				fragment_cv_.wait(lk, [&] { return current_buffer_size_bytes_ < max_hold_size_bytes_; });
			}

			TLOG(TLVL_DEBUG + 36) << GetTraceName() << "transfer_fragment_reliable_mode after wait for buffer";

			// Always send along System Fragments immediately
			if (Fragment::isSystemFragmentType(fragment.type()))
			{
				system_fragment_cached_ = true;
			}

			current_buffer_size_bytes_ += fragment.sizeBytes();
			fragment_buffer_.emplace_back(std::move(fragment));
		}
		TLOG(TLVL_DEBUG + 36) << GetTraceName() << "transfer_fragment_reliable_mode END";
		return CopyStatus::kSuccess;  // Might be a lie, but we're going to send from the thread proc
	}

	/**
	 * \brief Determine whether the TransferInterface plugin is able to send/receive data
	 * \return True if the TransferInterface plugin is currently able to send/receive data
	 */
	bool isRunning() override { return running_; }

	/**
	 * \brief Flush any in-flight data. This should be used by the receiver after the receive loop has
	 * ended.
	 */
	void flush_buffers() override { theTransfer_->flush_buffers(); }

private:
	BundleTransfer(BundleTransfer const&) = delete;
	BundleTransfer(BundleTransfer&&) = delete;
	BundleTransfer& operator=(BundleTransfer const&) = delete;
	BundleTransfer& operator=(BundleTransfer&&) = delete;

private:
	std::unique_ptr<TransferInterface> theTransfer_;
	size_t send_threshold_bytes_;
	size_t max_hold_size_bytes_;
	int max_hold_time_us_;
	FragmentPtr bundle_fragment_{nullptr};
	Fragments fragment_buffer_;
	size_t current_block_index_{0};
	int current_rank_ = 0;

	std::chrono::steady_clock::time_point send_fragment_started_;
	std::atomic<size_t> current_buffer_size_bytes_{0};
	std::unique_ptr<boost::thread> send_timeout_thread_;
	std::atomic<bool> system_fragment_cached_{false};
	std::atomic<bool> send_timeout_thread_running_{false};
	std::atomic<bool> last_send_call_reliable_{true};
	std::atomic<size_t> last_send_timeout_usec_{1000000};
	std::atomic<bool> running_{true};
	std::mutex fragment_mutex_;
	std::condition_variable fragment_cv_;

	bool check_send_(bool force);
	void start_timeout_thread_();
	void send_timeout_thread_proc_();
	bool send_bundle_fragment_(bool forceSend = false);
	void receive_bundle_fragment_(size_t receiveTimeout);
};
}  // namespace artdaq

artdaq::BundleTransfer::BundleTransfer(const fhicl::ParameterSet& pset, Role role)
    : TransferInterface(pset, role)
    , send_threshold_bytes_(pset.get<size_t>("send_threshold_bytes", 100 * 0x100000))  // 100 MB
    , max_hold_size_bytes_(pset.get<size_t>("max_hold_size_bytes", 1000 * 0x100000))  // 1000 MB
    , max_hold_time_us_(pset.get<int>("max_hold_time_us", 5000000))                  // 5 s
{
	TLOG(TLVL_INFO) << GetTraceName() << "Begin BundleTransfer constructor";
	TLOG(TLVL_INFO) << GetTraceName() << "Constructing TCPSocketTransfer";
	theTransfer_ = std::make_unique<TCPSocketTransfer>(pset, role);

	if (role == Role::kSend)
	{
		start_timeout_thread_();
	}
}

artdaq::BundleTransfer::~BundleTransfer()
{
	if (role_ == Role::kSend)
	{
		send_timeout_thread_running_ = false;
		if (send_timeout_thread_ && send_timeout_thread_->joinable())
		{
			send_timeout_thread_->join();
		}
		send_bundle_fragment_(true);
	}
	running_ = false;
}

void artdaq::BundleTransfer::start_timeout_thread_()
{
	if (send_timeout_thread_ && send_timeout_thread_->joinable())
	{
		send_timeout_thread_->join();
	}
	send_timeout_thread_running_ = true;
	TLOG(TLVL_INFO) << GetTraceName() << "Starting Send Timeout Thread";

	try
	{
		send_timeout_thread_ = std::make_unique<boost::thread>(&BundleTransfer::send_timeout_thread_proc_, this);
		char tname[16];                                            // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-SNDTMO", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                           // assure term. snprintf is not too evil :)
		auto handle = send_timeout_thread_->native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << GetTraceName() << "Caught boost::exception starting Send Timeout thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << GetTraceName() << "Caught boost::exception starting Send Timeout thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}

void artdaq::BundleTransfer::send_timeout_thread_proc_()
{
	while (send_timeout_thread_running_)
	{
		if (!send_bundle_fragment_())
		{
			usleep(5000);
		}
	}
}

bool artdaq::BundleTransfer::check_send_(bool force)
{
	if (force)
	{
		TLOG(TLVL_DEBUG + 37) << GetTraceName() << "check_send_: Send is forced, returning true";
		return true;
	}

	if (system_fragment_cached_.load())
	{
		TLOG(TLVL_DEBUG + 37) << GetTraceName() << "check_send_: System Fragment in cache, returning true";
		return true;
	}

	if (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - send_fragment_started_).count() >= max_hold_time_us_)
	{
		TLOG(TLVL_DEBUG + 37) << GetTraceName() << "check_send_: Send timeout reached, returning true";
		return true;
	}

	if (current_buffer_size_bytes_ >= send_threshold_bytes_)
	{
		TLOG(TLVL_DEBUG + 37) << GetTraceName() << "check_send_: Buffer is full, returning true";
		return true;
	}

	TLOG(TLVL_DEBUG + 37) << GetTraceName() << "check_send_: returning false";
	return false;
}

bool artdaq::BundleTransfer::send_bundle_fragment_(bool forceSend)
{
	{
		std::unique_lock<std::mutex> lk(fragment_mutex_);

		bool send_fragment = check_send_(forceSend);

		if (send_fragment && fragment_buffer_.size() > 0)
		{
			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Swapping in new buffer";
			Fragments temp_buffer;
			size_t size = current_buffer_size_bytes_;
			fragment_buffer_.swap(temp_buffer);
			send_fragment_started_ = std::chrono::steady_clock::now();
			current_buffer_size_bytes_ = 0;
			lk.unlock();
			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Notifying waiters";
			fragment_cv_.notify_one();

			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Setting up Bundle Fragment";
			bundle_fragment_.reset(new artdaq::Fragment(temp_buffer.front().sequenceID() + 1, temp_buffer.front().fragmentID()));
			bundle_fragment_->setTimestamp(temp_buffer.front().timestamp());
			bundle_fragment_->reserve(size / sizeof(artdaq::RawDataType));

			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Filling Bundle Fragment";
			ContainerFragmentLoader container_fragment(*bundle_fragment_);
			container_fragment.set_missing_data(false);  // Buffer mode is never missing data, even if there IS no data.
			container_fragment.addFragments(temp_buffer, true);
			temp_buffer.clear();

			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Sending Fragment, reliable mode " << last_send_call_reliable_.load();
			CopyStatus sts = CopyStatus::kSuccess;
			if (last_send_call_reliable_)
			{
				sts = theTransfer_->transfer_fragment_reliable_mode(std::move(*bundle_fragment_.get()));
				bundle_fragment_.reset(nullptr);
			}
			else
			{
				while (sts != CopyStatus::kSuccess && send_timeout_thread_running_)
				{
					sts = theTransfer_->transfer_fragment_min_blocking_mode(*bundle_fragment_.get(), last_send_timeout_usec_);
				}
				bundle_fragment_.reset(nullptr);
			}

			if (sts != CopyStatus::kSuccess)
			{
				auto sts_string = sts == CopyStatus::kTimeout ? "timeout" : "other error";
				TLOG(TLVL_WARNING) << GetTraceName() << "Transfer of Bundle fragment returned status " << sts_string;
			}

			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Done sending Bundle Fragment";

			return true;  // Status of actual transfer
		}
	}
	return false;  // Waiting on more data
}

void artdaq::BundleTransfer::receive_bundle_fragment_(size_t receiveTimeout)
{
	std::lock_guard<std::mutex> lk(fragment_mutex_);
	bundle_fragment_.reset(new artdaq::Fragment(1));

	TLOG(TLVL_DEBUG + 34) << GetTraceName() << "Going to receive next bundle fragment";
	current_rank_ = theTransfer_->receiveFragment(*bundle_fragment_, receiveTimeout);
	TLOG(TLVL_DEBUG + 34) << GetTraceName() << "Done with receiveFragment, current_rank_ = " << current_rank_;

	if (current_rank_ < RECV_SUCCESS)
	{
		bundle_fragment_.reset(nullptr);
	}
	current_block_index_ = 0;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::BundleTransfer)
