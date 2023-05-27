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
		TLOG(TLVL_DEBUG + 32) << "Retrieving Fragment " << (current_block_index_ + 1) << " of " << cf.block_count();
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
		TLOG(TLVL_DEBUG + 32) << "Retrieving Fragment Header " << (current_block_index_ + 1) << " of " << cf.block_count();
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
		TLOG(TLVL_DEBUG + 32) << "Retrieving Fragment Data " << (current_block_index_ + 1) << " of " << cf.block_count();
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
		last_send_call_reliable_ = false;
		{
			std::lock_guard<std::mutex> lk(fragment_mutex_);
			if (bundle_fragment_ == nullptr)
			{
				bundle_fragment_.reset(new artdaq::Fragment(fragment.sequenceID() + 1, fragment.fragmentID()));
				bundle_fragment_->setTimestamp(fragment.timestamp());
				bundle_fragment_->reserve(max_hold_size_bytes_ / sizeof(artdaq::RawDataType));
				send_fragment_started_ = std::chrono::steady_clock::now();

				container_fragment_.reset(new ContainerFragmentLoader(*bundle_fragment_));
				container_fragment_->set_missing_data(false);  // Buffer mode is never missing data, even if there IS no data.
			}

			// Eww, we have to copy
			artdaq::Fragment frag(fragment);

			container_fragment_->addFragment(frag, true);
		}
		return send_bundle_fragment_(send_timeout_usec);
	}

	/**
	 * \brief Send a Fragment in reliable mode, using the underlying transfer plugin
	 * \param fragment The Fragment to send
	 * \return A TransferInterface::CopyStatus result variable
	 */
	CopyStatus transfer_fragment_reliable_mode(artdaq::Fragment&& fragment) override
	{
		last_send_call_reliable_ = true;
		{
			std::lock_guard<std::mutex> lk(fragment_mutex_);
			if (bundle_fragment_ == nullptr)
			{
				bundle_fragment_.reset(new artdaq::Fragment(fragment.sequenceID() + 1, fragment.fragmentID()));
				bundle_fragment_->setTimestamp(fragment.timestamp());
				bundle_fragment_->reserve(max_hold_size_bytes_ / sizeof(artdaq::RawDataType));
				send_fragment_started_ = std::chrono::steady_clock::now();

				container_fragment_.reset(new ContainerFragmentLoader(*bundle_fragment_));
				container_fragment_->set_missing_data(false);  // Buffer mode is never missing data, even if there IS no data.
			}

			container_fragment_->addFragment(fragment, true);
		}
		return send_bundle_fragment_(0);
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
	size_t max_hold_size_bytes_;
	int max_hold_time_us_;
	FragmentPtr bundle_fragment_{nullptr};
	std::unique_ptr<ContainerFragmentLoader> container_fragment_{nullptr};
	size_t current_block_index_{0};
	int current_rank_ = 0;

	std::chrono::steady_clock::time_point send_fragment_started_;
	std::unique_ptr<boost::thread> send_timeout_thread_;
	std::atomic<bool> send_timeout_thread_running_{false};
	std::atomic<bool> last_send_call_reliable_{true};
	std::atomic<bool> running_{true};
	std::mutex fragment_mutex_;

	void start_timeout_thread_();
	void send_timeout_thread_proc_();
	CopyStatus send_bundle_fragment_(size_t send_timeout_usec, bool forceSend = false);
	void receive_bundle_fragment_(size_t receiveTimeout);
};
}  // namespace artdaq

artdaq::BundleTransfer::BundleTransfer(const fhicl::ParameterSet& pset, Role role)
    : TransferInterface(pset, role)
    , max_hold_size_bytes_(pset.get<size_t>("max_hold_size_bytes", 0x1000000))  // 16 MB
    , max_hold_time_us_(pset.get<int>("max_hold_time_us", 250000))
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
		send_bundle_fragment_(1000000, true);
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
		TLOG(TLVL_ERROR) << "Caught boost::exception starting Send Timeout thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting Send Timeout thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}
}

void artdaq::BundleTransfer::send_timeout_thread_proc_()
{
	while (send_timeout_thread_running_)
	{
		if (send_bundle_fragment_(1000000) != CopyStatus::kSuccess)
		{
			usleep(5000);
		}
	}
}

artdaq::TransferInterface::CopyStatus artdaq::BundleTransfer::send_bundle_fragment_(size_t send_timeout_usec, bool forceSend)
{
	CopyStatus sts = CopyStatus::kErrorNotRequiringException;
	std::lock_guard<std::mutex> lk(fragment_mutex_);

	if (bundle_fragment_ == nullptr)
	{
		return sts;
	}

	bool send_fragment = forceSend;
	if (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - send_fragment_started_).count() >= max_hold_time_us_)
	{
		send_fragment = true;
	}

	if (bundle_fragment_->dataSizeBytes() >= max_hold_size_bytes_)
	{
		send_fragment = true;
	}

	if (send_fragment)
	{
		if (last_send_call_reliable_)
		{
			sts = theTransfer_->transfer_fragment_reliable_mode(std::move(*bundle_fragment_.get()));
			bundle_fragment_.reset(nullptr);
		}
		else
		{
			while (sts != CopyStatus::kSuccess && send_timeout_thread_running_)
			{
				sts = theTransfer_->transfer_fragment_min_blocking_mode(*bundle_fragment_.get(), send_timeout_usec);
			}
			bundle_fragment_.reset(nullptr);
		}
		return sts;  // Status of actual transfer
	}

	return CopyStatus::kSuccess;  // Waiting on more data
}

void artdaq::BundleTransfer::receive_bundle_fragment_(size_t receiveTimeout)
{
	std::lock_guard<std::mutex> lk(fragment_mutex_);
	bundle_fragment_.reset(new artdaq::Fragment(1));

	TLOG(TLVL_DEBUG + 34) << "Going to receive next bundle fragment";
	current_rank_ = theTransfer_->receiveFragment(*bundle_fragment_, receiveTimeout);
	TLOG(TLVL_DEBUG + 34) << "Done with receiveFragment, current_rank_ = " << current_rank_;

	if (current_rank_ < RECV_SUCCESS)
	{
		bundle_fragment_.reset(nullptr);
	}
	current_block_index_ = 0;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::BundleTransfer)
