#include "artdaq/TransferPlugins/TransferInterface.hh"

namespace artdaq {
/**
 * \brief NullTransfer does not send or receive data, but acts as if it did
 */
class NullTransfer : public TransferInterface
{
public:
	/**
	 * \brief NullTransfer constructor
	 * \param pset ParameterSet used to configure TransferInterface
	 * \param role Role of this NullTransfer instance (kSend or kReceive)
	 *
	 * NullTransfer only requires the Parameters for configuring a TransferInterface
	 */
	NullTransfer(const fhicl::ParameterSet& pset, Role role);

	/**
	 * \brief NullTransfer default Destructor
	 */
	~NullTransfer() override = default;

	/**
	 * \brief Pretend to receive a Fragment
	 * \return Source Rank (Success code)
	 *
	 * WARNING: This function may create unintended side-effets. NullTransfer should
	 * only really be used in Role::kSend!
	 */
	int receiveFragment(artdaq::Fragment& /*fragment*/, size_t /*receiveTimeout*/) override { return source_rank(); }

	/**
	 * \brief Pretend to receive a Fragment Header
	 * \return Source Rank (Success code)
	 *
	 * WARNING: This function may create unintended side-effets. NullTransfer should
	 * only really be used in Role::kSend!
	 */
	int receiveFragmentHeader(detail::RawFragmentHeader& /*header*/, size_t /*receiveTimeout*/) override { return source_rank(); }

	/**
	 * \brief Pretend to receive Fragment Data
	 * \return Source Rank (Success code)
	 *
	 * WARNING: This function may create unintended side-effets. NullTransfer should
	 * only really be used in Role::kSend!
	 */
	int receiveFragmentData(RawDataType* /*destination*/ /*destination*/ /*destination*/ /*destination*/, size_t /*wordCount*/) override { return source_rank(); }

	/**
	 * \brief Pretend to send a Fragment to a destination
	 * \return CopyStatus::kSuccess (No-Op)
	 */
	CopyStatus transfer_fragment_min_blocking_mode(artdaq::Fragment const& /*fragment*/, size_t /*send_timeout_usec*/) override
	{
		return CopyStatus::kSuccess;
	}

	/**
	 * \brief Pretend to send a Fragment to a destination
	 * \return CopyStatus::kSuccess (No-Op)
	 */
	CopyStatus transfer_fragment_reliable_mode(artdaq::Fragment&& /*fragment*/) override { return CopyStatus::kSuccess; }

	/**
	 * \brief Determine whether the TransferInterface plugin is able to send/receive data
	 * \return True if the TransferInterface plugin is currently able to send/receive data
	 */
	bool isRunning() override { return true; }

	/**
	 * \brief Flush any in-flight data. This should be used by the receiver after the receive loop has
	 * ended.
	 */
	void flush_buffers() override {}

private:
	NullTransfer(NullTransfer const&) = delete;
	NullTransfer(NullTransfer&&) = delete;
	NullTransfer& operator=(NullTransfer const&) = delete;
	NullTransfer& operator=(NullTransfer&&) = delete;
};
}  // namespace artdaq

artdaq::NullTransfer::NullTransfer(const fhicl::ParameterSet& pset, Role role)
    : TransferInterface(pset, role) {}

DEFINE_ARTDAQ_TRANSFER(artdaq::NullTransfer)
