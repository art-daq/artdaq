#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/TransferPlugins/TCPSocketTransfer.hh"
#include "artdaq/TransferPlugins/ShmemTransfer.hh"

namespace artdaq
{
	/**
	 * \brief The AutodetectTransfer TransferInterface plugin sets up a
	 * Shmem_transfer plugin or TCPSocket_transfer plugin depending if
	 * the source and destination are on the same host, to maximize
	 * throughput.
	 */
	class AutodetectTransfer : public TransferInterface
	{
	public:
		/**
		 * \brief AutodetectTransfer Constructor
		 * \param pset ParameterSet used to configure AutodetectTransfer
		 * \param role Role of this TransferInterface, either kReceive or kSend
		 */
		AutodetectTransfer(const fhicl::ParameterSet& pset, Role role);

		/**
		 * \brief AutodetectTransfer default Destructor
		 */
		virtual ~AutodetectTransfer() = default;

		/**
		 * \brief Receive a Fragment, using the underlying transfer plugin
		 * \param fragment Output Fragment
		 * \param receiveTimeout Time to wait before returning TransferInterface::RECV_TIMEOUT
		 * \return Rank of sender
		 */
		int receiveFragment(artdaq::Fragment& fragment,
			size_t receiveTimeout) override
		{
			return theTransfer_->receiveFragment(fragment, receiveTimeout);
		}

		/**
		* \brief Receive a Fragment Header from the transport mechanism
		* \param[out] header Received Fragment Header
		* \param receiveTimeout Timeout for receive
		* \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		*/
		int receiveFragmentHeader(detail::RawFragmentHeader& header, size_t receiveTimeout) override
		{
			return theTransfer_->receiveFragmentHeader(header, receiveTimeout);
		}

		/**
		* \brief Receive the body of a Fragment to the given destination pointer
		* \param destination Pointer to memory region where Fragment data should be stored
		* \param wordCount Number of words of Fragment data to receive
		* \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
		*/
		int receiveFragmentData(RawDataType* destination, size_t wordCount) override
		{
			return theTransfer_->receiveFragmentData(destination, wordCount);
		}

		/**
		 * \brief Send a Fragment in non-reliable mode, using the underlying transfer plugin
		 * \param fragment The Fragment to send
		 * \param send_timeout_usec How long to wait before aborting. Defaults to size_t::MAX_VALUE
		 * \return A TransferInterface::CopyStatus result variable
		 */
		CopyStatus copyFragment(artdaq::Fragment& fragment, size_t send_timeout_usec) override
		{
			return theTransfer_->copyFragment(fragment, send_timeout_usec);
		}

		/**
		* \brief Send a Fragment in reliable mode, using the underlying transfer plugin
		* \param fragment The Fragment to send
		* \param send_timeout_usec How long to wait before aborting. Defaults to size_t::MAX_VALUE
		* \return A TransferInterface::CopyStatus result variable
		*/
		CopyStatus moveFragment(artdaq::Fragment&& fragment) override
		{
			return theTransfer_->moveFragment(std::move(fragment));
		}

	private:
		std::unique_ptr<TransferInterface> theTransfer_;
	};
}

artdaq::AutodetectTransfer::AutodetectTransfer(const fhicl::ParameterSet& pset, Role role)
	: TransferInterface(pset, role)
{
	TLOG_TRACE("AutodetectTransfer") << uniqueLabel() << " Begin AutodetectTransfer constructor" << TLOG_ENDL;
	std::string srcHost, destHost;
	auto hosts = pset.get<std::vector<fhicl::ParameterSet>>("host_map");
	for (auto& ps : hosts)
	{
		auto rank = ps.get<int>("rank", -1);
		if (rank == source_rank())
		{
			srcHost = ps.get<std::string>("host", "localhost");
		}
		if (rank == destination_rank())
		{
			destHost = ps.get<std::string>("host", "localhost");
		}
	}
	TLOG_TRACE("AutodetectTransfer") << uniqueLabel() << " ADT: srcHost=" << srcHost << ", destHost=" << destHost << TLOG_ENDL;
	if (srcHost == destHost)
	{
		TLOG_TRACE("AutodetectTransfer") << uniqueLabel() << " ADT: Constructing ShmemTransfer" << TLOG_ENDL;
		theTransfer_.reset(new ShmemTransfer(pset, role));
	}
	else
	{
		TLOG_TRACE("AutodetectTransfer") << uniqueLabel() << " ADT: Constructing TCPSocketTransfer" << TLOG_ENDL;
		theTransfer_.reset(new TCPSocketTransfer(pset, role));
	}
}

DEFINE_ARTDAQ_TRANSFER(artdaq::AutodetectTransfer)
