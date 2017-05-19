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
		 * \brief Send a Fragment in non-reliable mode, using the underlying transfer plugin
		 * \param fragment The Fragment to send
		 * \param send_timeout_usec How long to wait before aborting. Defaults to size_t::MAX_VALUE
		 * \return A TransferInterface::CopyStatus result variable
		 */
		CopyStatus copyFragment(artdaq::Fragment& fragment,
		                                size_t send_timeout_usec = std::numeric_limits<size_t>::max()) override
		{
			return theTransfer_->copyFragment(fragment, send_timeout_usec);
		}

		/**
		* \brief Send a Fragment in reliable mode, using the underlying transfer plugin
		* \param fragment The Fragment to send
		* \param send_timeout_usec How long to wait before aborting. Defaults to size_t::MAX_VALUE
		* \return A TransferInterface::CopyStatus result variable
		*/
		CopyStatus moveFragment(artdaq::Fragment&& fragment,
		                                size_t send_timeout_usec = std::numeric_limits<size_t>::max()) override
		{
			return theTransfer_->moveFragment(std::move(fragment), send_timeout_usec);
		}

	private:
		std::unique_ptr<TransferInterface> theTransfer_;
	};
}

artdaq::AutodetectTransfer::AutodetectTransfer(const fhicl::ParameterSet& pset, Role role)
	: TransferInterface(pset, role)
{
	TLOG_DEBUG(uniqueLabel()) << "Begin AutodetectTransfer constructor" << TLOG_ENDL;
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
	TLOG_DEBUG(uniqueLabel()) << "ADT: srcHost=" << srcHost << ", destHost=" << destHost << TLOG_ENDL;
	if (srcHost == destHost)
	{
		TLOG_DEBUG(uniqueLabel()) << "ADT: Constructing ShmemTransfer" << TLOG_ENDL;
		theTransfer_.reset(new ShmemTransfer(pset, role));
	}
	else
	{
		TLOG_DEBUG(uniqueLabel()) << "ADT: Constructing TCPSocketTransfer" << TLOG_ENDL;
		theTransfer_.reset(new TCPSocketTransfer(pset, role));
	}
}

DEFINE_ARTDAQ_TRANSFER(artdaq::AutodetectTransfer)
