#include "artdaq/TransferPlugins/TransferInterface.hh"

#include "artdaq/RTIDDS/RTIDDS.hh"

#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include <memory>
#include <iostream>


namespace fhicl
{
	class ParameterSet;
}

// ----------------------------------------------------------------------

namespace artdaq
{
	/**
	* \brief RTIDDSTransfer is a TransferInterface implementation plugin that transfers data using RTI DDS
	*/
	class RTIDDSTransfer : public TransferInterface
	{
	public:
		/**
		 * \brief RTIDDSTransfer default Destructor
		 */
		virtual ~RTIDDSTransfer() = default;

		/**
		* \brief RTIDDSTransfer Constructor
		* \param ps ParameterSet used to configure RTIDDSTransfer
		* \param role Role of this RTIDDSTransfer instance (kSend or kReceive)
		*
		* RTIDDSTransfer only requires the Parameters for configuring a TransferInterface
		*/
		RTIDDSTransfer(fhicl::ParameterSet const& ps, Role role) :
																 TransferInterface(ps, role)
																 , rtidds_reader_(std::make_unique<artdaq::RTIDDS>("RTIDDSTransfer_reader", artdaq::RTIDDS::IOType::reader))
																 , rtidds_writer_(std::make_unique<artdaq::RTIDDS>("RTIDDSTransfer_writer", artdaq::RTIDDS::IOType::writer)) { }

		/**
		* \brief Receive a Fragment using DDS
		* \param[out] fragment Received Fragment
		* \param receiveTimeout Timeout for receive, in microseconds
		* \return Rank of sender or RECV_TIMEOUT
		*/
		int receiveFragment(artdaq::Fragment& fragment,
									   size_t receiveTimeout) override;

		/**
		* \brief Copy a Fragment to the destination.
		* \param fragment Fragment to copy
		* \param send_timeout_usec Timeout for send, in microseconds. Default size_t::MAX_VALUE
		* \return CopyStatus detailing result of copy
		*/
		CopyStatus copyFragment(artdaq::Fragment& fragment,
										size_t send_timeout_usec = std::numeric_limits<size_t>::max()) override;

		/**
		* \brief Move a Fragment to the destination.
		* \param fragment Fragment to move
		* \param send_timeout_usec Timeout for send, in microseconds. Default size_t::MAX_VALUE
		* \return CopyStatus detailing result of copy
		*/
		CopyStatus moveFragment(artdaq::Fragment&& fragment,
										size_t send_timeout_usec = std::numeric_limits<size_t>::max()) override;

	private:
		std::unique_ptr<artdaq::RTIDDS> rtidds_reader_;
		std::unique_ptr<artdaq::RTIDDS> rtidds_writer_;
	};
}

int artdaq::RTIDDSTransfer::receiveFragment(artdaq::Fragment& fragment,
											   size_t receiveTimeout)
{
	bool receivedFragment = false;
	//  static std::size_t consecutive_timeouts = 0;
	//  std::size_t message_after_N_timeouts = 10;

	//  while (!receivedFragment) {

	try
	{
		receivedFragment = rtidds_reader_->octets_listener_.receiveFragmentFromDDS(fragment, receiveTimeout);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::yes,
						 "Error in RTIDDS transfer plugin: caught exception in call to OctetsListener::receiveFragmentFromDDS, rethrowing");
	}

	//    if (!receivedFragment) {

	//      consecutive_timeouts++;

	//      if (consecutive_timeouts % message_after_N_timeouts == 0) {
	//	TLOG_INFO("RTIDDSTransfer") << consecutive_timeouts << " consecutive " << 
	//	  static_cast<float>(receiveTimeout)/1e6 << "-second timeouts calling OctetsListener::receiveFragmentFromDDS, will continue trying..." << TLOG_ENDL;
	//      }
	//    } else {
	//      consecutive_timeouts = 0;
	//    }
	//  }

	//  return 0;

	return receivedFragment ? source_rank() : TransferInterface::RECV_TIMEOUT;
}

artdaq::TransferInterface::CopyStatus
artdaq::RTIDDSTransfer::moveFragment(artdaq::Fragment&& fragment, size_t send_timeout_usec)
{
	(void)&send_timeout_usec; // No-op to get the compiler not to complain about unused parameter

	rtidds_writer_->moveFragmentToDDS_(std::move(fragment));
	return CopyStatus::kSuccess;
}

artdaq::TransferInterface::CopyStatus
artdaq::RTIDDSTransfer::copyFragment(artdaq::Fragment& fragment,
									 size_t send_timeout_usec)
{
	(void) &send_timeout_usec; // No-op to get the compiler not to complain about unused parameter

	rtidds_writer_->copyFragmentToDDS_(fragment);
	return CopyStatus::kSuccess;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::RTIDDSTransfer)

// Local Variables:
// mode: c++
// End:
