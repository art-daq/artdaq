#ifndef TCPSocketTransfer_hh
#define TCPSocketTransfer_hh
// This file (TCPSocketTransfer.hh) was created by Ron Rechenmacher <ron@fnal.gov> on
// Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
// or COPYING file. If you do not have such a file, one can be obtained by
// contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
// $RCSfile: .emacs.gnu,v $
// rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

// C Includes
#include <stdint.h>				// uint64_t
#include <sys/uio.h>			// iovec

// C++ Includes
#include <condition_variable>
#include <boost/thread.hpp>

// Products includes
#include "fhiclcpp/fwd.h"

// artdaq Includes
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/TransferPlugins/detail/SRSockets.hh"
#include "artdaq/TransferPlugins/detail/Timeout.hh"	// Timeout
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/detail/HostMap.hh"

namespace artdaq
{
	class TCPSocketTransfer;
}

/**
 * \brief TransferInterface implementation plugin that sends data using TCP sockets
 */
class artdaq::TCPSocketTransfer : public TransferInterface
{
public:
	/**
	 * \brief TCPSocketTransfer Constructor
	 * \param ps ParameterSet used to configure TCPSocketTransfer
	 * \param role Role of this TCPSocketTransfer instance (kSend or kReceive)
	 * 
	 * \verbatim
	 * TCPSocketTransfer accepts the following Parameters:
	 * "tcp_receive_buffer_size" (Default: 0): The TCP buffer size on the receive socket
	 * "send_retry_timeout_us" (Default: 1000000): Microseconds between send retries (infinite retries for moveFragment, up to send_timeout_us for copyFragment)
	 * "host_map" (REQUIRED): List of FHiCL tables containing information about other hosts in the system.
	 *   Each table should contain:
	 *   "rank" (Default: RECV_TIMEOUT): Rank of this host
	 *   "host" (Default: "localhost"): Hostname of this host
	 *   "portOffset" (Default: 5500): To avoid collisions, each destination should specify its own port offset.
	 *     All TCPSocketTransfers sending to that destination will add their own rank to make a unique port number.
	 * \endverbatim
	 * TCPSocketTransfer also requires all Parameters for configuring a TransferInterface
	 */
	TCPSocketTransfer(fhicl::ParameterSet const& ps, Role role);

	virtual ~TCPSocketTransfer();

	/**
	* \brief Receive a Fragment Header from the transport mechanism
	* \param[out] header Received Fragment Header
	* \param receiveTimeout Timeout for receive
	* \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
	*/
	int receiveFragmentHeader(detail::RawFragmentHeader& header, size_t receiveTimeout) override;

	/**
	* \brief Receive the body of a Fragment to the given destination pointer
	* \param destination Pointer to memory region where Fragment data should be stored
	* \param wordCount Number of RawDataType words to receive
	* \return The rank the Fragment was received from (should be source_rank), or RECV_TIMEOUT
	*/
	int receiveFragmentData(RawDataType* destination, size_t wordCount) override;

	/**
	* \brief Copy a Fragment to the destination. Same implementation as moveFragment, as TCP is always reliable
	* \param frag Fragment to copy
	* \param timeout_usec Timeout for send, in microseconds
	* \return CopyStatus detailing result of copy
	*/
	CopyStatus copyFragment(Fragment& frag, size_t timeout_usec) override { return sendFragment_(std::move(frag), timeout_usec); }

	/**
	* \brief Move a Fragment to the destination.
	* \param frag Fragment to move
	* \return CopyStatus detailing result of copy
	*/
	CopyStatus moveFragment(Fragment&& frag) override { return sendFragment_(std::move(frag), 0); }


private:

	static int listen_thread_refcount_;
	static std::unique_ptr<boost::thread> listen_thread_;
	static std::map<int, std::set<int>> connected_fds_;
	int send_fd_;
	int active_receive_fd_;
	int last_active_receive_fd_;

	union
	{
		MessHead mh;
		uint8_t mha[sizeof(MessHead)];
	};

	enum class SocketState
	{
		Metadata,
		Data
	};

	SocketState state_;

	size_t offset;
	int target_bytes;
	size_t rcvbuf_;
	size_t sndbuf_;
	size_t send_retry_timeout_us_;

	hostMap_t hostMap_;

	volatile unsigned connect_state : 1; // 0=not "connected" (initial msg not sent)
	unsigned blocking : 1; // compatible with bool (true/false)


	Timeout tmo_;
	bool stats_connect_stop_;
	boost::thread stats_connect_thread_;
	std::condition_variable stopstatscv_;
	std::mutex stopstatscvm_; // protects 'stopcv'

	bool timeoutMessageArmed_; // don't repeatedly print about the send fd not being open...
    size_t not_connected_count_; // Number of times returned RECV_TIMEOUT because no receive sockets open
    size_t receive_err_threshold_; // Number of times TO print RECV_TIMEOUT before starting to return DATA_END
    size_t receive_err_wait_us_; // Amount of time to wait if there are no connected receive sockets

private: // methods
	CopyStatus sendFragment_(Fragment&& frag, size_t timeout_usec);

	CopyStatus sendData_(const void* buf, size_t bytes, size_t tmo);

	CopyStatus sendData_(const struct iovec* iov, int iovcnt, size_t tmo);

	// Thread to drive reconnect_ requests
	void stats_connect_();

	// Sender is responsible for connecting to receiver
	void connect_();

	void reconnect_();

	// Receiver should listen for connections
	void start_listen_thread_();
	void listen_();

	int calculate_port_() const { 
		return destination_rank() + ((GetPartitionNumber() % 22) * 1000) + 10000;
	}
};

#endif // TCPSocketTransfer_hh
