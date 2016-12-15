#ifndef TCPSocketTransfer_hh
#define TCPSocketTransfer_hh
// This file (TCPSocketTransfer.hh) was created by Ron Rechenmacher <ron@fnal.gov> on
// Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
// or COPYING file. If you do not have such a file, one can be obtained by
// contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
// $RCSfile: .emacs.gnu,v $
// rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

// C Includes
#include <sys/types.h>			// size_t
#include <stdint.h>				// uint64_t
#include <sys/uio.h>			// iovec
#include <poll.h>				// struct pollfd

// C++ Includes
#include <vector>
#include <map>
#include <set>
#include <vector>
#include <thread>				// std::thread
#include <condition_variable>

// Products includes
#include <fhiclcpp/fwd.h>

// artdaq Includes
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/TransferPlugins/detail/SRSockets.hh"
#include "artdaq/TransferPlugins/detail/Timeout.hh"	// Timeout
#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
	class TCPSocketTransfer;
}

class artdaq::TCPSocketTransfer : public artdaq::TransferInterface {
public:
	// env.var used: NODE_LIST, MPI_RANK
	TCPSocketTransfer(fhicl::ParameterSet const& ps, TransferInterface::Role role);
	~TCPSocketTransfer();

	// recvFragment() puts the next received fragment in frag (via swap), with
	// the source of that fragment as its return value.
	// It is a precondition that a sources_sending() != 0.
	// Returns -1 if tmo
	int receiveFragment(Fragment &frag, size_t timeout_usec = 0);

	// Send the given Fragment. Return the rank of the destination to which
	// the Fragment was sent.
	TransferInterface::CopyStatus copyFragment(Fragment& f, size_t tmo) { return sendFragment(std::move(f), tmo, false); }
	TransferInterface::CopyStatus moveFragment(Fragment&& f, size_t tmo) { return sendFragment(std::move(f), tmo, true); }
	TransferInterface::CopyStatus sendFragment(Fragment &&, size_t, bool);

private:
	int         fd_;
	int		listen_fd_;
	size_t              sndbuf_;
	size_t              rcvbuf_;

	union {
		MessHead  mh;
		uint8_t   mha[sizeof(MessHead)];
	};
	enum class SocketState {
		Metadata,
		Data
	};
	SocketState state_;
	
	Fragment      frag;
	uint8_t *     buffer;
	size_t       offset;
	int           target_bytes;

	struct DestinationInfo {
		std::string hostname;
		int portOffset;
	};
	std::unordered_map<size_t, DestinationInfo> hostMap_;

	volatile unsigned    connect_state : 1; // 0=not "connected" (initial msg not sent)
	unsigned    blocking : 1;   // compatible with bool (true/false)


	Timeout tmo_;
	bool             stats_connect_stop_;
	std::thread      stats_connect_thread_;
	std::condition_variable stopstatscv_;
	std::mutex              stopstatscvm_; // protects 'stopcv'

private: // methods
	TransferInterface::CopyStatus sendFragment_(const void* buf, size_t bytes, size_t tmo, bool needToken);
	TransferInterface::CopyStatus sendFragment_(const struct iovec *iov, int iovcnt, size_t tmo, bool needToken);

	// Thread to drive reconnect_ requests
	void   stats_connect_();

	// Sender is responsible for connecting to receiver
	void connect_();
	void reconnect_();
	// If necessary, get a token from the receiver before sending
	int getToken_();

	// Receiver should listen for connections
	void listen_();
	// Indicate to sender (if it cares) that we're ready for another buffer
	void post_();

	int calculate_port_() const { return (hostMap_.at(destination_rank())).portOffset + source_rank(); }
};

#endif // TCPSocketTransfer_hh
