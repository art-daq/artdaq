#ifndef TCPSocketTransfer_hh
#define TCPSocketTransfer_hh
// This file (TCPSocketTransfer.hh) was created by Ron Rechenmacher <ron@fnal.gov> on
// Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
// or COPYING file. If you do not have such a file, one can be obtained by
// contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
// $RCSfile: .emacs.gnu,v $
// rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include <sys/types.h>			// size_t
#include <stdint.h>				// uint64_t
#include "detail/STCPSocketTransfer.hh"
#include <vector>
#include <map>
#include <set>
#include <poll.h>				// struct pollfd
#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
	class TCPSocketTransfer;
}

class artdaq::TCPSocketTransfer {
public:
	static const size_t RECV_TIMEOUT;

	// env.var used: NODE_LIST, MPI_RANK
	TCPSocketTransfer(size_t buffer_count      // might not be needed (buffer_count must equal src_count)
		, uint64_t max_payload_lwrds // 8 byte entities
		, size_t src_count
		, size_t src_start
		, int    rcvbuf = -1
		// 
		// bool routing_master
	);
	~TCPSocketTransfer();

	// recvFragment() puts the next received fragment in frag (via swap), with
	// the source of that fragment as its return value.
	// It is a precondition that a sources_sending() != 0.
	// Returns -1 if tmo
	ssize_t recvFragment(Fragment &frag, ssize_t timeout_usec = 0, ssize_t *sts_out = NULL);

	// Number of sources still not done.
	size_t sourcesActive() const;

	// Are any sources still active (faster)?
	bool anySourceActive() const;

	// Number of sources pending (last fragments still in-flight).
	size_t sourcesPending() const;

private:
	struct ConnInfo {
		ConnInfo(uint64_t max_payload_lwrds)
			:fd(-1), connection_count(0), state(ConnInfo::metadata)
			, frag(max_payload_lwrds), offset(0)
			, target_bytes(sizeof(MessHead)), num_data_rcvs(0) {};
		int           fd;
		uint16_t      sender_rank;  // I've arbitrarily limited our system to 64K ranks
		int           connection_count; // to help detect problems including misconfig
		enum {
			metadata,
			data,
		}             state;  // 0=metadata or 1=data  (not connected when fd==-1
		union {
			MessHead  mh;
			uint8_t   mha[sizeof(MessHead)];
		};
		uint8_t *     buffer;
		Fragment      frag;
		ssize_t       offset;
		int           target_bytes;
		int           num_data_rcvs;
	};

	size_t const buffer_count_;
	uint64_t const max_payload_lwrds_;
	size_t const src_count_;
	size_t const src_start_idx_;

	int my_node_idx_;
	std::vector<ConnInfo> conninfo_;
	std::vector<pollfd>   read_fds_;
	std::map<int, size_t>  fd2conIdx_;
	std::map<size_t, size_t> rank2conIdx_;
	std::map<int, std::set<size_t>> active_numDataRcvs2conIdx_;
	std::map<int, std::set<size_t>> suppressed_numDataRcvs2conIdx_;
	int listen_port_;
	int listen_fd_;
	size_t connect_count_;

private: // methods
	void do_connect_();
	std::vector<pollfd>::iterator close_erase_(std::vector<pollfd>::iterator ii, size_t conIdx);
	void adjust_read_fds_(std::vector<pollfd>::iterator ii, size_t conIdx);
};


#endif // TCPSocketTransfer_hh
#ifndef TCPSocketTransfer_hh
#define TCPSocketTransfer_hh
// This file (TCPSocketTransfer.hh) was created by Ron Rechenmacher <ron@fnal.gov> on
// Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
// or COPYING file. If you do not have such a file, one can be obtained by
// contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
// $RCSfile: .emacs.gnu,v $
// rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include <sys/types.h>			// size_t
#include <stdint.h>				// uint64_t
#include <stddef.h>				// NULL
#include <sys/uio.h>			// iovec
#include <vector>
#include <thread>				// std::thread
#include <condition_variable>
#include "detail/Timeout.hh"	// Timeout
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"

namespace artdaq {
	class TCPSocketTransfer;
}

class artdaq::TCPSocketTransfer {
public:

	TCPSocketTransfer(size_t buffer_count
		, uint64_t max_payload_lwrds // 8 byte entities
		, size_t dst_count
		, size_t dst_start
		, bool broadcast_sends = false
		, bool synchronous_sends = true
		, int sndbuf = -1
	);

	// Make sure we clean up and wait for in-flight sends.
	~TCPSocketTransfer();

	// Send the given Fragment. Return the rank of the destination to which
	// the Fragment was sent.
	ssize_t sendFragment(Fragment &&, ssize_t* sts_out = NULL);
	ssize_t sendFragment(const void* buf, size_t bytes, ssize_t* sts_out = NULL);
	ssize_t sendFragment(const struct iovec *iov, int iovcnt, ssize_t* sts_out = NULL, int sndIdx = -1);

	// How many fragments have been sent using this SHandles object?
	size_t count() const;

	// How many fragments have been sent to a particular destination.
	size_t slotCount(size_t rank) const;

	// Wait for all the data transfers scheduled by calls
	// to MPI_Isend to finish, then return.
	void   waitAll();

private:
	struct ConnInfo {
		std::string dst_name;
		int         dst_port;
		int         fd;
		int         sndbuf_bytes;   // return from getsockopt after setsockopt
		volatile unsigned    connect_state : 1; // 0=not "connected" (initial msg not sent)
		unsigned    blocking : 1;   // compatible with bool (true/false)
	};

	size_t   const   buffer_count_;
	uint64_t const   max_payload_lwrds_;
	size_t   const   dest_count_;
	size_t   const   dest_start_idx_;
	detail::FragCounter sent_frag_count_;

	uint16_t         my_node_idx_;
	int              current_snd_idx_;
	int              sndbuf_;
	std::vector<ConnInfo> conninfo_;  // vector Idx+dest_count_ -> rank

	Timeout          tmo_;   // must be (constructed) before thread (if thread(stats_connect_,this) is created in initializer list
	bool             stats_connect_stop_;
	std::thread      stats_connect_thread_;
	std::condition_variable stopstatscv_;
	std::mutex              stopstatscvm_; // protects 'stopcv'

private: // methods
	void   stats_connect_();
	void   connect_(int sndIdx);
	void   reconnect_();
};

#endif // TCPSocketTransfer_hh
