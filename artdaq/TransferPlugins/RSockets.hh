#ifndef RSockets_hh
#define RSockets_hh
 // This file (RSockets.hh) was created by Ron Rechenmacher <ron@fnal.gov> on
 // Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
 // or COPYING file. If you do not have such a file, one can be obtained by
 // contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
 // $RCSfile: .emacs.gnu,v $
 // rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include <sys/types.h>			// size_t
#include <stdint.h>				// uint64_t
#include "detail/SRSockets.hh"
#include <vector>
#include <map>
#include <set>
#include <poll.h>				// struct pollfd
#include "artdaq-core/Data/Fragment.hh"

namespace artdaq {
  class RSockets;
}

class artdaq::RSockets {
public:
	static const size_t RECV_TIMEOUT;

	// env.var used: NODE_LIST, PMI_RANK
	RSockets(  size_t buffer_count      // might not be needed (buffer_count must equal src_count)
	         , uint64_t max_payload_lwrds // 8 byte entities
	         , size_t src_count
	         , size_t src_start
	         , int    rcvbuf=-1
	         // 
	         // bool routing_master
	         );
	~RSockets();

	// recvFragment() puts the next received fragment in frag (via swap), with
	// the source of that fragment as its return value.
	// It is a precondition that a sources_sending() != 0.
	// Returns -1 if tmo
	ssize_t recvFragment( Fragment &frag, ssize_t timeout_usec = 0, ssize_t *sts_out=NULL );

	// Number of sources still not done.
	size_t sourcesActive() const;

	// Are any sources still active (faster)?
	bool anySourceActive() const;

	// Number of sources pending (last fragments still in-flight).
	size_t sourcesPending() const;

private:
	struct ConnInfo {
		ConnInfo(uint64_t max_payload_lwrds)
              :fd(-1),connection_count(0),state(ConnInfo::metadata)
			  ,frag(max_payload_lwrds),offset(0)
			  ,target_bytes(sizeof(MessHead)),num_data_rcvs(0) {};
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
	std::map<int,size_t>  fd2conIdx_;
	std::map<size_t,size_t> rank2conIdx_;
	std::map<int,std::set<size_t>> active_numDataRcvs2conIdx_;
	std::map<int,std::set<size_t>> suppressed_numDataRcvs2conIdx_;
	int listen_port_;
	int listen_fd_;
	size_t connect_count_;

private: // methods
	void do_connect_();
	std::vector<pollfd>::iterator close_erase_( std::vector<pollfd>::iterator ii, size_t conIdx );
	void adjust_read_fds_( std::vector<pollfd>::iterator ii, size_t conIdx );
};


#endif // RSockets_hh
