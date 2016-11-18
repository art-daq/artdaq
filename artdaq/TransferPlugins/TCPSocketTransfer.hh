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

	// env.var used: NODE_LIST, PMI_RANK
	TCPSocketTransfer(  size_t buffer_count      // might not be needed (buffer_count must equal src_count)
	         , uint64_t max_payload_lwrds // 8 byte entities
	         , size_t src_count
	         , size_t src_start
	         , int    rcvbuf=-1
	         // 
	         // bool routing_master
	         );
	~TCPSocketTransfer();

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

	TCPSocketTransfer(  size_t buffer_count
	         , uint64_t max_payload_lwrds // 8 byte entities
	         , size_t dst_count
	         , size_t dst_start
	         , bool broadcast_sends=false
	         , bool synchronous_sends=true
	         , int sndbuf=-1
	         );

	// Make sure we clean up and wait for in-flight sends.
	~TCPSocketTransfer();

	// Send the given Fragment. Return the rank of the destination to which
	// the Fragment was sent.
	ssize_t sendFragment( Fragment &&, ssize_t* sts_out=NULL );
	ssize_t sendFragment( const void* buf, size_t bytes, ssize_t* sts_out=NULL );
	ssize_t sendFragment( const struct iovec *iov, int iovcnt, ssize_t* sts_out=NULL, int sndIdx=-1 );

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
		volatile unsigned    connect_state:1; // 0=not "connected" (initial msg not sent)
		unsigned    blocking:1;   // compatible with bool (true/false)
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
	void   connect_( int sndIdx );
	void   reconnect_();
};

#endif // TCPSocketTransfer_hh
 // This file (TCPSocketTransfer.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
 // Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
 // or COPYING file. If you do not have such a file, one can be obtained by
 // contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
 // $RCSfile: .emacs.gnu,v $
 // rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include "TCPSocketTransfer.hh"
#include "detail/TCPConnect.hh"
#include "detail/Timeout.hh"
#include "detail/STCPSocketTransfer.hh"
#define TRACE_NAME "TCPSocketTransfer"
#include "trace.h"				// TRACE
#include <stdlib.h>				// atoi, strtoul
#include <sys/socket.h>		// getsockopt, SOL_SOCKET, SO_SNDBUF
#include <stdexcept>
#include <string>
#include <fstream>
#include <assert.h>
#include <arpa/inet.h>			// htonl, htons
#include "artdaq-core/Data/Fragment.hh"


artdaq::TCPSocketTransfer::
TCPSocketTransfer(  size_t buffer_count
         , uint64_t max_payload_lwrds
         , size_t dst_count
         , size_t dst_start
         , bool broadcast_sends   __attribute__((unused))
         , bool synchronous_sends __attribute__((unused))
         , int    sndbuf
         )
  :
  buffer_count_(buffer_count),
  max_payload_lwrds_(max_payload_lwrds),
  dest_count_(dst_count<=0xffff?dst_count:0),
  dest_start_idx_(dst_start),
  sent_frag_count_(dst_count, dst_start),
  my_node_idx_(atoi(getenv("PMI_RANK"))),   // rank is from MPI env. ("SHandles")
  current_snd_idx_(0),
  sndbuf_(sndbuf),
  conninfo_(dst_count<=0xffff?dst_count:0),
  stats_connect_stop_(false),
  stats_connect_thread_(std::bind(&TCPSocketTransfer::stats_connect_,this))
{
	TRACE( 4, "TCPSocketTransfer ctor max_payload_lwrds=%lu dst_cnt=%lu dst_start=%lu"
	      , max_payload_lwrds, dest_count_, dest_start_idx_ );
	char* envvar;
	if ((envvar=getenv("NODE_LIST"))) {
		TRACE( 3, "NODE_LIST="+std::string(envvar)+" dstrtIdx=%d dcnt=%d"
		      , dst_start, dest_count_);
		std::ifstream node_list_file(envvar);
		std::string line;
		for (size_t ii=0; ii<dest_start_idx_; ++ii)
			getline(node_list_file, line);
		for (size_t ii=0; ii<dest_count_; ++ii) {
			getline(node_list_file, line);
			conninfo_[ii].dst_name = line.substr(0,line.find(":"));
			conninfo_[ii].dst_port = atoi( line.substr(line.find(":")+1).c_str() );
			connect_( ii ); // MAY FAIL -- periodic reconnect should connect it later
		}
		std::function<void()> function = std::bind( &TCPSocketTransfer::reconnect_,this );
		tmo_.add_periodic( "reconnect", NULL, function, 200/*millisec*/ );
		// check if all connected
		for (size_t ii=0; ii<dest_count_; ++ii) {
			while (conninfo_[ii].connect_state == 0) {
				TRACE(2, "waiting for dst rank %lu", dst_start+ii );
				sleep( 3 );
			}
		}
	}
	else
		throw std::runtime_error("ERROR: NODE_LIST env.var. not set.");
	if (dst_count>0xffff)
		throw std::runtime_error("ERROR: TCPSocketTransfer::TCPSocketTransfer ctor dst_count too big");
}

artdaq::TCPSocketTransfer::~TCPSocketTransfer()
{
	for (size_t ii=0; ii<conninfo_.size(); ++ii) {
		size_t nFragments=sent_frag_count_.slotCount(ii+dest_start_idx_);
		artdaq::Fragment grab_ownership_frag=std::move(*Fragment::eodFrag(nFragments));
		iovec iov = { (void*)grab_ownership_frag.headerBegin(), grab_ownership_frag.sizeBytes() };
		sendFragment( &iov, 1, NULL, ii );
	}

    stats_connect_stop_ = true;
	stopstatscv_.notify_all();
    stats_connect_thread_.join();


    // close all open connections (send stop_v0) first
	MessHead mh={0,MessHead::stop_v0,htons(my_node_idx_),0};
	for (size_t ii=0; ii<conninfo_.size(); ++ii) {
		if (conninfo_[ii].fd != -1) {
			// should be blocking with modest timeo
		    timeval tv={0,100000};
			socklen_t len=sizeof(tv);
		    setsockopt( conninfo_[ii].fd, SOL_SOCKET,SO_SNDTIMEO, &tv,len );
			write( conninfo_[ii].fd, &mh, sizeof(mh) );
			close( conninfo_[ii].fd );
		}
	}
	TRACE( 4, "TCPSocketTransfer dtor" );
}


// Send the given Fragment. Return the rank of the destination to which
// the Fragment was sent OR -1 if to none.
ssize_t artdaq::TCPSocketTransfer::sendFragment( Fragment &&frag, ssize_t* sts_out )
{
	if (frag.type() == Fragment::EndOfDataFragmentType) {
		throw cet::exception("LogicError")
			<< "EOD fragments should not be sent on as received: "
			<< "use sendEODFrag() instead.";
	}
	size_t sndIdx=frag.sequenceID() % dest_count_;
	artdaq::Fragment grab_ownership_frag=std::move(frag);
	iovec iov = { (void*)grab_ownership_frag.headerBegin(), grab_ownership_frag.sizeBytes() };
	return sendFragment( &iov, 1, sts_out, sndIdx );
}
ssize_t artdaq::TCPSocketTransfer::sendFragment( const void* buf, size_t bytes, ssize_t* sts_out )
{
	iovec iov = { (void*)buf, bytes };
	return sendFragment( &iov, 1, sts_out );
}
ssize_t artdaq::TCPSocketTransfer::sendFragment( const struct iovec *iov, int iovcnt, ssize_t* sts_out, int sndIdx )
{
    // check all connected??? -- currently just check fd!=-1
	if (sndIdx == -1)
		sndIdx=current_snd_idx_++%dest_count_;
	else if (sndIdx >= (int)dest_count_) {
		return -1;
	}
	if (conninfo_[sndIdx].fd == -1) {
		if (sts_out)
			*sts_out=-1;
		return -1;
	}

	uint32_t total_to_write_bytes=0;
	std::vector<iovec> iov_in(iovcnt+1); // need contigous (for the unlike case that only partial MH
	std::vector<iovec> iovv(iovcnt+2); // 1 more for mh and another one for any partial
	int ii;
	for (ii=0; ii<iovcnt; ++ii) {
		iov_in[ii+1] = iov[ii];
		total_to_write_bytes+=iov[ii].iov_len;
	}
	MessHead mh={0,MessHead::data_v0,htons(my_node_idx_),htonl(total_to_write_bytes)};
	iov_in[0].iov_base = &mh;
	iov_in[0].iov_len  = sizeof(mh);
	total_to_write_bytes += sizeof(mh);

	ssize_t sts=0;
	ssize_t total_written_bytes=0;
	ssize_t per_write_max_bytes=(32*1024);

	size_t in_iov_idx=0;  // only increment this when we know the associated data has been xferred
	size_t out_iov_idx=0;
	ssize_t this_write_bytes=0;

	do {

		// The first out_iov may be set at the end of the previous loop.
		// iov looping from below (b/c of the latter, we need to check this_write_bytes)
		for (;
			 (in_iov_idx+out_iov_idx)<iov_in.size() && this_write_bytes<per_write_max_bytes;
			 ++out_iov_idx) {
			this_write_bytes += iov_in[in_iov_idx+out_iov_idx].iov_len;
			iovv[out_iov_idx] = iov_in[in_iov_idx+out_iov_idx];
		}
		if (this_write_bytes > per_write_max_bytes) {
			iovv[out_iov_idx-1].iov_len -= this_write_bytes-per_write_max_bytes;
			this_write_bytes  = per_write_max_bytes;
		}

		// need to do blocking algorythm -- including throttled block notifications
 do_again:
		TRACE( 7, "sendFragment b4 writev %7zu total_written_bytes fd=%d in_idx=%zu iovcnt=%zu 1st.len=%zu"
		      , total_written_bytes, conninfo_[sndIdx].fd, in_iov_idx, out_iov_idx, iovv[0].iov_len );

		sts = writev( conninfo_[sndIdx].fd, &(iovv[0]), out_iov_idx );

		if (sts == -1) {
			if (errno == EAGAIN /* same as EWOULDBLOCK */) {
				TRACE( 2, "sendFragment EWOULDBLOCK to rank=%lu", sndIdx+dest_start_idx_ );
				fcntl( conninfo_[sndIdx].fd, F_SETFL, 0 ); // clear O_NONBLOCK
				conninfo_[sndIdx].blocking=true;
				// NOTE: YES -- could drop here
				goto do_again;
			}
			conninfo_[sndIdx].connect_state=0; // any write error closes
			close( conninfo_[sndIdx].fd );
			conninfo_[sndIdx].fd = -1;
			break;  // 
		}
		else if (sts != this_write_bytes) {
			// we'll loop around -- with
			TRACE( 4, "sendFragment writev sts(%ld)!=requested_send_bytes(%ld)"
			      ,sts,this_write_bytes );
			total_written_bytes+=sts;  // add sts to total_written_bytes now as sts is adjusted next
			// find which iovs are done
			for (ii=0; (size_t)sts >= iovv[ii].iov_len; ++ii)
				sts -= iovv[ii].iov_len;
			in_iov_idx += ii;		 // done with these in_iovs
			iovv[ii].iov_len -= sts;  // adjust partial iov
		    iovv[ii].iov_base = (uint8_t*)(iovv[ii].iov_base)+sts; // adjust partial iov

			// add more to get up to per_write_max_bytes
			out_iov_idx=0;
			if (ii != 0)
				iovv[out_iov_idx] = iovv[ii];
			// starting over
			this_write_bytes = iovv[out_iov_idx].iov_len;
			// add any left over from appropriate in_iov_idx --
			// i.e. match this out_iov with the in_iov that was used to
			// initialize it; see how close the out base+len is to in base+len
			// check !>per_write_max_bytes
			unsigned long additional=((unsigned long)iov_in[in_iov_idx].iov_base+iov_in[in_iov_idx].iov_len)
				- ((unsigned long)iovv[out_iov_idx].iov_base+iovv[out_iov_idx].iov_len);
			if (additional) {
				iovv[out_iov_idx].iov_len += additional;
				this_write_bytes          += additional;
				if (this_write_bytes > per_write_max_bytes) {
					iovv[out_iov_idx].iov_len -= this_write_bytes-per_write_max_bytes;
					this_write_bytes  = per_write_max_bytes;
				}
			}
			++out_iov_idx; // done with
			TRACE( 4, "sendFragment writev sts!=: this_write_bytes=%zd out_iov_idx=%zu additional=%lu ii=%d"
			      , this_write_bytes, out_iov_idx, additional, ii );
		}
		else {
			TRACE( 4, "sendFragment writev sts(%ld)==requested_send_bytes(%ld)"
			      ,sts,this_write_bytes );
			total_written_bytes+=sts;
			--out_iov_idx;  // make it the index of the last iovv
			iovv[out_iov_idx].iov_base = (uint8_t*)(iovv[out_iov_idx].iov_base)+iovv[out_iov_idx].iov_len;
			iovv[out_iov_idx].iov_len = 0;
			in_iov_idx += out_iov_idx; // at least this many complete (one more if "last iovv" is complete
			this_write_bytes = 0;
			// need to check last iovv against appropriate iov_in
			unsigned long additional=((unsigned long)iov_in[in_iov_idx].iov_base+iov_in[in_iov_idx].iov_len)
				- ((unsigned long)iovv[out_iov_idx].iov_base+iovv[out_iov_idx].iov_len);
			if (additional) {
				iovv[out_iov_idx].iov_len += additional;
				this_write_bytes          += additional;
				if (this_write_bytes > per_write_max_bytes) {
					iovv[out_iov_idx].iov_len -= this_write_bytes-per_write_max_bytes;
					this_write_bytes  = per_write_max_bytes;
				}
				if (out_iov_idx != 0)
					iovv[0] = iovv[out_iov_idx];
				out_iov_idx = 1;
			}
			else {
				++in_iov_idx;
				out_iov_idx = 0;
			}
		}
		
	} while (total_written_bytes < total_to_write_bytes);
	if (total_written_bytes > total_to_write_bytes)
		TRACE( 0, "sendFragment program error: too many bytes transferred");

	if (conninfo_[sndIdx].blocking) {
		conninfo_[sndIdx].blocking=false;
		fcntl( conninfo_[sndIdx].fd, F_SETFL, 0 ); // clear O_NONBLOCK
	}
	sts = total_written_bytes-sizeof(MessHead);
	if (sts_out)
		*sts_out=sts;
	TRACE( 10, "sendFragment sndIdx=%d sts=%ld", sndIdx, sts );
	sent_frag_count_.incSlot(sndIdx+dest_start_idx_); // incSlot(dest_rank)
	return sndIdx+dest_start_idx_;
}

// How many fragments have been sent using this SHandles object?
size_t artdaq::TCPSocketTransfer::count() const
{
	return (size_t)0;
}

// How many fragments have been sent to a particular destination.
size_t artdaq::TCPSocketTransfer::slotCount(size_t rank) const
{
	TRACE( 9, "slotCount rank=%ld",rank );
	return (size_t)0;
}

// Wait for all the data transfers scheduled by calls
// to MPI_Isend to finish, then return.
void artdaq::TCPSocketTransfer::waitAll()
{
}


//=============================================

void   artdaq::TCPSocketTransfer::stats_connect_()  // thread
{
	std::cv_status sts;
    while (!stats_connect_stop_) {
		std::string desc;
		void       *tag;
		std::function<void()> function;
		uint64_t	ts_us;

		int msdly=tmo_.get_next_timeout_msdly();
		if (msdly <= 0)
			msdly = 2000;
		std::unique_lock<std::mutex> lck(stopstatscvm_);
		sts=stopstatscv_.wait_until( lck
		                            ,std::chrono::system_clock::now()
		                            +std::chrono::milliseconds(msdly) );
		TRACE( 5, "thread1 after wait_until(msdly=%d) - sts=%d", msdly, sts );
		if (sts == std::cv_status::no_timeout)
			break;			
		tmo_.get_next_expired_timeout( desc, &tag, function, &ts_us );
		while (desc != "") {
			if (function!=NULL)
					function();
			tmo_.get_next_expired_timeout( desc, &tag, function, &ts_us );
		}
	}
}

void   artdaq::TCPSocketTransfer::connect_( int sndIdx )
{
	size_t ii=sndIdx;
	conninfo_[ii].sndbuf_bytes = sndbuf_;
	conninfo_[ii].fd = TCPConnect( conninfo_[ii].dst_name.c_str()
	                              ,conninfo_[ii].dst_port
	                              ,O_NONBLOCK
	                              ,&conninfo_[ii].sndbuf_bytes );
	conninfo_[ii].connect_state = 0;
	conninfo_[ii].blocking      = 0;
	TRACE( 7, "connect_ "+conninfo_[ii].dst_name+":%d write_fds.back()=%d"
	      , conninfo_[ii].dst_port, conninfo_[ii].fd );
	if (conninfo_[ii].fd != -1) {
		// write connect msg
		MessHead mh={0,MessHead::connect_v0,htons(my_node_idx_),htonl(CONN_MAGIC)};
		ssize_t sts=write(conninfo_[ii].fd,&mh,sizeof(mh));
		if (sts==-1) {
			// a write error here is completely unexpected!
			conninfo_[ii].connect_state=0;
			close(conninfo_[ii].fd);
			conninfo_[ii].fd = -1;
		}
		else {
			// consider it all connected/established
			conninfo_[ii].connect_state = 1;
		}
	}
}

void   artdaq::TCPSocketTransfer::reconnect_()
{
	TRACE( 5, "check/reconnect" );
	for (size_t ii=0; ii<dest_count_; ++ii)
		if (conninfo_[ii].fd == -1)
			connect_(ii);
}
// This file (TCPSocketTransfer.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
 // Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
 // or COPYING file. If you do not have such a file, one can be obtained by
 // contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
 // $RCSfile: .emacs.gnu,v $
 // rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include "TCPSocketTransfer.hh"
#include "detail/TCP_listen_fd.hh"
#define TRACE_NAME "TCPSocketTransfer"
#include "trace.h"				// TRACE
#include <stdlib.h>				// atoi, strtoul
#include <string>
#include <fstream>
#include <sys/socket.h>         // socket, socklen_t
#include <sys/un.h>				// sockaddr_un
#include "artdaq-core/Data/Fragment.hh"
#include <arpa/inet.h>			// ntohl, ntohs

const size_t artdaq::TCPSocketTransfer::RECV_TIMEOUT = 0xfedcba98;


artdaq::TCPSocketTransfer::TCPSocketTransfer(  size_t buffer_count
                   , uint64_t max_payload_lwrds // 8 byte entities
                   , size_t src_count
                   , size_t src_start_idx
                   , int    rcvbuf
                   )
	:
	buffer_count_(buffer_count),
	max_payload_lwrds_(max_payload_lwrds),
	src_count_(src_count),
	src_start_idx_(src_start_idx),
	my_node_idx_(atoi(getenv("PMI_RANK"))),
	conninfo_(src_count,ConnInfo(max_payload_lwrds)),
	listen_port_(-1),
	connect_count_(0)
{
	char* envvar;
	if ((envvar=getenv("NODE_LIST"))) {
		TRACE( 3, "NODE_LIST="+std::string(envvar) );
		std::ifstream node_list_file(envvar);
		std::string line;
		for (int ii=0; ii<=my_node_idx_; ++ii)
			getline(node_list_file, line);
		listen_port_ = atoi( line.substr(line.find(":")+1).c_str() );
	}
	else
		throw std::runtime_error("ERROR: NODE_LIST env.var. not set.");
	TRACE( 5, "TCPSocketTransfer ctor my_node_idx=%d listen_port=%d src_cnt=%lu src_start_idx=%lu"
	      , my_node_idx_, listen_port_, src_count, src_start_idx );

	if (buffer_count_ < src_count_)
		throw std::runtime_error("ERROR: TCPSocketTransfer too few buffers");

	std::set<size_t> indexes;
	for (size_t ii=0; ii<src_count_; ++ii) {
		conninfo_[ii].buffer = conninfo_[ii].frag.headerBeginBytes();
		indexes.insert( ii ); // for all conIdx's to be mapped to numDataRcvs[0]
	}
	active_numDataRcvs2conIdx_[0] = indexes;

	listen_fd_ = TCP_listen_fd( listen_port_, rcvbuf );
	// wait forever (for now)
	while (connect_count_ < src_count_)
		do_connect_();

	// now put listen_fd_
	pollfd       pollfd_s;
	pollfd_s.events = POLLIN|POLLERR;
	pollfd_s.fd = listen_fd_;
	read_fds_.push_back( pollfd_s );	
}

artdaq::TCPSocketTransfer::~TCPSocketTransfer()
{
	for (size_t ii=0; ii<conninfo_.size(); ++ii) {
		if (conninfo_[ii].fd != -1) {
			close( conninfo_[ii].fd );
		}
	}
}

static uint64_t gettimeofday_us()
{	struct timeval tv;
    gettimeofday( &tv, NULL );
    return (uint64_t)tv.tv_sec*1000000+tv.tv_usec;
}

void artdaq::TCPSocketTransfer::do_connect_() {
	int sts;
	pollfd       pollfd_s;
	pollfd_s.events = POLLIN|POLLERR;
	sockaddr_un  un;
	socklen_t    arglen=sizeof(un);
	int          fd;
	fd = accept(listen_fd_,(sockaddr *)&un,&arglen);

	timeval tv={2,0};   // maybe increase of some global "debugging" flag set???
	socklen_t lenlen=sizeof(tv);
	/*sts=*/setsockopt( fd, SOL_SOCKET, SO_RCVTIMEO, &tv, lenlen ); // see man 7 socket.
	MessHead mh;
	uint64_t mark_us=gettimeofday_us();
	sts = read( fd, &mh, sizeof(mh) );
	uint64_t delta_us=gettimeofday_us()-mark_us;
	TRACE( 10, "do_connect read of connect msg (after accept) took %lu microseconds", delta_us ); // imperically, read take a couple hundred usecs.
	if (sts != sizeof(mh)) {
		TRACE( 0, "do_connect_ problem with connect msg sts(%d)!=sizeof(mh)(%ld)"
		      , sts, sizeof(mh) );
		close(fd);
		return;
	}

	// check for "magic" and valid source_id(aka rank)
	mh.source_id = ntohs(mh.source_id); // convert here as it is reference several times
	if (   ntohl(mh.conn_magic) != CONN_MAGIC
	    || !(  mh.source_id>=src_start_idx_
		     &&mh.source_id<(src_start_idx_+src_count_))) {
		close(fd);
		return;
	}
	size_t conIdx=mh.source_id-src_start_idx_;  // validity checked above
	if (conninfo_[conIdx].fd != -1) {
		// close previous  dec connect_count_
		close( conninfo_[conIdx].fd );
		std::vector<pollfd>::iterator ii=read_fds_.begin();
		while (ii->fd != conninfo_[conIdx].fd)
			++ii;
		read_fds_.erase(ii);
		--connect_count_;
	}
	++connect_count_;
	// now add (new) connection
	pollfd_s.fd = fd;
	read_fds_.push_back( pollfd_s );
	conninfo_[conIdx].fd = fd;
	conninfo_[conIdx].sender_rank = mh.source_id;
	fd2conIdx_[fd] = conIdx;
	rank2conIdx_[mh.source_id] = conIdx;
	conninfo_[conIdx].connection_count++;
	TRACE( 3, "do_connect_ connection from sender_rank=%u state=0x%x connect_count=%ld/%ld"
	      , mh.source_id,conninfo_[conIdx].state,connect_count_,src_count_ );
}  // do_connect_

std::vector<pollfd>::iterator
artdaq::TCPSocketTransfer::close_erase_( std::vector<pollfd>::iterator ii, size_t conIdx )
{
	close( conninfo_[conIdx].fd );
	conninfo_[conIdx].fd = -1;
	conninfo_[conIdx].state = ConnInfo::metadata;
	conninfo_[conIdx].offset=0;
	--connect_count_;
	return read_fds_.erase(ii);
}
// recvFragment() puts the next received fragment in frag, with the
// source of that fragment as its return value.
//
// It is a precondition that a sources_sending() != 0.
ssize_t artdaq::TCPSocketTransfer::recvFragment( Fragment &outfrag, ssize_t timeout_usec, ssize_t *sts_out )
{
	ssize_t ret_rank=-1;
	if (read_fds_.size() == 0)  // what if just listen_fd???
		return ret_rank;;
	TRACE( 7, "TCPSocketTransfer::recvFragment timeout_usec=%ld", timeout_usec );
	//void* buff=alloca(max_payload_lwrds_*8);
	uint8_t* buff;
	size_t   byte_cnt=0;
	int      sts;
	uint64_t start_time_us=gettimeofday_us();
	int timeout_ms;
	if (timeout_usec < 0)
		timeout_ms = -1;
	else if (timeout_usec == 0)
		timeout_ms=0;
	else
		timeout_ms=(timeout_usec+999)/1000; // want at least 1 ms
	bool done=false;
    while (!done) {
		int num_fds_ready=poll( &read_fds_[0], read_fds_.size(), timeout_ms );
		if (num_fds_ready<=0) {
			if (num_fds_ready==0 && timeout_ms>0)
				return RECV_TIMEOUT;
			break;
		}
			
		std::vector<pollfd>::iterator ii=read_fds_.begin();
		do {
			if (!(ii->revents&(POLLIN|POLLERR))) {
				++ii; continue;
			}
			if (ii->fd == listen_fd_) {
				do_connect_();
				--num_fds_ready;
				++ii; continue;
			}

			size_t conIdx=fd2conIdx_[ii->fd];
			if (conninfo_[conIdx].state == ConnInfo::metadata) {
				buff     = &(conninfo_[conIdx].mha[conninfo_[conIdx].offset]);
				byte_cnt = sizeof(MessHead)-conninfo_[conIdx].offset;
			}
			else {
				buff     = conninfo_[conIdx].buffer + conninfo_[conIdx].offset;
				byte_cnt = conninfo_[conIdx].mh.byte_count-conninfo_[conIdx].offset;
			}

			sts = read(  ii->fd, buff, byte_cnt );

			TRACE( 9, "recvFragment state=%d read=%d (errno=%d) read_fds.size()=%ld"
			      , conninfo_[conIdx].state, sts, errno, read_fds_.size() );
			if (sts <= 0)
				ii = close_erase_( ii, conIdx ); // close it
			else {
				// see if we're done (with this state)
				sts = conninfo_[conIdx].offset += sts;
				if (sts == conninfo_[conIdx].target_bytes) {
					conninfo_[conIdx].offset = 0;
					if (conninfo_[conIdx].state == ConnInfo::metadata) {
						conninfo_[conIdx].state = ConnInfo::data;
						conninfo_[conIdx].mh.byte_count = ntohl(conninfo_[conIdx].mh.byte_count);
						conninfo_[conIdx].mh.source_id  = ntohs(conninfo_[conIdx].mh.source_id);
						conninfo_[conIdx].target_bytes = conninfo_[conIdx].mh.byte_count;
					}
					else {
						if (sts_out)
							*sts_out = conninfo_[conIdx].target_bytes;
						conninfo_[conIdx].state = ConnInfo::metadata;
						conninfo_[conIdx].target_bytes = sizeof(MessHead);
						conninfo_[conIdx].num_data_rcvs+=1;
						ret_rank = conninfo_[conIdx].sender_rank;
						TRACE( 9, "recvFragment done sts=%d src=%ld cnt=%d"
						      ,sts, ret_rank, conninfo_[conIdx].num_data_rcvs );
						// see if any connection should be removed from or added to read_fds_ (for next time)???
						adjust_read_fds_( ii, conIdx );

						conninfo_[conIdx].frag.autoResize();
						outfrag.swap( conninfo_[conIdx].frag );
						conninfo_[conIdx].frag.reserve( max_payload_lwrds_ );
						conninfo_[conIdx].buffer = conninfo_[conIdx].frag.headerBeginBytes();
						done=true; // no more polls
						break; // no more read of ready fds
					}
				}
				++ii;
			}
			--num_fds_ready;
		} while (ii!=read_fds_.end() && num_fds_ready>0);

		if (!done && timeout_usec > 0) {
			// calc next timeout_ms (unless timed out)
			int64_t delta_us=gettimeofday_us()-start_time_us;
			if (delta_us > timeout_usec)
				return RECV_TIMEOUT;
			timeout_ms=((timeout_usec-delta_us)+999)/1000; // want at least 1 ms
		}
	}  // while(!done)...poll
	return ret_rank;
} // recvFragment

// Number of sources still not done.
size_t artdaq::TCPSocketTransfer::sourcesActive() const
{
	return connect_count_;
}

// Are any sources still active (faster)?
bool artdaq::TCPSocketTransfer::anySourceActive() const
{
	return true;
}

// Number of sources pending (last fragments still in-flight).
size_t artdaq::TCPSocketTransfer::sourcesPending() const
{
	return (size_t)0;
}

void artdaq::TCPSocketTransfer::adjust_read_fds_( std::vector<pollfd>::iterator pollit, size_t conIdx )
{
	int prev_num_rcvs = conninfo_[conIdx].num_data_rcvs-1;
	int min_rcvs = active_numDataRcvs2conIdx_.begin()->first;
	bool new_min=false;
	std::map<int,std::set<size_t>>::iterator numRcvsIt;

	// remove -- AND NOTE, if it was THE (only) min, that there is a new min
	numRcvsIt = active_numDataRcvs2conIdx_.find( prev_num_rcvs );
	//std::set<size_t> &indexes=numRcvsIt->second;   // this doesn't seem to do what I think it should :(
	if (numRcvsIt->second.size() == 1) {
		if (prev_num_rcvs == min_rcvs) {
			TRACE( 22, "adjust_read_fds_ conIdx=%zu num_rcvs=%d new min -- erasing set for prev_num_rcvs=%d set.size=%lu better be 1"
			      , conIdx, conninfo_[conIdx].num_data_rcvs, prev_num_rcvs, numRcvsIt->second.size() );
			new_min=true;
			++min_rcvs;
		}
		else
			TRACE( 22, "adjust_read_fds_ conIdx=%zu num_rcvs=%d erasing prev_num_rcvs - old set erased min=%d"
			      , conIdx, conninfo_[conIdx].num_data_rcvs, min_rcvs );
		active_numDataRcvs2conIdx_.erase( numRcvsIt );
	}
	else {
		size_t size_before=numRcvsIt->second.size();
		size_t erased=numRcvsIt->second.erase( conIdx );
		TRACE( 22, "adjust_read_fds_ conIdx=%zu num_rcvs=%d erased prev_num_rcvs - old set.sizeb4=%zu,erased=%zu,sizeAfter=%zu min=%d"
		      , conIdx, conninfo_[conIdx].num_data_rcvs, size_before,erased,numRcvsIt->second.size(), min_rcvs );
	}

	// now, if too far ahead,
	//       add to suppressed_ AND remove from read_fds_
	//   else
	//       add back into active_
	if (conninfo_[conIdx].num_data_rcvs >= (min_rcvs+2)) {
		// suppress reads from this connection
		read_fds_.erase( pollit );
		suppressed_numDataRcvs2conIdx_[conninfo_[conIdx].num_data_rcvs].insert(conIdx);
		TRACE( 22, "adjust_read_fds_ suppressing conIdx=%zu with num_data_rcvs=%d set.size=%lu"
		      ,conIdx, conninfo_[conIdx].num_data_rcvs
		      ,suppressed_numDataRcvs2conIdx_[conninfo_[conIdx].num_data_rcvs].size());
	}
	else {
		active_numDataRcvs2conIdx_[conninfo_[conIdx].num_data_rcvs].insert(conIdx);
		TRACE( 22, "adjust_read_fds_ add to active conIdx=%zu active[%d].set.size=%lu"
		      , conIdx, conninfo_[conIdx].num_data_rcvs
		      , active_numDataRcvs2conIdx_[conninfo_[conIdx].num_data_rcvs].size() );
	}

	// if there is a new min, then the (whole) suppressed list becomes unsuppressed!!!
	// see if previously suppressed connection(s) should should be unsuppressed
	if (new_min && suppressed_numDataRcvs2conIdx_.size()) {
		pollfd pollfd_s={0,POLLIN|POLLERR,0}; // see man poll
		numRcvsIt = suppressed_numDataRcvs2conIdx_.begin();
		for (auto conIdxSetIt : numRcvsIt->second) {
			pollfd_s.fd = conninfo_[conIdxSetIt].fd;
			read_fds_.push_back( pollfd_s );
			active_numDataRcvs2conIdx_[conninfo_[conIdxSetIt].num_data_rcvs].insert(conIdxSetIt);
			TRACE( 23, "adjust_read_fds_ conIdx=%lu unsuppressing conIdx=%lu with num_data_rcvs=%d suppressed.size=%lu active.set.size=%lu"
			      , conIdx, conIdxSetIt, conninfo_[conIdxSetIt].num_data_rcvs
			      , suppressed_numDataRcvs2conIdx_.size()
			      , active_numDataRcvs2conIdx_[conninfo_[conIdxSetIt].num_data_rcvs].size() );
		}
		// clear suppressed
		suppressed_numDataRcvs2conIdx_.clear();
	}
}  // adjust_read_fds_
