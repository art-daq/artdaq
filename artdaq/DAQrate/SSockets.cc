 // This file (SSockets.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
 // Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
 // or COPYING file. If you do not have such a file, one can be obtained by
 // contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
 // $RCSfile: .emacs.gnu,v $
 // rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include "SSockets.hh"
#include "detail/TCPConnect.hh"
#include "detail/Timeout.hh"
#include "detail/SRSockets.hh"
#define TRACE_NAME "SSockets"
#include "trace.h"				// TRACE
#include <stdlib.h>				// atoi, strtoul
#include <sys/socket.h>		// getsockopt, SOL_SOCKET, SO_SNDBUF
#include <stdexcept>
#include <string>
#include <fstream>
#include <assert.h>
#include <arpa/inet.h>			// htonl, htons
#include "artdaq-core/Data/Fragment.hh"


artdaq::SSockets::
SSockets(  size_t buffer_count
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
  stats_connect_thread_(std::bind(&SSockets::stats_connect_,this))
{
	TRACE( 4, "SSockets ctor max_payload_lwrds=%lu dst_cnt=%lu dst_start=%lu"
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
		std::function<void()> function = std::bind( &SSockets::reconnect_,this );
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
		throw std::runtime_error("ERROR: SSockets::SSockets ctor dst_count too big");
}

artdaq::SSockets::~SSockets()
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
	TRACE( 4, "SSockets dtor" );
}


// Send the given Fragment. Return the rank of the destination to which
// the Fragment was sent OR -1 if to none.
ssize_t artdaq::SSockets::sendFragment( Fragment &&frag, ssize_t* sts_out )
{
	if (frag.type() == Fragment::EndOfDataFragmentType) {
		throw cet::exception("LogicError")
			<< "EOD fragments should not be sent on as received: "
			<< "use sendEODFrag() instead.";
	}
	return -1;
	size_t sndIdx=frag.sequenceID() % dest_count_;
	artdaq::Fragment grab_ownership_frag=std::move(frag);
	iovec iov = { (void*)grab_ownership_frag.headerBegin(), grab_ownership_frag.sizeBytes() };
	return sendFragment( &iov, 1, sts_out, sndIdx );
}
ssize_t artdaq::SSockets::sendFragment( const void* buf, size_t bytes, ssize_t* sts_out )
{
	iovec iov = { (void*)buf, bytes };
	return sendFragment( &iov, 1, sts_out );
}
ssize_t artdaq::SSockets::sendFragment( const struct iovec *iov, int iovcnt, ssize_t* sts_out, int sndIdx )
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
size_t artdaq::SSockets::count() const
{
	return (size_t)0;
}

// How many fragments have been sent to a particular destination.
size_t artdaq::SSockets::slotCount(size_t rank) const
{
	TRACE( 9, "slotCount rank=%ld",rank );
	return (size_t)0;
}

// Wait for all the data transfers scheduled by calls
// to MPI_Isend to finish, then return.
void artdaq::SSockets::waitAll()
{
}


//=============================================

void   artdaq::SSockets::stats_connect_()  // thread
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

void   artdaq::SSockets::connect_( int sndIdx )
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

void   artdaq::SSockets::reconnect_()
{
	TRACE( 5, "check/reconnect" );
	for (size_t ii=0; ii<dest_count_; ++ii)
		if (conninfo_[ii].fd == -1)
			connect_(ii);
}
