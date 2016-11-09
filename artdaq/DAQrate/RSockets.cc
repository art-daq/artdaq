// This file (RSockets.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
 // Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
 // or COPYING file. If you do not have such a file, one can be obtained by
 // contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
 // $RCSfile: .emacs.gnu,v $
 // rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

#include "RSockets.hh"
#include "detail/TCP_listen_fd.hh"
#define TRACE_NAME "RSockets"
#include "trace.h"				// TRACE
#include <stdlib.h>				// atoi, strtoul
#include <string>
#include <fstream>
#include <sys/socket.h>         // socket, socklen_t
#include <sys/un.h>				// sockaddr_un
#include "artdaq-core/Data/Fragment.hh"
#include <arpa/inet.h>			// ntohl, ntohs

const size_t artdaq::RSockets::RECV_TIMEOUT = 0xfedcba98;


artdaq::RSockets::RSockets(  size_t buffer_count
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
	TRACE( 5, "RSockets ctor my_node_idx=%d listen_port=%d src_cnt=%lu src_start_idx=%lu"
	      , my_node_idx_, listen_port_, src_count, src_start_idx );

	if (buffer_count_ < src_count_)
		throw std::runtime_error("ERROR: RSockets too few buffers");

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

artdaq::RSockets::~RSockets()
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

void artdaq::RSockets::do_connect_() {
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
artdaq::RSockets::close_erase_( std::vector<pollfd>::iterator ii, size_t conIdx )
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
ssize_t artdaq::RSockets::recvFragment( Fragment &outfrag, ssize_t timeout_usec, ssize_t *sts_out )
{
	ssize_t ret_rank=-1;
	if (read_fds_.size() == 0)  // what if just listen_fd???
		return ret_rank;;
	TRACE( 7, "RSockets::recvFragment timeout_usec=%ld", timeout_usec );
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
size_t artdaq::RSockets::sourcesActive() const
{
	return connect_count_;
}

// Are any sources still active (faster)?
bool artdaq::RSockets::anySourceActive() const
{
	return true;
}

// Number of sources pending (last fragments still in-flight).
size_t artdaq::RSockets::sourcesPending() const
{
	return (size_t)0;
}

void artdaq::RSockets::adjust_read_fds_( std::vector<pollfd>::iterator pollit, size_t conIdx )
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
