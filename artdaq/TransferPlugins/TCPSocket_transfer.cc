// This file (TCPSocketTransfer.cc) was created by Ron Rechenmacher <ron@fnal.gov> on
// Sep 14, 2016. "TERMS AND CONDITIONS" governing this file are in the README
// or COPYING file. If you do not have such a file, one can be obtained by
// contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
// $RCSfile: .emacs.gnu,v $
// rev="$Revision: 1.30 $$Date: 2016/03/01 14:27:27 $";

// C Includes
#include <stdlib.h>				// atoi, strtoul
#include <sys/socket.h>         // socket, socklen_t
#include <sys/un.h>				// sockaddr_un
#include <arpa/inet.h>			// ntohl, ntohs
#include <assert.h>

// C++ Includes
#include <string>
#include <fstream>
#include <stdexcept>

// product Includes
#define TRACE_NAME "TCPSocketTransfer"
#include "artdaq/DAQdata/Globals.hh"

// artdaq Includes
#include "artdaq/TransferPlugins/TCPSocketTransfer.hh"
#include "artdaq/DAQdata/TCP_listen_fd.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/TransferPlugins/detail/Timeout.hh"
#include "artdaq/TransferPlugins/detail/SRSockets.hh"
#include "artdaq-core/Data/Fragment.hh"


artdaq::TCPSocketTransfer::
TCPSocketTransfer(fhicl::ParameterSet const& pset, TransferInterface::Role role)
	: TransferInterface(pset, role)
	, fd_(-1)
	, listen_fd_(-1)
	, state_(SocketState::Metadata)
	, frag(max_fragment_size_words_)
	, buffer(frag.headerBeginBytes())
	, offset(0)
	, target_bytes(sizeof(MessHead))
	, rcvbuf_(pset.get<size_t>("tcp_receive_buffer_size", 0))
	, sndbuf_(max_fragment_size_words_ * sizeof(artdaq::RawDataType) * buffer_count_)
	, stats_connect_stop_(false)
	, stats_connect_thread_(std::bind(&TCPSocketTransfer::stats_connect_, this))
	, timeoutMessageArmed_(true)
{
	TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer Constructor: pset=" << pset.to_string() << ", role=" << (role == TransferInterface::Role::kReceive ? "kReceive" : "kSend") << TLOG_ENDL;
	auto hosts = pset.get<std::vector<fhicl::ParameterSet>>("host_map");
	for (auto& ps : hosts)
	{
		auto rank = ps.get<size_t>("rank", RECV_TIMEOUT);
		DestinationInfo info;
		info.hostname = ps.get<std::string>("host", "localhost");
		info.portOffset = ps.get<int>("portOffset", 5500);

		hostMap_[rank] = info;
	}

	std::function<void()> function = std::bind(&TCPSocketTransfer::reconnect_, this);
	tmo_.add_periodic("reconnect", NULL, function, 200/*millisec*/);

	if (role == TransferInterface::Role::kReceive)
	{
		// Wait for sender to connect...
		TLOG_DEBUG(uniqueLabel()) << "Listening for connections" << TLOG_ENDL;
		listen_();
		TLOG_DEBUG(uniqueLabel()) << "Done Listening" << TLOG_ENDL;
	}
	else
	{
		TLOG_DEBUG(uniqueLabel()) << "Connecting to destination" << TLOG_ENDL;
		connect_();
		TLOG_DEBUG(uniqueLabel()) << "Done Connecting" << TLOG_ENDL;
	}
	TLOG_DEBUG(uniqueLabel()) << "End of TCPSocketTransfer Constructor" << TLOG_ENDL;
}

artdaq::TCPSocketTransfer::~TCPSocketTransfer()
{
	TLOG_DEBUG(uniqueLabel()) << "Shutting down TCPSocketTransfer" << TLOG_ENDL;
	stats_connect_stop_ = true;
	stopstatscv_.notify_all();
	stats_connect_thread_.join();

	if (role() == TransferInterface::Role::kSend)
	{
		// close all open connections (send stop_v0) first
		MessHead mh = {0,MessHead::stop_v0,htons(source_rank()),0};
		if (fd_ != -1)
		{
			// should be blocking with modest timeo
			timeval tv = {0,100000};
			socklen_t len = sizeof(tv);
			setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, len);
			write(fd_, &mh, sizeof(mh));
		}
	}
	close(fd_);
	TLOG_DEBUG(uniqueLabel()) << "End of TCPSocketTransfer Destructor" << TLOG_ENDL;
	TRACE(4, "TCPSocketTransfer dtor");
}


// Send the given Fragment. Return the rank of the destination to which
// the Fragment was sent OR -1 if to none.
artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendFragment_(Fragment&& frag, size_t send_timeout_usec)
{
	TRACE(7, "TCPSocketTransfer::sendFragment begin");
	artdaq::Fragment grab_ownership_frag = std::move(frag);
	iovec iov = {(void*)grab_ownership_frag.headerBeginBytes(), grab_ownership_frag.sizeBytes()};
	auto sts = sendFragment_(&iov, 1, send_timeout_usec);
	while (sts != CopyStatus::kSuccess)
	{
		TRACE(7, "TCPSocketTransfer::sendFragment: Timeout or Error sending fragment");
		sts = sendFragment_(&iov, 1, send_timeout_usec);
		usleep(1000);
	}

	std::string result = (sts == TransferInterface::CopyStatus::kSuccess ? "kSuccess" : (sts == TransferInterface::CopyStatus::kTimeout ? "kTimeout" : "kErrorNotRequiringException"));

	TRACE_(7, "TCPSocketTransfer::sendFragment returning " + result);
	return sts;
}

artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendFragment_(const void* buf, size_t bytes, size_t send_timeout_usec)
{
	TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::sendFragment_ Converting buf to iovec" << TLOG_ENDL;
	iovec iov = {(void*)buf, bytes};
	return sendFragment_(&iov, 1, send_timeout_usec);
}

artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendFragment_(const struct iovec* iov, int iovcnt, size_t send_timeout_usec)
{
	// check all connected??? -- currently just check fd!=-1
	if (fd_ == -1)
	{
		if (timeoutMessageArmed_)
		{
			TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::sendFragment_: Send fd is not open. Returning kTimeout" << TLOG_ENDL;
			timeoutMessageArmed_ = false;
		}
		return TransferInterface::CopyStatus::kTimeout;
	}
	timeoutMessageArmed_ = true;
	TRACE(12, "send_timeout_usec is %zu, currently unused.", send_timeout_usec);

	//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::sendFragment_: Determining write size" << TLOG_ENDL;
	uint32_t total_to_write_bytes = 0;
	std::vector<iovec> iov_in(iovcnt + 1); // need contiguous (for the unlike case that only partial MH
	std::vector<iovec> iovv(iovcnt + 2); // 1 more for mh and another one for any partial
	int ii;
	for (ii = 0; ii < iovcnt; ++ii)
	{
		iov_in[ii + 1] = iov[ii];
		total_to_write_bytes += iov[ii].iov_len;
	}
	//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::sendFragment_: Constructing Message Header" << TLOG_ENDL;
	MessHead mh = {0,MessHead::data_v0,htons(source_rank()),htonl(total_to_write_bytes)};
	iov_in[0].iov_base = &mh;
	iov_in[0].iov_len = sizeof(mh);
	total_to_write_bytes += sizeof(mh);

	ssize_t sts = 0;
	ssize_t total_written_bytes = 0;
	ssize_t per_write_max_bytes = (32 * 1024);

	size_t in_iov_idx = 0; // only increment this when we know the associated data has been xferred
	size_t out_iov_idx = 0;
	ssize_t this_write_bytes = 0;

	do
	{
		// The first out_iov may be set at the end of the previous loop.
		// iov looping from below (b/c of the latter, we need to check this_write_bytes)
		for (;
			(in_iov_idx + out_iov_idx) < iov_in.size() && this_write_bytes < per_write_max_bytes;
			++out_iov_idx)
		{
			this_write_bytes += iov_in[in_iov_idx + out_iov_idx].iov_len;
			iovv[out_iov_idx] = iov_in[in_iov_idx + out_iov_idx];
		}
		if (this_write_bytes > per_write_max_bytes)
		{
			iovv[out_iov_idx - 1].iov_len -= this_write_bytes - per_write_max_bytes;
			this_write_bytes = per_write_max_bytes;
		}

		// need to do blocking algorithm -- including throttled block notifications
	do_again:
		TRACE(7, "sendFragment b4 writev %7zu total_written_bytes fd=%d in_idx=%zu iovcnt=%zu 1st.len=%zu"
			, total_written_bytes, fd_, in_iov_idx, out_iov_idx, iovv[0].iov_len);
		//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer calling writev" << TLOG_ENDL;
		sts = writev(fd_, &(iovv[0]), out_iov_idx);
		//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer done with writev" << TLOG_ENDL;

		if (sts == -1)
		{
			if (errno == EAGAIN /* same as EWOULDBLOCK */)
			{
				TRACE(2, "sendFragment EWOULDBLOCK");
				fcntl(fd_, F_SETFL, 0); // clear O_NONBLOCK
				blocking = true;
				// NOTE: YES -- could drop here
				goto do_again;
			}
			TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::sendFragment_: WRITE ERROR!" << TLOG_ENDL;
			connect_state = 0; // any write error closes
			close(fd_);
			fd_ = -1;
			return TransferInterface::CopyStatus::kErrorNotRequiringException;
		}
		else if (sts != this_write_bytes)
		{
			// we'll loop around -- with
			TRACE(4, "sendFragment writev sts(%ld)!=requested_send_bytes(%ld)"
				, sts, this_write_bytes);
			total_written_bytes += sts; // add sts to total_written_bytes now as sts is adjusted next
			// find which iovs are done
			for (ii = 0; (size_t)sts >= iovv[ii].iov_len; ++ii)
				sts -= iovv[ii].iov_len;
			in_iov_idx += ii; // done with these in_iovs
			iovv[ii].iov_len -= sts; // adjust partial iov
			iovv[ii].iov_base = (uint8_t*)(iovv[ii].iov_base) + sts; // adjust partial iov

			// add more to get up to per_write_max_bytes
			out_iov_idx = 0;
			if (ii != 0)
				iovv[out_iov_idx] = iovv[ii];
			// starting over
			this_write_bytes = iovv[out_iov_idx].iov_len;
			// add any left over from appropriate in_iov_idx --
			// i.e. match this out_iov with the in_iov that was used to
			// initialize it; see how close the out base+len is to in base+len
			// check !>per_write_max_bytes
			unsigned long additional = ((unsigned long)iov_in[in_iov_idx].iov_base + iov_in[in_iov_idx].iov_len)
			                           - ((unsigned long)iovv[out_iov_idx].iov_base + iovv[out_iov_idx].iov_len);
			if (additional)
			{
				iovv[out_iov_idx].iov_len += additional;
				this_write_bytes += additional;
				if (this_write_bytes > per_write_max_bytes)
				{
					iovv[out_iov_idx].iov_len -= this_write_bytes - per_write_max_bytes;
					this_write_bytes = per_write_max_bytes;
				}
			}
			++out_iov_idx; // done with
			TRACE(4, "sendFragment writev sts!=: this_write_bytes=%zd out_iov_idx=%zu additional=%lu ii=%d"
				, this_write_bytes, out_iov_idx, additional, ii);
		}
		else
		{
			TRACE(4, "sendFragment writev sts(%ld)==requested_send_bytes(%ld)"
				, sts, this_write_bytes);
			total_written_bytes += sts;
			--out_iov_idx; // make it the index of the last iovv
			iovv[out_iov_idx].iov_base = (uint8_t*)(iovv[out_iov_idx].iov_base) + iovv[out_iov_idx].iov_len;
			iovv[out_iov_idx].iov_len = 0;
			in_iov_idx += out_iov_idx; // at least this many complete (one more if "last iovv" is complete
			this_write_bytes = 0;
			// need to check last iovv against appropriate iov_in
			unsigned long additional = ((unsigned long)iov_in[in_iov_idx].iov_base + iov_in[in_iov_idx].iov_len)
			                           - ((unsigned long)iovv[out_iov_idx].iov_base + iovv[out_iov_idx].iov_len);
			if (additional)
			{
				iovv[out_iov_idx].iov_len += additional;
				this_write_bytes += additional;
				if (this_write_bytes > per_write_max_bytes)
				{
					iovv[out_iov_idx].iov_len -= this_write_bytes - per_write_max_bytes;
					this_write_bytes = per_write_max_bytes;
				}
				if (out_iov_idx != 0)
					iovv[0] = iovv[out_iov_idx];
				out_iov_idx = 1;
			}
			else
			{
				++in_iov_idx;
				out_iov_idx = 0;
			}
		}
	}
	while (total_written_bytes < total_to_write_bytes);
	if (total_written_bytes > total_to_write_bytes)
	TRACE(0, "sendFragment program error: too many bytes transferred");

	if (blocking)
	{
		blocking = false;
		fcntl(fd_, F_SETFL, 0); // clear O_NONBLOCK
	}
	sts = total_written_bytes - sizeof(MessHead);

	TRACE(10, "sendFragment sts=%ld", sts);
	return TransferInterface::CopyStatus::kSuccess;
}

//=============================================

void artdaq::TCPSocketTransfer::connect_()
{
	TLOG_DEBUG(uniqueLabel()) << "Connecting sender socket" << TLOG_ENDL;
	int sndbuf_bytes = static_cast<int>(sndbuf_);
	fd_ = TCPConnect(hostMap_[destination_rank()].hostname.c_str()
	                 , calculate_port_()
	                 , O_NONBLOCK
	                 , sndbuf_bytes);
	connect_state = 0;
	blocking = 0;
	TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::connect_ " + hostMap_[destination_rank()].hostname + ":" << calculate_port_() << " fd_=" << fd_ << TLOG_ENDL;
	if (fd_ != -1)
	{
		// write connect msg
		TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::connect_: Writing connect message" << TLOG_ENDL;
		MessHead mh = {0,MessHead::connect_v0,htons(source_rank()),htonl(CONN_MAGIC)};
		ssize_t sts = write(fd_, &mh, sizeof(mh));
		if (sts == -1)
		{
			TLOG_ERROR(uniqueLabel()) << "TCPSocketTransfer::connect_: Error writing connect message!" << TLOG_ENDL;
			// a write error here is completely unexpected!
			connect_state = 0;
			close(fd_);
			fd_ = -1;
		}
		else
		{
			TLOG_INFO(uniqueLabel()) << "TCPSocketTransfer::connect_: Successfully connected" << TLOG_ENDL;
			// consider it all connected/established
			connect_state = 1;
		}
	}
}

void artdaq::TCPSocketTransfer::reconnect_()
{
	TRACE(5, "check/reconnect");
	if (fd_ == -1 && role() == TransferInterface::Role::kSend) return connect_();
	if ((fd_ == -1 || listen_fd_ == -1) && role() == TransferInterface::Role::kReceive) return listen_();
}


static uint64_t gettimeofday_us()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

void artdaq::TCPSocketTransfer::listen_()
{
	TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Listening/accepting new connections" << TLOG_ENDL;
	if (listen_fd_ == -1)
	{
		TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Opening listener" << TLOG_ENDL;
		listen_fd_ = TCP_listen_fd(calculate_port_(), rcvbuf_);
	}
	if (listen_fd_ == -1)
	{
		TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Error creating listen_fd_!" << TLOG_ENDL;
		return;
	}

	int res;
	timeval tv = {2,0}; // maybe increase of some global "debugging" flag set???
	fd_set rfds;
	FD_ZERO(&rfds);
	FD_SET(listen_fd_, &rfds);

	res = select(listen_fd_ + 1, &rfds, (fd_set *)0, (fd_set *)0, &tv);
	if (res > 0)
	{
		int sts;
		sockaddr_un un;
		socklen_t arglen = sizeof(un);
		int fd;
		TLOG_DEBUG(uniqueLabel()) << "Calling accept" << TLOG_ENDL;
		fd = accept(listen_fd_, (sockaddr *)&un, &arglen);
		TLOG_DEBUG(uniqueLabel()) << "Done with accept" << TLOG_ENDL;

		TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Reading connect message" << TLOG_ENDL;
		socklen_t lenlen = sizeof(tv);
		/*sts=*/
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, lenlen); // see man 7 socket.
		MessHead mh;
		uint64_t mark_us = gettimeofday_us();
		sts = read(fd, &mh, sizeof(mh));
		uint64_t delta_us = gettimeofday_us() - mark_us;
		TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Read of connect message took " << delta_us << " microseconds." << TLOG_ENDL;
		TRACE(10, "do_connect read of connect msg (after accept) took %lu microseconds", delta_us); // emperically, read take a couple hundred usecs.
		if (sts != sizeof(mh))
		{
			TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Wrong message header length received!" << TLOG_ENDL;
			TRACE(0, "do_connect_ problem with connect msg sts(%d)!=sizeof(mh)(%ld)", sts, sizeof(mh));
			close(fd);
			return;
		}

		// check for "magic" and valid source_id(aka rank)
		mh.source_id = ntohs(mh.source_id); // convert here as it is reference several times
		if (ntohl(mh.conn_magic) != CONN_MAGIC || mh.source_id != source_rank())
		{
			TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::listen_: Wrong magic bytes in header!" << TLOG_ENDL;
			close(fd);
			return;
		}
		if (fd_ != -1)
		{
			// close previous  dec connect_count_
			close(fd_);
		}

		// now add (new) connection
		fd_ = fd;
		TLOG_INFO(uniqueLabel()) << "TCPSocketTransfer::listen_: New fd is " << fd_ << TLOG_ENDL;

		TRACE(3, "do_connect_ connection from sender_rank=%zu", mh.source_id);
	}
	else
	{
		TRACE(10, "TCPSocketTransfer::do_connect_: No connections in timeout interval!");
	}
} // do_connect_

// recvFragment() puts the next received fragment in frag, with the
// source of that fragment as its return value.
//
// It is a precondition that a sources_sending() != 0.
int artdaq::TCPSocketTransfer::receiveFragment(Fragment& outfrag, size_t timeout_usec)
{
	TRACE(7, "TCPSocketTransfer::receiveFragment: BEGIN");
	int ret_rank = RECV_TIMEOUT;
	if (fd_ == -1)
	{ // what if just listen_fd??? 
		TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Receive socket not connected, returning RECV_TIMEOUT" << TLOG_ENDL;
		return RECV_TIMEOUT;
	}

	TRACE(7, "TCPSocketTransfer::recvFragment timeout_usec=%ld", timeout_usec);
	//void* buff=alloca(max_fragment_size_words_*8);
	uint8_t* buff;
	size_t byte_cnt = 0;
	int sts;
	uint64_t start_time_us = gettimeofday_us();

	pollfd pollfd_s;
	pollfd_s.events = POLLIN | POLLERR;
	pollfd_s.fd = fd_;

	int timeout_ms;
	if (timeout_usec == 0)
		timeout_ms = 0;
	else
		timeout_ms = (timeout_usec + 999) / 1000; // want at least 1 ms

	bool done = false;
	while (!done)
	{
		//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Polling fd to see if there's data" << TLOG_ENDL;
		int num_fds_ready = poll(&pollfd_s, 1, timeout_ms);
		if (num_fds_ready <= 0)
		{
			if (num_fds_ready == 0 && timeout_ms > 0)
			{
				TRACE(7, "TCPSocketTransfer::receiveFragment: No data on receive socket, returning RECV_TIMEOUT");
				return RECV_TIMEOUT;
			}
			break;
		}

		if (!(pollfd_s.revents & (POLLIN | POLLERR)))
		{
			TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Wrong event received from pollfd" << TLOG_ENDL;
			continue;
		}

		if (state_ == SocketState::Metadata)
		{
			//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Reading Message Header" << TLOG_ENDL;
			buff = &(mha[offset]);
			byte_cnt = sizeof(MessHead) - offset;
		}
		else
		{
			//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Reading data" << TLOG_ENDL;
			buff = buffer + offset;
			byte_cnt = mh.byte_count - offset;
		}

		//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Reading " << byte_cnt << " bytes from socket" << TLOG_ENDL;
		sts = read(fd_, buff, byte_cnt);
		//TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Done with read" << TLOG_ENDL;

		TRACE(9, "recvFragment state=%d read=%d (errno=%d)", static_cast<int>(state_), sts, errno);
		if (sts <= 0)
		{
			TLOG_DEBUG(uniqueLabel()) << "TCPSocketTransfer::receiveFragment: Error on receive, closing socket" << TLOG_ENDL;
			close(fd_);
		}
		else
		{
			// see if we're done (with this state)
			sts = offset += sts;
			if (sts == target_bytes)
			{
				TRACE(7, "TCPSocketTransfer::receiveFragment: Target read bytes reached. Changing state");
				offset = 0;
				if (state_ == SocketState::Metadata)
				{
					state_ = SocketState::Data;
					mh.byte_count = ntohl(mh.byte_count);
					mh.source_id = ntohs(mh.source_id);
					target_bytes = mh.byte_count;
				}
				else
				{
					state_ = SocketState::Metadata;
					target_bytes = sizeof(MessHead);
					ret_rank = source_rank();
					TRACE(9, "recvFragment done sts=%d src=%d", sts, ret_rank);
					TRACE(7, "TCPSocketTransfer::receiveFragment: Done receiving fragment. Moving into output.");
					frag.autoResize();
					if (frag.type() == artdaq::Fragment::EndOfDataFragmentType)
					{
						stats_connect_stop_ = true; // Don't reconnect if we're done receiving data...
						stopstatscv_.notify_all();
					}
					outfrag.swap(frag);
					frag.reserve(max_fragment_size_words_);
					buffer = frag.headerBeginBytes();
					done = true; // no more polls
					break; // no more read of ready fds
				}
			}
		}

		if (!done && timeout_usec > 0)
		{
			// calc next timeout_ms (unless timed out)
			size_t delta_us = gettimeofday_us() - start_time_us;
			if (delta_us > timeout_usec)
				return RECV_TIMEOUT;
			timeout_ms = ((timeout_usec - delta_us) + 999) / 1000; // want at least 1 ms
		}
	} // while(!done)...poll

	TRACE(7, "TCPSocketTransfer::receiveFragment: Returning %d", ret_rank);
	return ret_rank;
} // recvFragment

void artdaq::TCPSocketTransfer::stats_connect_() // thread
{
	std::cv_status sts;
	while (!stats_connect_stop_)
	{
		std::string desc;
		void* tag;
		std::function<void()> function;
		uint64_t ts_us;

		int msdly = tmo_.get_next_timeout_msdly();

		if (msdly <= 0)
			msdly = 2000;

		std::unique_lock<std::mutex> lck(stopstatscvm_);
		sts = stopstatscv_.wait_until(lck
		                              , std::chrono::system_clock::now()
		                                + std::chrono::milliseconds(msdly));
		TRACE(5, "thread1 after wait_until(msdly=%d) - sts=%d", msdly, static_cast<int>(sts));

		if (sts == std::cv_status::no_timeout)
			break;

		auto sts = tmo_.get_next_expired_timeout(desc, &tag, function, &ts_us);

		while (sts != -1 && desc != "")
		{
			if (function != NULL)
				function();

			sts = tmo_.get_next_expired_timeout(desc, &tag, function, &ts_us);
		}
	}
}

DEFINE_ARTDAQ_TRANSFER(artdaq::TCPSocketTransfer)
