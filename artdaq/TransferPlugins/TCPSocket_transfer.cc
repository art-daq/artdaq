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
#include <sys/types.h>			// size_t
#include <poll.h>				// struct pollfd

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
#include "artdaq-core/Utilities/TimeUtils.hh"
#include <iomanip>

artdaq::TCPSocketTransfer::
TCPSocketTransfer(fhicl::ParameterSet const& pset, TransferInterface::Role role)
	: TransferInterface(pset, role)
	, fd_(-1)
	, listen_fd_(-1)
	, state_(SocketState::Metadata)
	, offset(0)
	, target_bytes(sizeof(MessHead))
	, rcvbuf_(pset.get<size_t>("tcp_receive_buffer_size", 0))
	, sndbuf_(max_fragment_size_words_ * sizeof(artdaq::RawDataType) * buffer_count_)
	, send_retry_timeout_us_(pset.get<size_t>("send_retry_timeout_us", 1000000))
	, stats_connect_stop_(false)
	, stats_connect_thread_(std::bind(&TCPSocketTransfer::stats_connect_, this))
	, timeoutMessageArmed_(true)
{
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer Constructor: pset=" << pset.to_string() << ", role=" << (role == TransferInterface::Role::kReceive ? "kReceive" : "kSend") << TLOG_ENDL;
	auto masterPortOffset = pset.get<int>("offset_all_ports", 0);
	hostMap_ = MakeHostMap(pset, masterPortOffset);

	std::function<void()> function = std::bind(&TCPSocketTransfer::reconnect_, this);
	tmo_.add_periodic("reconnect", NULL, function, 200/*millisec*/);

	if (role == TransferInterface::Role::kReceive)
	{
		// Wait for sender to connect...
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " Listening for connections" << TLOG_ENDL;
		listen_();
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " Done Listening" << TLOG_ENDL;
	}
	else
	{
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " Connecting to destination" << TLOG_ENDL;
		connect_();
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " Done Connecting" << TLOG_ENDL;
	}
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " End of TCPSocketTransfer Constructor" << TLOG_ENDL;
}

artdaq::TCPSocketTransfer::~TCPSocketTransfer()
{
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " Shutting down TCPSocketTransfer" << TLOG_ENDL;
	stats_connect_stop_ = true;
	stopstatscv_.notify_all();
	stats_connect_thread_.join();

	if (role() == TransferInterface::Role::kSend)
	{
		// close all open connections (send stop_v0) first
		MessHead mh = { 0,MessHead::stop_v0,htons(TransferInterface::source_rank()),{0} };
		if (fd_ != -1)
		{
			// should be blocking with modest timeo
			timeval tv = { 0,100000 };
			socklen_t len = sizeof(tv);
			setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, len);
			write(fd_, &mh, sizeof(mh));
		}
	}
	close(fd_);
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " End of TCPSocketTransfer Destructor" << TLOG_ENDL;
}

int artdaq::TCPSocketTransfer::receiveFragmentHeader(detail::RawFragmentHeader& header, size_t timeout_usec)
{
	TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: BEGIN" << TLOG_ENDL;
	int ret_rank = RECV_TIMEOUT;
	if (fd_ == -1)
	{ // what if just listen_fd??? 
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Receive socket not connected, returning RECV_TIMEOUT" << TLOG_ENDL;
		return RECV_TIMEOUT;
	}

	TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::recvFragment timeout_usec=" << std::to_string(timeout_usec) << TLOG_ENDL;
	//void* buff=alloca(max_fragment_size_words_*8);
	size_t byte_cnt = 0;
	int sts;
	uint64_t start_time_us = TimeUtils::gettimeofday_us();

	pollfd pollfd_s;
	pollfd_s.events = POLLIN | POLLERR;
	pollfd_s.fd = fd_;
	uint8_t* buff;

	int timeout_ms;
	if (timeout_usec == 0)
		timeout_ms = 0;
	else
		timeout_ms = (timeout_usec + 999) / 1000; // want at least 1 ms

	bool done = false;
	while (!done)
	{
		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Polling fd to see if there's data" << TLOG_ENDL;
		int num_fds_ready = poll(&pollfd_s, 1, timeout_ms);
		if (num_fds_ready <= 0)
		{
			if (num_fds_ready == 0 && timeout_ms > 0)
			{
				TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: No data on receive socket, returning RECV_TIMEOUT" << TLOG_ENDL;
				return RECV_TIMEOUT;
			}
			break;
		}

		if (!(pollfd_s.revents & (POLLIN | POLLHUP | POLLERR)))
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Wrong event received from pollfd: " << static_cast<int>(pollfd_s.revents) << TLOG_ENDL;
			continue;
		}

		if (state_ == SocketState::Metadata)
		{
			//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Reading Message Header" << TLOG_ENDL;
			buff = &(mha[offset]);
			byte_cnt = sizeof(MessHead) - offset;
		}
		else
		{
			//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Reading data" << TLOG_ENDL;
			buff = reinterpret_cast<uint8_t*>(&header) + offset;
			byte_cnt = mh.byte_count - offset;
		}

		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Reading " << byte_cnt << " bytes from socket" << TLOG_ENDL;
		sts = read(fd_, buff, byte_cnt);
		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Done with read" << TLOG_ENDL;

		TLOG_ARB(9, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::recvFragment state=" << static_cast<int>(state_) << " read=" << sts << " (errno=" << strerror(errno) << ")" << TLOG_ENDL;
		if (sts <= 0)
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Error on receive, closing socket" << TLOG_ENDL;
			close(fd_);
		}
		else
		{
			// see if we're done (with this state)
			sts = offset += sts;
			if (sts == target_bytes)
			{
				TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Target read bytes reached. Changing state" << TLOG_ENDL;
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
					TLOG_ARB(9, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::recvFragment done sts=" << sts << " src=" << ret_rank << TLOG_ENDL;
					TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Done receiving fragment. Moving into output." << TLOG_ENDL;

					done = true; // no more polls
					break; // no more read of ready fds
				}
			}
		}

		if (!done && timeout_usec > 0)
		{
			// calc next timeout_ms (unless timed out)
			size_t delta_us = TimeUtils::gettimeofday_us() - start_time_us;
			if (delta_us > timeout_usec)
				return RECV_TIMEOUT;
			timeout_ms = ((timeout_usec - delta_us) + 999) / 1000; // want at least 1 ms
		}
	} // while(!done)...poll

	TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Returning " << ret_rank << TLOG_ENDL;
	return ret_rank;
}

int artdaq::TCPSocketTransfer::receiveFragmentData(RawDataType* destination, size_t)
{
	TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: BEGIN" << TLOG_ENDL;
	int ret_rank = RECV_TIMEOUT;
	if (fd_ == -1)
	{ // what if just listen_fd??? 
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Receive socket not connected, returning RECV_TIMEOUT" << TLOG_ENDL;
		return RECV_TIMEOUT;
	}

	//void* buff=alloca(max_fragment_size_words_*8);
	uint8_t* buff;
	size_t byte_cnt = 0;
	int sts;

	pollfd pollfd_s;
	pollfd_s.events = POLLIN | POLLERR;
	pollfd_s.fd = fd_;

	bool done = false;
	while (!done)
	{
		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Polling fd to see if there's data" << TLOG_ENDL;
		int num_fds_ready = poll(&pollfd_s, 1, -1);
		if (num_fds_ready <= 0)
		{
			if (num_fds_ready == 0)
			{
				TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: No data on receive socket, returning RECV_TIMEOUT" << TLOG_ENDL;
				return RECV_TIMEOUT;
			}
			break;
		}

		if (!(pollfd_s.revents & (POLLIN | POLLERR)))
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Wrong event received from pollfd" << TLOG_ENDL;
			continue;
		}

		if (state_ == SocketState::Metadata)
		{
			//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Reading Message Header" << TLOG_ENDL;
			buff = &(mha[offset]);
			byte_cnt = sizeof(MessHead) - offset;
		}
		else
		{
			//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Reading data" << TLOG_ENDL;
			buff = reinterpret_cast<uint8_t*>(destination) + offset;
			byte_cnt = mh.byte_count - offset;
		}

		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Reading " << byte_cnt << " bytes from socket" << TLOG_ENDL;
		sts = read(fd_, buff, byte_cnt);
		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Done with read" << TLOG_ENDL;

		TLOG_ARB(9, "TCPSocketTransfer") << uniqueLabel() << " recvFragment state=" << static_cast<int>(state_) << " read=" << sts << " (errno=" << strerror(errno) << ")" << TLOG_ENDL;
		if (sts <= 0)
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Error on receive, closing socket" << TLOG_ENDL;
			close(fd_);
		}
		else
		{
			// see if we're done (with this state)
			sts = offset += sts;
			if (sts == target_bytes)
			{
				TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Target read bytes reached. Changing state" << TLOG_ENDL;
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
					TLOG_ARB(9, "TCPSocketTransfer") << uniqueLabel() << " recvFragment done sts=" << sts << " src=" << ret_rank << TLOG_ENDL;
					TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Done receiving fragment. Moving into output." << TLOG_ENDL;

					done = true; // no more polls
					break; // no more read of ready fds
				}
			}
		}
	} // while(!done)...poll

	TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::receiveFragment: Returning " << ret_rank << TLOG_ENDL;
	return ret_rank;
}

// Send the given Fragment. Return the rank of the destination to which
// the Fragment was sent OR -1 if to none.
artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendFragment_(Fragment&& frag, size_t send_timeout_usec)
{
	TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::sendFragment begin" << TLOG_ENDL;
	artdaq::Fragment grab_ownership_frag = std::move(frag);

	// Send Fragment Header

	iovec iov = { reinterpret_cast<void*>(grab_ownership_frag.headerAddress()),
		detail::RawFragmentHeader::num_words() * sizeof(RawDataType) };

	auto sts = sendData_(&iov, 1, send_retry_timeout_us_);
	while (sts != CopyStatus::kSuccess)
	{
		TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::sendFragment: Timeout or Error sending fragment" << TLOG_ENDL;
		sts = sendData_(&iov, 1, send_retry_timeout_us_);
		usleep(1000);
	}

	// Send Fragment Data

	auto start_time = std::chrono::steady_clock::now();
	iov = { reinterpret_cast<void*>(grab_ownership_frag.headerAddress() + detail::RawFragmentHeader::num_words()),
		grab_ownership_frag.sizeBytes() - detail::RawFragmentHeader::num_words() * sizeof(RawDataType) };
	sts = sendData_(&iov, 1, send_retry_timeout_us_);
	while (sts != CopyStatus::kSuccess && (send_timeout_usec == 0 || TimeUtils::GetElapsedTimeMicroseconds(start_time) < send_timeout_usec))
	{
		TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << " TCPSocketTransfer::sendFragment: Timeout or Error sending fragment" << TLOG_ENDL;
		sts = sendData_(&iov, 1, send_retry_timeout_us_);
		usleep(1000);
	}

	TRACE(7, "TCPSocketTransfer::sendFragment returning kSuccess");
	return sts;
}

artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendData_(const void* buf, size_t bytes, size_t send_timeout_usec)
{
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::sendData_ Converting buf to iovec" << TLOG_ENDL;
	iovec iov = { (void*)buf, bytes };
	return sendData_(&iov, 1, send_timeout_usec);
}

artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendData_(const struct iovec* iov, int iovcnt, size_t send_timeout_usec)
{
	// check all connected??? -- currently just check fd!=-1
	if (fd_ == -1)
	{
		if (timeoutMessageArmed_)
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::sendData_: Send fd is not open. Returning kTimeout" << TLOG_ENDL;
			timeoutMessageArmed_ = false;
		}
		return CopyStatus::kTimeout;
	}
	timeoutMessageArmed_ = true;
	TLOG_ARB(12, "TCPSocketTransfer") << uniqueLabel() << "send_timeout_usec is " << std::to_string(send_timeout_usec) << ", currently unused." << TLOG_ENDL;

	//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::sendData_: Determining write size" << TLOG_ENDL;
	uint32_t total_to_write_bytes = 0;
	std::vector<iovec> iov_in(iovcnt + 1); // need contiguous (for the unlike case that only partial MH
	std::vector<iovec> iovv(iovcnt + 2); // 1 more for mh and another one for any partial
	int ii;
	for (ii = 0; ii < iovcnt; ++ii)
	{
		iov_in[ii + 1] = iov[ii];
		total_to_write_bytes += iov[ii].iov_len;
	}
	//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::sendData_: Constructing Message Header" << TLOG_ENDL;
	MessHead mh = { 0,MessHead::data_v0,htons(source_rank()),{htonl(total_to_write_bytes)} };
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
		TLOG_ARB(7, "TCPSocketTransfer") << uniqueLabel() << "sendFragment b4 writev " << std::setw(7) << std::to_string(total_written_bytes) << " total_written_bytes fd=" << fd_ << " in_idx=" << std::to_string(in_iov_idx)
			<< " iovcnt=" << std::to_string(out_iov_idx) << " 1st.len=" << std::to_string(iovv[0].iov_len) << TLOG_ENDL;
		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer calling writev" << TLOG_ENDL;
		sts = writev(fd_, &(iovv[0]), out_iov_idx);
		//TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer done with writev" << TLOG_ENDL;

		if (sts == -1)
		{
			if (errno == EAGAIN /* same as EWOULDBLOCK */)
			{
				TLOG_ARB(2, "TCPSocketTransfer") << uniqueLabel() << "sendFragment EWOULDBLOCK" << TLOG_ENDL;
				fcntl(fd_, F_SETFL, 0); // clear O_NONBLOCK
				blocking = true;
				// NOTE: YES -- could drop here
				goto do_again;
			}
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::sendFragment_: WRITE ERROR: " << strerror(errno) << TLOG_ENDL;
			connect_state = 0; // any write error closes
			close(fd_);
			fd_ = -1;
			return TransferInterface::CopyStatus::kErrorNotRequiringException;
		}
		else if (sts != this_write_bytes)
		{
			// we'll loop around -- with
			TLOG_ARB(4, "TCPSocketTransfer") << uniqueLabel() << "sendFragment writev sts(" << std::to_string(sts) << ")!=requested_send_bytes(" << std::to_string(this_write_bytes) << ")" << TLOG_ENDL;
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
			TLOG_ARB(4, "TCPSocketTransfer") << uniqueLabel() << "sendFragment writev sts!=: this_write_bytes=" << std::to_string(this_write_bytes)
				<< " out_iov_idx=" << std::to_string(out_iov_idx)
				<< " additional=" << std::to_string(additional)
				<< " ii=" << ii << TLOG_ENDL;
		}
		else
		{
			TLOG_ARB(4, "TCPSocketTransfer") << uniqueLabel() << "sendFragment writev sts(" << std::to_string(sts) << ")==requested_send_bytes(" << std::to_string(this_write_bytes) << ")" << TLOG_ENDL;
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
	} while (total_written_bytes < total_to_write_bytes);
	if (total_written_bytes > total_to_write_bytes)
		TLOG_ARB(0, "TCPSocketTransfer") << uniqueLabel() << "sendFragment program error: too many bytes transferred" << TLOG_ENDL;

	if (blocking)
	{
		blocking = false;
		fcntl(fd_, F_SETFL, 0); // clear O_NONBLOCK
	}
	sts = total_written_bytes - sizeof(MessHead);

	TLOG_ARB(10, "TCPSocketTransfer") << uniqueLabel() << "sendFragment sts=" << std::to_string(sts) << TLOG_ENDL;
	return TransferInterface::CopyStatus::kSuccess;
}

//=============================================

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
		TLOG_ARB(5, "TCPSocketTransfer") << uniqueLabel() << "thread1 after wait_until(msdly=" << msdly << ") - sts=" << static_cast<int>(sts) << TLOG_ENDL;

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

void artdaq::TCPSocketTransfer::connect_()
{
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "Connecting sender socket" << TLOG_ENDL;
	int sndbuf_bytes = static_cast<int>(sndbuf_);
	fd_ = TCPConnect(hostMap_[destination_rank()].hostname.c_str()
		, calculate_port_()
		, O_NONBLOCK
		, sndbuf_bytes);
	connect_state = 0;
	blocking = 0;
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::connect_ " + hostMap_[destination_rank()].hostname + ":" << calculate_port_() << " fd_=" << fd_ << TLOG_ENDL;
	if (fd_ != -1)
	{
		// write connect msg
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::connect_: Writing connect message" << TLOG_ENDL;
		MessHead mh = { 0,MessHead::connect_v0,htons(source_rank()),{htonl(CONN_MAGIC)} };
		ssize_t sts = write(fd_, &mh, sizeof(mh));
		if (sts == -1)
		{
			TLOG_ERROR("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::connect_: Error writing connect message!" << TLOG_ENDL;
			// a write error here is completely unexpected!
			connect_state = 0;
			close(fd_);
			fd_ = -1;
		}
		else
		{
			TLOG_INFO("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::connect_: Successfully connected" << TLOG_ENDL;
			// consider it all connected/established
			connect_state = 1;
		}
	}
}

void artdaq::TCPSocketTransfer::reconnect_()
{
	TLOG_ARB(5, "TCPSocketTransfer") << uniqueLabel() << "check/reconnect" << TLOG_ENDL;
	if (fd_ == -1 && role() == TransferInterface::Role::kSend) return connect_();
	if ((fd_ == -1 || listen_fd_ == -1) && role() == TransferInterface::Role::kReceive) return listen_();
}

void artdaq::TCPSocketTransfer::listen_()
{
	TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Listening/accepting new connections" << TLOG_ENDL;
	if (listen_fd_ == -1)
	{
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Opening listener" << TLOG_ENDL;
		listen_fd_ = TCP_listen_fd(calculate_port_(), rcvbuf_);
	}
	if (listen_fd_ == -1)
	{
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Error creating listen_fd_!" << TLOG_ENDL;
		return;
	}

	int res;
	timeval tv = { 2,0 }; // maybe increase of some global "debugging" flag set???
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
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "Calling accept" << TLOG_ENDL;
		fd = accept(listen_fd_, (sockaddr *)&un, &arglen);
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "Done with accept" << TLOG_ENDL;

		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Reading connect message" << TLOG_ENDL;
		socklen_t lenlen = sizeof(tv);
		/*sts=*/
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, lenlen); // see man 7 socket.
		MessHead mh;
		uint64_t mark_us = TimeUtils::gettimeofday_us();
		sts = read(fd, &mh, sizeof(mh));
		uint64_t delta_us = TimeUtils::gettimeofday_us() - mark_us;
		TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Read of connect message took " << delta_us << " microseconds." << TLOG_ENDL;
		TLOG_ARB(10, "TCPSocketTransfer") << uniqueLabel() << "do_connect read of connect msg (after accept) took " << std::to_string(delta_us) << " microseconds" << TLOG_ENDL; // emperically, read take a couple hundred usecs.
		if (sts != sizeof(mh))
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Wrong message header length received!" << TLOG_ENDL;
			TLOG_ARB(0, "TCPSocketTransfer") << uniqueLabel() << "do_connect_ problem with connect msg sts(" << sts << ")!=sizeof(mh)(" << std::to_string(sizeof(mh)) << ")" << TLOG_ENDL;
			close(fd);
			return;
		}

		// check for "magic" and valid source_id(aka rank)
		mh.source_id = ntohs(mh.source_id); // convert here as it is reference several times
		if (ntohl(mh.conn_magic) != CONN_MAGIC || mh.source_id != source_rank())
		{
			TLOG_DEBUG("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: Wrong magic bytes in header!" << TLOG_ENDL;
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
		TLOG_INFO("TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::listen_: New fd is " << fd_ << TLOG_ENDL;

		TLOG_ARB(3, "TCPSocketTransfer") << uniqueLabel() << "do_connect_ connection from sender_rank=" << std::to_string(mh.source_id) << TLOG_ENDL;
	}
	else
	{
		TLOG_ARB(10, "TCPSocketTransfer") << uniqueLabel() << "TCPSocketTransfer::do_connect_: No connections in timeout interval!" << TLOG_ENDL;
	}
} // do_connect_

DEFINE_ARTDAQ_TRANSFER(artdaq::TCPSocketTransfer)
