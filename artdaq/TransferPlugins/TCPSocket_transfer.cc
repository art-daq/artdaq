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

int artdaq::TCPSocketTransfer::listen_thread_refcount_ = 0;
std::unique_ptr<boost::thread> artdaq::TCPSocketTransfer::listen_thread_ = nullptr;
std::map<int, std::set<int>> artdaq::TCPSocketTransfer::connected_fds_ = std::map<int, std::set<int>>();

artdaq::TCPSocketTransfer::
TCPSocketTransfer(fhicl::ParameterSet const& pset, TransferInterface::Role role)
	: TransferInterface(pset, role)
	, send_fd_(-1)
	, active_receive_fd_(-1)
	, last_active_receive_fd_(-1)
	, state_(SocketState::Metadata)
	, offset(0)
	, target_bytes(sizeof(MessHead))
	, rcvbuf_(pset.get<size_t>("tcp_receive_buffer_size", 0))
	, sndbuf_(max_fragment_size_words_ * sizeof(artdaq::RawDataType) * buffer_count_)
	, send_retry_timeout_us_(pset.get<size_t>("send_retry_timeout_us", 1000000))
	, stats_connect_stop_(false)
	, stats_connect_thread_(std::bind(&TCPSocketTransfer::stats_connect_, this))
	, timeoutMessageArmed_(true)
    , not_connected_count_(0)
    , receive_err_threshold_(pset.get<size_t>("receive_socket_disconnected_max_count", 1000))
    , receive_err_wait_us_(pset.get<size_t>("receive_socket_disconnected_wait_us", 10000))
{
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer Constructor: pset=" << pset.to_string() << ", role=" << (role == TransferInterface::Role::kReceive ? "kReceive" : "kSend") << TLOG_ENDL;
	auto masterPortOffset = pset.get<int>("offset_all_ports", 0);
	hostMap_ = MakeHostMap(pset, masterPortOffset);

	std::function<void()> function = std::bind(&TCPSocketTransfer::reconnect_, this);
	tmo_.add_periodic("reconnect", NULL, function, 200/*millisec*/);

	if (role == TransferInterface::Role::kReceive)
	{
		// Wait for sender to connect...
		TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Listening for connections" << TLOG_ENDL;
		start_listen_thread_();
		TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Done Listening" << TLOG_ENDL;
	}
	else
	{
		TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Connecting to destination" << TLOG_ENDL;
		connect_();
		TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Done Connecting" << TLOG_ENDL;
	}
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: End of Constructor" << TLOG_ENDL;
}

artdaq::TCPSocketTransfer::~TCPSocketTransfer()
{
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Shutting down TCPSocketTransfer" << TLOG_ENDL;
	stats_connect_stop_ = true;
	stopstatscv_.notify_all();
	stats_connect_thread_.join();

	if (role() == TransferInterface::Role::kSend)
	{
		// close all open connections (send stop_v0) first
		MessHead mh = { 0,MessHead::stop_v0,htons(TransferInterface::source_rank()),{0} };
		if (send_fd_ != -1)
		{
			// should be blocking with modest timeo
			timeval tv = { 0,100000 };
			socklen_t len = sizeof(tv);
			setsockopt(send_fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, len);
			write(send_fd_, &mh, sizeof(mh));
		}
		close(send_fd_);
	}
	else
	{
		for (auto& fd : connected_fds_[source_rank()])
		{
			close(fd); 
			connected_fds_[source_rank()].erase(fd);
		}

		static std::mutex teardown_mutex;
		std::unique_lock<std::mutex> lk(teardown_mutex);
		listen_thread_refcount_--;
		if (listen_thread_refcount_ == 0 && listen_thread_->joinable())
		{
			listen_thread_->join();
		}
	}
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: End of Destructor" << TLOG_ENDL;
}

int artdaq::TCPSocketTransfer::receiveFragmentHeader(detail::RawFragmentHeader& header, size_t timeout_usec)
{
	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: BEGIN" << TLOG_ENDL;
	int ret_rank = RECV_TIMEOUT;

	if (connected_fds_[source_rank()].size() == 0)
	{ // what if just listen_fd??? 
        if(++not_connected_count_ > receive_err_threshold_) { return DATA_END; }
        TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Receive socket not connected, returning RECV_TIMEOUT" << TLOG_ENDL;
        usleep(receive_err_wait_us_);
		return RECV_TIMEOUT;
	}
    not_connected_count_ = 0;

	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader timeout_usec=" << std::to_string(timeout_usec) << TLOG_ENDL;
	//void* buff=alloca(max_fragment_size_words_*8);
	size_t byte_cnt = 0;
	int sts;
	uint64_t start_time_us = TimeUtils::gettimeofday_us();

	while (active_receive_fd_ != -1) {
		TLOG_TRACE(GetTraceName()) << "Currently receiving from fd " << active_receive_fd_ << ", waiting!";
		usleep(1000);
	}

	
	uint8_t* buff;

	int timeout_ms;
	if (timeout_usec == 0)
		timeout_ms = 0;
	else
		timeout_ms = (timeout_usec + 999) / 1000; // want at least 1 ms

	bool done = false;
	while (!done)
	{
		if (active_receive_fd_ == -1) {
			size_t fd_count = connected_fds_[source_rank()].size();
			auto iter = connected_fds_[source_rank()].begin();
			std::vector<pollfd> pollfds(fd_count);
			for (size_t ii = 0; ii < fd_count; ++ii) {
				pollfds[ii].events = POLLIN | POLLERR;
				pollfds[ii].fd = *iter;
				++iter;
			}

			//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragment: Polling fd to see if there's data" << TLOG_ENDL;
			int num_fds_ready = poll(&pollfds[0], fd_count, timeout_ms);
			if (num_fds_ready <= 0)
			{
				if (num_fds_ready == 0 && timeout_ms > 0)
				{
					TLOG_ARB(7, GetTraceName()) <<"TCPSocketTransfer::receiveFragmentHeader: No data on receive socket, returning RECV_TIMEOUT" << TLOG_ENDL;
					return RECV_TIMEOUT;
				}
				break;
			}

			size_t index = 0;
			if (last_active_receive_fd_ != -1) {
				for (auto& pollfd : pollfds) {
					index++;
					if (pollfd.fd == last_active_receive_fd_) {
						break;
					}
				}
			}

			int active_index = -1;
			short anomolous_events = 0;
			for (size_t ii = index; ii < index + pollfds.size(); ++ii)
			{
				if (pollfds[index % pollfds.size()].revents & (POLLIN | POLLPRI | POLLHUP | POLLERR)) {
					active_index = index % pollfds.size();
					active_receive_fd_ = pollfds[active_index].fd;
					break;
				}
				else if (pollfds[index % pollfds.size()].revents & (POLLNVAL)) {
					TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Error on receive, closing socket" << TLOG_ENDL;
					close(pollfds[active_index].fd);
					connected_fds_[source_rank()].erase(pollfds[active_index].fd);
					continue;
				}
				else if (pollfds[index % pollfds.size()].revents) {
					anomolous_events |= pollfds[index % pollfds.size()].revents;
				}
			}

			if (active_index == -1)
			{
				TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Wrong event received from a pollfd. Mask: " << static_cast<int>(anomolous_events) << TLOG_ENDL;
				active_receive_fd_ = -1;
				continue;
			}
		}

		if (state_ == SocketState::Metadata)
		{
			//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Reading Message Header" << TLOG_ENDL;
			buff = &(mha[offset]);
			byte_cnt = sizeof(MessHead) - offset;
		}
		else
		{
			//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Reading data" << TLOG_ENDL;
			buff = reinterpret_cast<uint8_t*>(&header) + offset;
			byte_cnt = mh.byte_count - offset;
		}

		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Reading " << byte_cnt << " bytes from socket" << TLOG_ENDL;
		sts = read(active_receive_fd_, buff, byte_cnt);
		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Done with read" << TLOG_ENDL;

		TLOG_ARB(9, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader state=" << static_cast<int>(state_) << " read=" << sts << " (errno=" << strerror(errno) << ")" << TLOG_ENDL;
		if (sts <= 0)
		{
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Error on receive, closing socket" << TLOG_ENDL;
			close(active_receive_fd_);
			connected_fds_[source_rank()].erase(active_receive_fd_);
			active_receive_fd_ = -1;
		}
		else
		{
			// see if we're done (with this state)
			sts = offset += sts;
			if (sts == target_bytes)
			{
				TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Target read bytes reached. Changing state" << TLOG_ENDL;
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
					TLOG_ARB(9, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader done sts=" << sts << " src=" << ret_rank << TLOG_ENDL;
					TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Done receiving fragment. Moving into output." << TLOG_ENDL;

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

	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentHeader: Returning " << ret_rank << TLOG_ENDL;
	return ret_rank;
}

int artdaq::TCPSocketTransfer::receiveFragmentData(RawDataType* destination, size_t)
{
	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: BEGIN" << TLOG_ENDL;
	int ret_rank = RECV_TIMEOUT;
	if (active_receive_fd_ == -1)
	{ // what if just listen_fd??? 
		TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Receive socket not connected, returning RECV_TIMEOUT" << TLOG_ENDL;
		return RECV_TIMEOUT;
	}

	//void* buff=alloca(max_fragment_size_words_*8);
	uint8_t* buff;
	size_t byte_cnt = 0;
	int sts;

	pollfd pollfd_s;
	pollfd_s.events = POLLIN | POLLERR;
	pollfd_s.fd = active_receive_fd_;

	bool done = false;
	while (!done)
	{
		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Polling fd to see if there's data" << TLOG_ENDL;
		int num_fds_ready = poll(&pollfd_s, 1, -1);
		if (num_fds_ready <= 0)
		{
			if (num_fds_ready == 0)
			{
				TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: No data on receive socket, returning RECV_TIMEOUT" << TLOG_ENDL;
				return RECV_TIMEOUT;
			}
			break;
		}

		if (!(pollfd_s.revents & (POLLIN | POLLERR)))
		{
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Wrong event received from pollfd" << TLOG_ENDL;
			continue;
		}

		if (state_ == SocketState::Metadata)
		{
			//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Reading Message Header" << TLOG_ENDL;
			buff = &(mha[offset]);
			byte_cnt = sizeof(MessHead) - offset;
		}
		else
		{
			//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Reading data" << TLOG_ENDL;
			buff = reinterpret_cast<uint8_t*>(destination) + offset;
			byte_cnt = mh.byte_count - offset;
		}

		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Reading " << byte_cnt << " bytes from socket" << TLOG_ENDL;
		sts = read(active_receive_fd_, buff, byte_cnt);
		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Done with read" << TLOG_ENDL;

		TLOG_ARB(9, GetTraceName()) << "TCPSocketTransfer: recvFragment state=" << static_cast<int>(state_) << " read=" << sts << " (errno=" << strerror(errno) << ")" << TLOG_ENDL;
		if (sts <= 0)
		{
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Error on receive, closing socket" << TLOG_ENDL;
			close(active_receive_fd_);
			connected_fds_[source_rank()].erase(active_receive_fd_);
			active_receive_fd_ = -1;
		}
		else
		{
			// see if we're done (with this state)
			sts = offset += sts;
			if (sts == target_bytes)
			{
				TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Target read bytes reached. Changing state" << TLOG_ENDL;
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
					TLOG_ARB(9, GetTraceName()) << "TCPSocketTransfer: receiveFragmentData done sts=" << sts << " src=" << ret_rank << TLOG_ENDL;
					TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Done receiving fragment. Moving into output." << TLOG_ENDL;

					done = true; // no more polls
					break; // no more read of ready fds
				}
			}
		}
	} // while(!done)...poll

	last_active_receive_fd_ = active_receive_fd_;
	active_receive_fd_ = -1;

	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::receiveFragmentData: Returning " << ret_rank << TLOG_ENDL;
	return ret_rank;
}

// Send the given Fragment. Return the rank of the destination to which
// the Fragment was sent OR -1 if to none.
artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendFragment_(Fragment&& frag, size_t send_timeout_usec)
{
	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::sendFragment begin" << TLOG_ENDL;
	artdaq::Fragment grab_ownership_frag = std::move(frag);

	// Send Fragment Header

	iovec iov = { reinterpret_cast<void*>(grab_ownership_frag.headerAddress()),
		detail::RawFragmentHeader::num_words() * sizeof(RawDataType) };

	auto sts = sendData_(&iov, 1, send_retry_timeout_us_);
	auto start_time = std::chrono::steady_clock::now();
	//If it takes more than 10 seconds to write a Fragment header, give up
	while (sts != CopyStatus::kSuccess && (send_timeout_usec == 0 || TimeUtils::GetElapsedTimeMicroseconds(start_time) < send_timeout_usec) && TimeUtils::GetElapsedTimeMicroseconds(start_time) < 10000000)
	{
		TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::sendFragment: Timeout or Error sending fragment" << TLOG_ENDL;
		sts = sendData_(&iov, 1, send_retry_timeout_us_);
		usleep(1000);
	}
	if (sts != CopyStatus::kSuccess) return sts;

	// Send Fragment Data

	iov = { reinterpret_cast<void*>(grab_ownership_frag.headerAddress() + detail::RawFragmentHeader::num_words()),
		grab_ownership_frag.sizeBytes() - detail::RawFragmentHeader::num_words() * sizeof(RawDataType) };
	sts = sendData_(&iov, 1, send_retry_timeout_us_);
	while (sts != CopyStatus::kSuccess && (send_timeout_usec == 0 || TimeUtils::GetElapsedTimeMicroseconds(start_time) < send_timeout_usec))
	{
		TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::sendFragment: Timeout or Error sending fragment" << TLOG_ENDL;
		sts = sendData_(&iov, 1, send_retry_timeout_us_);
		usleep(1000);
	}

	TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer::sendFragment returning kSuccess";
	return sts;
}

artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendData_(const void* buf, size_t bytes, size_t send_timeout_usec)
{
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::sendData_ Converting buf to iovec" << TLOG_ENDL;
	iovec iov = { (void*)buf, bytes };
	return sendData_(&iov, 1, send_timeout_usec);
}

artdaq::TransferInterface::CopyStatus artdaq::TCPSocketTransfer::sendData_(const struct iovec* iov, int iovcnt, size_t send_timeout_usec)
{
	// check all connected??? -- currently just check fd!=-1
	if (send_fd_ == -1)
	{
		if (timeoutMessageArmed_)
		{
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::sendData_: Send fd is not open. Returning kTimeout" << TLOG_ENDL;
			timeoutMessageArmed_ = false;
		}
		return CopyStatus::kTimeout;
	}
	timeoutMessageArmed_ = true;
	TLOG_ARB(12, GetTraceName()) << "TCPSocketTransfer: send_timeout_usec is " << std::to_string(send_timeout_usec) << ", currently unused." << TLOG_ENDL;

	//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::sendData_: Determining write size" << TLOG_ENDL;
	uint32_t total_to_write_bytes = 0;
	std::vector<iovec> iov_in(iovcnt + 1); // need contiguous (for the unlike case that only partial MH
	std::vector<iovec> iovv(iovcnt + 2); // 1 more for mh and another one for any partial
	int ii;
	for (ii = 0; ii < iovcnt; ++ii)
	{
		iov_in[ii + 1] = iov[ii];
		total_to_write_bytes += iov[ii].iov_len;
	}
	//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::sendData_: Constructing Message Header" << TLOG_ENDL;
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
		TLOG_ARB(7, GetTraceName()) << "TCPSocketTransfer: sendFragment b4 writev " << std::setw(7) << std::to_string(total_written_bytes) << " total_written_bytes send_fd_=" << send_fd_ << " in_idx=" << std::to_string(in_iov_idx)
			<< " iovcnt=" << std::to_string(out_iov_idx) << " 1st.len=" << std::to_string(iovv[0].iov_len) << TLOG_ENDL;
		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer calling writev" << TLOG_ENDL;
		sts = writev(send_fd_, &(iovv[0]), out_iov_idx);
		//TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer done with writev" << TLOG_ENDL;

		if (sts == -1)
		{
			if (errno == EAGAIN /* same as EWOULDBLOCK */)
			{
				TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: sendFragment EWOULDBLOCK" << TLOG_ENDL;
				fcntl(send_fd_, F_SETFL, 0); // clear O_NONBLOCK
				blocking = true;
				// NOTE: YES -- could drop here
				goto do_again;
			}
			TLOG_WARNING(GetTraceName()) << "TCPSocketTransfer::sendFragment_: WRITE ERROR: " << strerror(errno) << TLOG_ENDL;
			connect_state = 0; // any write error closes
			close(send_fd_);
			send_fd_ = -1;
			return TransferInterface::CopyStatus::kErrorNotRequiringException;
		}
		else if (sts != this_write_bytes)
		{
			// we'll loop around -- with
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: sendFragment writev sts(" << std::to_string(sts) << ")!=requested_send_bytes(" << std::to_string(this_write_bytes) << ")" << TLOG_ENDL;
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
			TLOG_ARB(4, GetTraceName()) << "TCPSocketTransfer: sendFragment writev sts!=: this_write_bytes=" << std::to_string(this_write_bytes)
				<< " out_iov_idx=" << std::to_string(out_iov_idx)
				<< " additional=" << std::to_string(additional)
				<< " ii=" << ii << TLOG_ENDL;
		}
		else
		{
			TLOG_ARB(4, GetTraceName()) << "TCPSocketTransfer: sendFragment writev sts(" << std::to_string(sts) << ")==requested_send_bytes(" << std::to_string(this_write_bytes) << ")" << TLOG_ENDL;
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
		TLOG_ERROR(GetTraceName()) << "TCPSocketTransfer: sendFragment program error: too many bytes transferred" << TLOG_ENDL;

	if (blocking)
	{
		blocking = false;
		fcntl(send_fd_, F_SETFL, 0); // clear O_NONBLOCK
	}
	sts = total_written_bytes - sizeof(MessHead);

	TLOG_ARB(10, GetTraceName()) << "TCPSocketTransfer: sendFragment sts=" << std::to_string(sts) << TLOG_ENDL;
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
		TLOG_ARB(5, GetTraceName()) << "TCPSocketTransfer: thread1 after wait_until(msdly=" << msdly << ") - sts=" << static_cast<int>(sts) << TLOG_ENDL;

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
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Connecting sender socket" << TLOG_ENDL;
	int sndbuf_bytes = static_cast<int>(sndbuf_);
	send_fd_ = TCPConnect(hostMap_[destination_rank()].hostname.c_str()
		, calculate_port_()
		, O_NONBLOCK
		, sndbuf_bytes);
	connect_state = 0;
	blocking = 0;
	TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::connect_ " + hostMap_[destination_rank()].hostname + ":" << calculate_port_() << " send_fd_=" << send_fd_ << TLOG_ENDL;
	if (send_fd_ != -1)
	{
		// write connect msg
		TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::connect_: Writing connect message" << TLOG_ENDL;
		MessHead mh = { 0,MessHead::connect_v0,htons(source_rank()),{htonl(CONN_MAGIC)} };
		ssize_t sts = write(send_fd_, &mh, sizeof(mh));
		if (sts == -1)
		{
			TLOG_ERROR(GetTraceName()) << "TCPSocketTransfer::connect_: Error writing connect message!" << TLOG_ENDL;
			// a write error here is completely unexpected!
			connect_state = 0;
			close(send_fd_);
			send_fd_ = -1;
		}
		else
		{
			TLOG_INFO(GetTraceName()) << "TCPSocketTransfer::connect_: Successfully connected" << TLOG_ENDL;
			// consider it all connected/established
			connect_state = 1;
		}
	}
}

void artdaq::TCPSocketTransfer::reconnect_()
{
	TLOG_TRACE(GetTraceName()) << "TCPSocketTransfer: check/reconnect" << TLOG_ENDL;
	if (send_fd_ == -1 && role() == TransferInterface::Role::kSend) return connect_();
}


void artdaq::TCPSocketTransfer::start_listen_thread_()
{
	static std::mutex start_listen_mutex;
	std::unique_lock<std::mutex> start_lock(start_listen_mutex);
	if (listen_thread_refcount_ == 0)
	{
		listen_thread_refcount_ = 1;
		if (listen_thread_ && listen_thread_->joinable()) listen_thread_->join();
		TLOG_INFO(GetTraceName()) << "TCPSocketTransfer: Starting Listener Thread" << TLOG_ENDL;
		listen_thread_ = std::make_unique<boost::thread>(&TCPSocketTransfer::listen_, this);
	}
	else
	{
		listen_thread_refcount_++;
	}
}

void artdaq::TCPSocketTransfer::listen_()
{
	int listen_fd = -1;
	while (listen_thread_refcount_ > 0)
	{
		TLOG_TRACE(GetTraceName()) << "TCPSocketTransfer::listen_: Listening/accepting new connections" << TLOG_ENDL;
		if (listen_fd == -1)
		{
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::listen_: Opening listener" << TLOG_ENDL;
			listen_fd = TCP_listen_fd(calculate_port_(), rcvbuf_);
		}
		if (listen_fd == -1)
		{
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::listen_: Error creating listen_fd!" << TLOG_ENDL;
			break;
		}

		int res;
		timeval tv = { 2,0 }; // maybe increase of some global "debugging" flag set???
		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(listen_fd, &rfds);

		res = select(listen_fd + 1, &rfds, (fd_set *)0, (fd_set *)0, &tv);
		if (res > 0)
		{
			int sts;
			sockaddr_un un;
			socklen_t arglen = sizeof(un);
			int fd;
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Calling accept" << TLOG_ENDL;
			fd = accept(listen_fd, (sockaddr *)&un, &arglen);
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer: Done with accept" << TLOG_ENDL;

			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::listen_: Reading connect message" << TLOG_ENDL;
			socklen_t lenlen = sizeof(tv);
			/*sts=*/
			setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, lenlen); // see man 7 socket.
			MessHead mh;
			uint64_t mark_us = TimeUtils::gettimeofday_us();
			sts = read(fd, &mh, sizeof(mh));
			uint64_t delta_us = TimeUtils::gettimeofday_us() - mark_us;
			TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::listen_: Read of connect message took " << delta_us << " microseconds." << TLOG_ENDL;
			if (sts != sizeof(mh))
			{
				TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::listen_: Wrong message header length received!" << TLOG_ENDL;
				close(fd);
				return;
			}

			// check for "magic" and valid source_id(aka rank)
			mh.source_id = ntohs(mh.source_id); // convert here as it is reference several times
			if (ntohl(mh.conn_magic) != CONN_MAGIC)
			{
				TLOG_DEBUG(GetTraceName()) << "TCPSocketTransfer::listen_: Wrong magic bytes in header!" << TLOG_ENDL;
				close(fd);
				return;
			}

			// now add (new) connection
			connected_fds_[mh.source_id].insert(fd);
			
			TLOG_INFO(GetTraceName()) << "TCPSocketTransfer::listen_: New fd is " << fd << " for source rank " << mh.source_id << TLOG_ENDL;
		}
		else
		{
			TLOG_ARB(10, GetTraceName()) << "TCPSocketTransfer::do_connect_: No connections in timeout interval!" << TLOG_ENDL;
		}
	}

	if (listen_fd != -1) close(listen_fd);
	for (auto& rank : connected_fds_)
	{
		for (auto& fd : rank.second)
		{
			close(fd);
		}
	}
	connected_fds_.clear();

} // do_connect_

DEFINE_ARTDAQ_TRANSFER(artdaq::TCPSocketTransfer)
