#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_TokenReceiver").c_str()

#include <arpa/inet.h>

#include <utility>

#include <utility>
#include "artdaq/DAQdata/TCP_listen_fd.hh"
#include "artdaq/DAQrate/detail/TokenReceiver.hh"

artdaq::TokenReceiver::TokenReceiver(const fhicl::ParameterSet& ps, std::shared_ptr<RoutingManagerPolicy> policy,
                                     size_t update_interval_msec)
    : token_port_(ps.get<int>("routing_token_port", 35555))
    , policy_(std::move(std::move(policy)))
    , update_interval_msec_(update_interval_msec)
    , token_socket_(-1)
    , token_epoll_fd_(-1)
    , thread_is_running_(false)
    , reception_is_paused_(false)
    , shutdown_requested_(false)
    , run_number_(0)
    , statsHelperPtr_(nullptr)
{
	receive_token_events_ = std::vector<epoll_event>(policy_->GetReceiverCount() + 1);
}

artdaq::TokenReceiver::~TokenReceiver()
{
	stopTokenReception(true);
}

void artdaq::TokenReceiver::startTokenReception()
{
	if (token_thread_.joinable())
	{
		token_thread_.join();
	}
	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 8000 KB

	reception_is_paused_ = false;
	shutdown_requested_ = false;

	TLOG(TLVL_INFO) << "Starting Token Reception Thread";
	try
	{
		token_thread_ = boost::thread(attrs, boost::bind(&TokenReceiver::receiveTokensLoop_, this));
		char tname[16];
		snprintf(tname, 16, "%d-TokenRecv", my_rank);  // NOLINT
		auto handle = token_thread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (boost::exception const& e)
	{
		TLOG(TLVL_ERROR) << "Exception encountered starting Token Reception thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Exception encountered starting Token Reception thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(3);
	}
	received_token_count_ = 0;
	thread_is_running_ = true;
	TLOG(TLVL_INFO) << "Started Token Reception Thread";
}

void artdaq::TokenReceiver::stopTokenReception(bool force)
{
	shutdown_requested_ = true;
	reception_is_paused_ = false;
	if (thread_is_running_)
	{
		if (received_token_count_ == 0 && !force)
		{
			TLOG(TLVL_DEBUG) << "Stop request received by TokenReceiver, but no tokens have ever been received.";
		}
		TLOG(TLVL_DEBUG) << "Joining tokenThread";
		try
		{
			if (token_thread_.joinable())
			{
				token_thread_.join();
			}
		}
		catch (...)
		{
			// IGNORED
		}
		thread_is_running_ = false;
	}

	if (token_socket_ != -1)
	{
		close(token_socket_);
		token_socket_ = -1;
		token_epoll_fd_ = -1;
	}
}

void artdaq::TokenReceiver::receiveTokensLoop_()
{
	while (!shutdown_requested_)
	{
		TLOG(TLVL_TRACE) << "Receive Token loop start";
		if (token_socket_ == -1)
		{
			TLOG(TLVL_DEBUG) << "Opening token listener socket";
			token_socket_ = TCP_listen_fd(token_port_, 3 * sizeof(detail::RoutingToken));

			if (token_epoll_fd_ != -1)
			{
				close(token_epoll_fd_);
			}
			struct epoll_event ev;
			token_epoll_fd_ = epoll_create1(0);
			ev.events = EPOLLIN;
			ev.data.fd = token_socket_;
			if (epoll_ctl(token_epoll_fd_, EPOLL_CTL_ADD, token_socket_, &ev) == -1)
			{
				TLOG(TLVL_ERROR) << "Could not register listen socket to epoll fd";
				exit(3);
			}
		}
		if (token_socket_ == -1 || token_epoll_fd_ == -1)
		{
			TLOG(TLVL_DEBUG) << "One of the listen sockets was not opened successfully.";
			return;
		}

		auto nfds = epoll_wait(token_epoll_fd_, &receive_token_events_[0], receive_token_events_.size(), update_interval_msec_);
		if (nfds == -1)
		{
			TLOG(TLVL_ERROR) << "Error status received from epoll_wait, exiting with code " << EXIT_FAILURE << ", errno=" << errno << " (" << strerror(errno) << ")";
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		while (reception_is_paused_ && !shutdown_requested_)
		{
			usleep(10000);
		}

		TLOG(13) << "Received " << nfds << " events on token sockets";
		for (auto n = 0; n < nfds; ++n)
		{
			if (receive_token_events_[n].data.fd == token_socket_)
			{
				TLOG(TLVL_DEBUG) << "Accepting new connection on token_socket";
				sockaddr_in addr;
				socklen_t arglen = sizeof(addr);
				auto conn_sock = accept(token_socket_, reinterpret_cast<struct sockaddr*>(&addr), &arglen);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
				fcntl(conn_sock, F_SETFL, O_NONBLOCK);                                                       // set O_NONBLOCK

				if (conn_sock == -1)
				{
					TLOG(TLVL_ERROR) << "Error status received from accept, exiting with code " << EXIT_FAILURE << ", errno=" << errno << " (" << strerror(errno) << ")";
					perror("accept");
					exit(EXIT_FAILURE);
				}

				receive_token_addrs_[conn_sock] = std::string(inet_ntoa(addr.sin_addr));
				TLOG(TLVL_DEBUG) << "New fd is " << conn_sock << " for data-receiver at " << receive_token_addrs_[conn_sock];
				struct epoll_event ev;
				ev.events = EPOLLIN;
				ev.data.fd = conn_sock;
				if (epoll_ctl(token_epoll_fd_, EPOLL_CTL_ADD, conn_sock, &ev) == -1)
				{
					TLOG(TLVL_ERROR) << "Error status received from epoll_ctl, exiting with code " << EXIT_FAILURE << ", errno=" << errno << " (" << strerror(errno) << ")";
					perror("epoll_ctl: conn_sock");
					exit(EXIT_FAILURE);
				}
			}
			else if ((receive_token_events_[n].events & EPOLLIN) != 0)
			{
				auto startTime = artdaq::MonitoredQuantity::getCurrentTime();

				detail::RoutingToken buff;
				int sts = recv(receive_token_events_[n].data.fd, &buff, sizeof(detail::RoutingToken), MSG_WAITALL);
				if (sts == 0)
				{
					TLOG(TLVL_WARNING) << "Received 0-size token from " << receive_token_addrs_[receive_token_events_[n].data.fd] << ", closing socket";
					receive_token_addrs_.erase(receive_token_events_[n].data.fd);
					close(receive_token_events_[n].data.fd);
					epoll_ctl(token_epoll_fd_, EPOLL_CTL_DEL, receive_token_events_[n].data.fd, nullptr);
				}
				else if (sts < 0 && errno == EAGAIN)
				{
					TLOG(TLVL_DEBUG) << "No more tokens from this rank. Continuing poll loop.";
				}
				else if (sts < 0)
				{
					TLOG(TLVL_ERROR) << "Error reading from token socket: sts=" << sts << ", errno=" << errno;
					receive_token_addrs_.erase(receive_token_events_[n].data.fd);
					close(receive_token_events_[n].data.fd);
					epoll_ctl(token_epoll_fd_, EPOLL_CTL_DEL, receive_token_events_[n].data.fd, nullptr);
				}
				else if (sts == sizeof(detail::RoutingToken) && buff.header != TOKEN_MAGIC)
				{
					TLOG(TLVL_ERROR) << "Received invalid token from " << receive_token_addrs_[receive_token_events_[n].data.fd] << " sts=" << sts;
				}
				else if (sts == sizeof(detail::RoutingToken))
				{
					TLOG(TLVL_DEBUG) << "Received token from " << buff.rank << " indicating " << buff.new_slots_free << " slots are free. (run=" << buff.run_number << ")";
					if (buff.run_number != run_number_)
					{
						TLOG(TLVL_DEBUG) << "Received token from a different run number! Current = " << run_number_ << ", token = " << buff.run_number << ", ignoring (n=" << buff.new_slots_free << ")";
					}
					else
					{
						received_token_count_ += buff.new_slots_free;
						policy_->AddReceiverToken(buff.rank, buff.new_slots_free);
					}
				}
				auto delta_time = artdaq::MonitoredQuantity::getCurrentTime() - startTime;
				if (statsHelperPtr_ != nullptr) { statsHelperPtr_->addSample(tokens_received_stat_key_, delta_time); }
			}
			else
			{
				TLOG(TLVL_DEBUG) << "Received event mask " << receive_token_events_[n].events << " from token fd " << receive_token_events_[n].data.fd;
			}
		}
	}
}
