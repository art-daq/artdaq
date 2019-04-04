#define TRACE_NAME "RoutingMasterCore_t"

#define BOOST_TEST_MODULE RoutingMasterCore_t
#include <boost/test/auto_unit_test.hpp>

#include "artdaq/Application/RoutingMasterCore.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQrate/RequestSender.hh"
#include "sys/poll.h"

/**
 * \brief RoutingMasterCoreTest class for testing (friend of RoutingMasterCore to collect internal state
 */
class artdaqtest::RoutingMasterCoreTest
{
public:
	explicit RoutingMasterCoreTest(artdaq::RoutingMasterCore& rm)
	    : routing_master_(&rm), table_socket_(-1), ack_socket_(-1)
	{}

	~RoutingMasterCoreTest()
	{
		if (routing_master_ && !routing_master_->shutdown_requested_.load()) routing_master_->shutdown_requested_.store(true);
		if (routing_master_thread_.joinable()) routing_master_thread_.join();
	}

	void call_receive_tokens_() { routing_master_->receive_tokens_(); }
	void call_start_receive_token_thread_() { routing_master_->start_receive_token_thread_(); }
	artdaq::detail::RoutingPacket call_get_current_table_() { return routing_master_->get_current_table_(); }
	std::string call_buildStatisticsString_() { return routing_master_->buildStatisticsString_(); }
	void call_sendMetrics_() { routing_master_->sendMetrics_(); }

	art::RunID get_run_id_() { return routing_master_->run_id_; }

	fhicl::ParameterSet const& get_policy_pset_() { return routing_master_->policy_pset_; }
	int const& get_rt_priority_() { return routing_master_->rt_priority_; }

	size_t const& get_max_table_update_interval_ms_() { return routing_master_->max_table_update_interval_ms_; }
	size_t const& get_max_ack_cycle_count_() { return routing_master_->max_ack_cycle_count_; }
	size_t const& get_table_entry_timeout_ms_() { return routing_master_->table_entry_timeout_ms_; }
	artdaq::detail::RoutingMasterMode const& get_routing_mode_() { return routing_master_->routing_mode_; }
	std::atomic<size_t> const& get_current_table_interval_ms_() { return routing_master_->current_table_interval_ms_; }
	std::atomic<size_t> const& get_table_update_count_() { return routing_master_->table_update_count_; }
	std::atomic<size_t> const& get_received_token_count_() { return routing_master_->received_token_count_; }
	std::unordered_map<int, size_t> const& get_received_token_counter_() { return routing_master_->received_token_counter_; }

	std::map<std::chrono::steady_clock::time_point, artdaq::detail::RoutingPacket> const& get_current_tables_() { return routing_master_->current_tables_; }

	std::vector<int> const& get_sender_ranks_() { return routing_master_->sender_ranks_; }

	std::unique_ptr<artdaq::RoutingMasterPolicy> const& get_policy_() { return routing_master_->policy_; }

	std::atomic<bool> const& get_shutdown_requested_() { return routing_master_->shutdown_requested_; }
	std::atomic<bool> const& get_stop_requested_() { return routing_master_->stop_requested_; }
	std::atomic<bool> const& get_pause_requested_() { return routing_master_->pause_requested_; }

	artdaq::StatisticsHelper const& get_statsHelper_() { return routing_master_->statsHelper_; }

	int const& get_receive_token_port_() { return routing_master_->receive_token_port_; }
	int const& get_send_tables_port_() { return routing_master_->send_tables_port_; }
	int const& get_receive_acks_port_() { return routing_master_->receive_acks_port_; }
	std::string const& get_send_tables_address_() { return routing_master_->send_tables_address_; }
	std::string const& get_receive_address_() { return routing_master_->receive_address_; }
	struct sockaddr_in const& get_send_tables_addr_() { return routing_master_->send_tables_addr_; }
	std::vector<epoll_event> const& get_receive_ack_events_() { return routing_master_->receive_ack_events_; }
	std::vector<epoll_event> const& get_receive_token_events_() { return routing_master_->receive_token_events_; }
	std::unordered_map<int, std::string> const& get_receive_token_addrs_() { return routing_master_->receive_token_addrs_; }
	int const& get_token_epoll_fd_() { return routing_master_->token_epoll_fd_; }

	int const& get_token_socket_() { return routing_master_->token_socket_; }
	int const& get_table_socket_() { return routing_master_->table_socket_; }
	int const& get_ack_socket_() { return routing_master_->ack_socket_; }

	static fhicl::ParameterSet MakeRoutingMasterPset()
	{
		fhicl::ParameterSet ps;
		fhicl::ParameterSet daq_ps;
		daq_ps.put<std::vector<int>>("sender_ranks", {5, 6, 7});

		fhicl::ParameterSet policy_ps;
		policy_ps.put<std::vector<int>>("receiver_ranks", {1, 2, 3, 8});
		policy_ps.put<std::string>("policy", "NoOp");
		daq_ps.put<fhicl::ParameterSet>("policy", policy_ps);
		ps.put<fhicl::ParameterSet>("daq", daq_ps);

		return ps;
	}

	void setupTableListener()
	{
		int sts;
		table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (table_socket_ < 0)
		{
			TLOG(TLVL_ERROR) << "Error creating socket for receiving table updates!";
			exit(1);
		}

		struct sockaddr_in si_me_request;

		int yes = 1;
		if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		{
			TLOG(TLVL_ERROR) << " Unable to enable port reuse on request socket";
			exit(1);
		}
		memset(&si_me_request, 0, sizeof(si_me_request));
		si_me_request.sin_family = AF_INET;
		si_me_request.sin_port = htons(get_send_tables_port_());
		//si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
		struct in_addr in_addr_s;
		sts = inet_aton(get_send_tables_address_().c_str(), &in_addr_s);
		if (sts == 0)
		{
			TLOG(TLVL_ERROR) << "inet_aton says table_address " << get_send_tables_address_() << " is invalid";
		}
		si_me_request.sin_addr.s_addr = in_addr_s.s_addr;
		if (bind(table_socket_, (struct sockaddr*)&si_me_request, sizeof(si_me_request)) == -1)
		{
			TLOG(TLVL_ERROR) << "Cannot bind request socket to port " << get_send_tables_port_();
			exit(1);
		}

		struct ip_mreq mreq;
		sts = ResolveHost(get_send_tables_address_().c_str(), mreq.imr_multiaddr);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Unable to resolve multicast address for table updates";
			exit(1);
		}
		mreq.imr_interface.s_addr = htonl(INADDR_ANY);
		if (setsockopt(table_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
		{
			TLOG(TLVL_ERROR) << "Unable to join multicast group";
			exit(1);
		}
	}

	artdaq::detail::RoutingPacket receiveTableUpdate()
	{
		TLOG(TLVL_TRACE) << __func__ << ": Polling table socket for new routes";
		if (table_socket_ == -1)
		{
			TLOG(TLVL_DEBUG) << __func__ << ": Opening table listener socket";
			setupTableListener();
		}
		if (table_socket_ == -1)
		{
			TLOG(TLVL_DEBUG) << __func__ << ": The listen socket was not opened successfully.";
			throw cet::exception("SocketException") << "The listen socket was not opened successfully.";
		}

		struct pollfd fd;
		fd.fd = table_socket_;
		fd.events = POLLIN | POLLPRI;

		auto res = poll(&fd, 1, 10000);
		if (res > 0)
		{
			std::vector<uint8_t> buf(MAX_ROUTING_TABLE_SIZE);
			artdaq::detail::RoutingPacketHeader hdr;

			TLOG(TLVL_DEBUG) << __func__ << ": Going to receive RoutingPacketHeader";
			struct sockaddr_in from;
			socklen_t len = sizeof(from);
			auto stss = recvfrom(table_socket_, &buf[0], MAX_ROUTING_TABLE_SIZE, 0, (struct sockaddr*)&from, &len);
			TLOG(TLVL_DEBUG) << __func__ << ": Received " << stss << " bytes from " << inet_ntoa(from.sin_addr) << ":" << from.sin_port;

			if (stss > static_cast<ssize_t>(sizeof(hdr)))
			{
				memcpy(&hdr, &buf[0], sizeof(artdaq::detail::RoutingPacketHeader));
			}
			else if (stss == -1)
			{
				TLOG(TLVL_ERROR) << "Error in recvfrom: " << errno;
				throw cet::exception("SocketException") << "Error in recvfrom: " << errno;
			}
			else
			{
				TLOG(TLVL_TRACE) << __func__ << ": Incorrect size received. Discarding.";
				throw cet::exception("TableException") << "Incorrect size received. Discarding.";
			}

			TRACE(TLVL_DEBUG, "receiveTableUpdatesLoop_: Checking for valid header with nEntries=%lu headerData:0x%016lx%016lx", hdr.nEntries, ((unsigned long*)&hdr)[0], ((unsigned long*)&hdr)[1]);
			if (hdr.header != ROUTING_MAGIC)
			{
				TLOG(TLVL_TRACE) << __func__ << ": non-RoutingPacket received. No ROUTING_MAGIC. size(bytes)=" << stss;
				throw cet::exception("TableException") << __func__ << ": non-RoutingPacket received. No ROUTING_MAGIC. size(bytes)=" << stss;
			}
			if (get_routing_mode_() != artdaq::detail::RoutingMasterMode::INVALID && get_routing_mode_() != hdr.mode)
			{
				TLOG(TLVL_ERROR) << __func__ << ": Received table has different RoutingMasterMode than expected!";
				throw cet::exception("TableException") << __func__ << ": Received table has different RoutingMasterMode than expected!";
			}

			artdaq::detail::RoutingPacket buffer(hdr.nEntries);
			assert(static_cast<size_t>(stss) == sizeof(artdaq::detail::RoutingPacketHeader) + sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);
			memcpy(&buffer[0], &buf[sizeof(artdaq::detail::RoutingPacketHeader)], sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);
			TRACE(6, "receiveTableUpdatesLoop_: Received a packet of %ld bytes. 1st 16 bytes: 0x%016lx%016lx", stss, ((unsigned long*)&buffer[0])[0], ((unsigned long*)&buffer[0])[1]);

			if (buffer[0].sequence_id + hdr.nEntries - 1 != buffer[buffer.size() - 1].sequence_id)
			{
				TLOG(TLVL_ERROR) << __func__ << ": Skipping this RoutingPacket because the first (" << buffer[0].sequence_id << ") and last (" << buffer[buffer.size() - 1].sequence_id << ") entries are inconsistent (sz=" << hdr.nEntries << ")!";
				throw cet::exception("TableException") << __func__ << ": Skipping this RoutingPacket because the first (" << buffer[0].sequence_id << ") and last (" << buffer[buffer.size() - 1].sequence_id << ") entries are inconsistent (sz=" << hdr.nEntries << ")!";
			}

			return buffer;
		}
		TLOG(TLVL_ERROR) << "Error receiving routing table from Routing master: " << errno;
		throw cet::exception("TableException") << "Error receiving routing table from Routing master: " << errno;
		return artdaq::detail::RoutingPacket(0);
	}

	void sendAck(int rank, uint64_t first, uint64_t last)
	{
		if (ack_socket_ == -1)
		{
			ack_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
			auto sts = ResolveHost(get_receive_address_().c_str(), get_receive_acks_port_(), ack_addr_);
			if (sts == -1)
			{
				TLOG(TLVL_ERROR) << __func__ << ": Unable to resolve routing_master_address";
				exit(1);
			}
			TLOG(TLVL_DEBUG) << __func__ << ": Ack socket is fd " << ack_socket_;
		}

		artdaq::detail::RoutingAckPacket ack;
		ack.rank = rank;
		ack.first_sequence_id = first;
		ack.last_sequence_id = last;

		TLOG(TLVL_DEBUG) << __func__ << ": Sending RoutingAckPacket with first= " << first << " and last= " << last << " to " << get_receive_address_() << ", port " << get_receive_acks_port_() << " (rank = " << rank << ")";
		sendto(ack_socket_, &ack, sizeof(artdaq::detail::RoutingAckPacket), 0, (struct sockaddr*)&ack_addr_, sizeof(ack_addr_));
	}

	void start_rmcore_table_thread()
	{
		boost::thread::attributes attrs;
		attrs.set_stack_size(4096 * 2000);  // 8 MB
		routing_master_thread_ = boost::thread(attrs, boost::bind(&artdaq::RoutingMasterCore::process_event_table, routing_master_));
	}

private:
	artdaq::RoutingMasterCore* routing_master_;
	boost::thread routing_master_thread_;

	int table_socket_;
	int ack_socket_;
	struct sockaddr_in ack_addr_;
};

BOOST_AUTO_TEST_SUITE(RoutingMasterCore_t)

BOOST_AUTO_TEST_CASE(Construct)
{
	TLOG(TLVL_INFO) << "Test case Construct START";
	app_name = "RoutingMasterCore_t";
	my_rank = 54;
	artdaq::RoutingMasterCore core;
	BOOST_REQUIRE_EQUAL(core.get_update_count(), 0);
	BOOST_REQUIRE(core.report("").size() > 0);
	TLOG(TLVL_INFO) << "Test case Construct END";
}

BOOST_AUTO_TEST_CASE(Initialize)
{
	TLOG(TLVL_INFO) << "Test case Initialize START";
	fhicl::ParameterSet ps = artdaqtest::RoutingMasterCoreTest::MakeRoutingMasterPset();

	artdaq::RoutingMasterCore core;
	artdaqtest::RoutingMasterCoreTest coreTest(core);

	auto sts = core.initialize(ps, 0ULL, 0ULL);
	BOOST_REQUIRE_EQUAL(sts, true);

	// Check some parameters
	BOOST_REQUIRE(coreTest.get_policy_() != nullptr);
	BOOST_REQUIRE_EQUAL(coreTest.get_policy_()->GetReceiverCount(), 4);
	BOOST_REQUIRE_EQUAL(coreTest.get_sender_ranks_().size(), 3);
	BOOST_REQUIRE_EQUAL(coreTest.get_max_ack_cycle_count_(), 5);

	core.shutdown(0ULL);
	TLOG(TLVL_INFO) << "Test case Initialize END";
}

BOOST_AUTO_TEST_CASE(StateMachine)
{
	TLOG(TLVL_INFO) << "Test case StateMachine START";
	fhicl::ParameterSet ps = artdaqtest::RoutingMasterCoreTest::MakeRoutingMasterPset();

	// All of these transitions should work without exceptions
	std::unique_ptr<artdaq::RoutingMasterCore> coreptr(new artdaq::RoutingMasterCore());
	std::unique_ptr<artdaqtest::RoutingMasterCoreTest> coretestptr(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));

	// Standard flow
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr->pause(0ULL, 0ULL);
	coreptr->resume(0ULL, 0ULL);
	coreptr->stop(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Stop from pause
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr->pause(0ULL, 0ULL);
	coreptr->stop(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Shutdown from running
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Shutdown from pause
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr->pause(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Destruct from initialized
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Destruct from running
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Destruct from paused
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr->pause(0ULL, 0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	// Standard flow
	coreptr.reset(new artdaq::RoutingMasterCore());
	coretestptr.reset(new artdaqtest::RoutingMasterCoreTest(*coreptr.get()));
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coretestptr->start_rmcore_table_thread();
	coreptr->pause(0ULL, 0ULL);
	coreptr->resume(0ULL, 0ULL);
	coreptr->stop(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);
	coretestptr.reset(nullptr);

	TLOG(TLVL_INFO) << "Test case StateMachine END";
}

BOOST_AUTO_TEST_CASE(SendTokens)
{
	TLOG(TLVL_INFO) << "Test case SendTokens START";
	fhicl::ParameterSet ps = artdaqtest::RoutingMasterCoreTest::MakeRoutingMasterPset();

	artdaq::RoutingMasterCore core;
	artdaqtest::RoutingMasterCoreTest coreTest(core);

	auto sts = core.initialize(ps, 0ULL, 0ULL);
	BOOST_REQUIRE_EQUAL(sts, true);

	fhicl::ParameterSet sender_ps;
	fhicl::ParameterSet sender_rm_ps;
	sender_rm_ps.put<bool>("use_routing_master", true);
	sender_ps.put<fhicl::ParameterSet>("routing_token_config", sender_rm_ps);

	artdaq::RequestSender rs(sender_ps);

	my_rank = 3;
	// This token will be held until after the start of the run
	rs.SendRoutingToken(1, 1);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 0);

	core.start(art::RunID(1), 0ULL, 0ULL);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 1);

	rs.SendRoutingToken(1, 1);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 2);

	//This token should be ignored, as it is for the wrong run
	rs.SendRoutingToken(1, 5);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 2);

	// This token should be ignored, as it is not from a registered receiver
	my_rank = 10;
	rs.SendRoutingToken(1, 1);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 2);

	my_rank = 8;
	rs.SendRoutingToken(1, 1);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 3);

	rs.SendRoutingToken(0, 1);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 3);

	rs.SendRoutingToken(10, 1);
	usleep(100000);
	BOOST_REQUIRE_EQUAL(coreTest.get_received_token_count_(), 13);

	TLOG(TLVL_INFO) << "Test case SendTokens END";
}

BOOST_AUTO_TEST_CASE(Tables)
{
	TLOG(TLVL_INFO) << "Test case Tables BEGIN";
	fhicl::ParameterSet ps = artdaqtest::RoutingMasterCoreTest::MakeRoutingMasterPset();

	artdaq::RoutingMasterCore core;
	artdaqtest::RoutingMasterCoreTest coreTest(core);
	my_rank = 2;

	auto sts = core.initialize(ps, 0ULL, 0ULL);
	BOOST_REQUIRE_EQUAL(sts, true);

	fhicl::ParameterSet sender_ps;
	fhicl::ParameterSet sender_rm_ps;
	sender_rm_ps.put<bool>("use_routing_master", true);
	sender_ps.put<fhicl::ParameterSet>("routing_token_config", sender_rm_ps);

	artdaq::RequestSender rs(sender_ps);
	rs.SendRoutingToken(10, 1);

	core.start(art::RunID(1), 0ULL, 0ULL);
	coreTest.start_rmcore_table_thread();

	auto table = coreTest.receiveTableUpdate();
	BOOST_REQUIRE_EQUAL(table.size(), 10);

	auto first = table[0].sequence_id;
	auto last = table[table.size() - 1].sequence_id;
	BOOST_REQUIRE_GT(last, first);
	BOOST_REQUIRE_EQUAL(first, 1);
	BOOST_REQUIRE_EQUAL(last, 10);
	BOOST_REQUIRE_EQUAL(table[0].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(table[9].destination_rank, 2);

	coreTest.sendAck(5, first, last);
	coreTest.sendAck(6, first, last);
	coreTest.sendAck(7, first, last);

	my_rank = 1;
	rs.SendRoutingToken(1, 1);
	my_rank = 2;
	rs.SendRoutingToken(1, 1);
	my_rank = 3;
	rs.SendRoutingToken(1, 1);
	my_rank = 8;
	rs.SendRoutingToken(1, 1);
	my_rank = 3;
	rs.SendRoutingToken(1, 1);
	my_rank = 2;
	rs.SendRoutingToken(1, 1);
	my_rank = 1;
	rs.SendRoutingToken(1, 1);

	table = coreTest.receiveTableUpdate();
	BOOST_REQUIRE_EQUAL(table.size(), 7);
	BOOST_REQUIRE_EQUAL(table[0].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(table[0].sequence_id, 11);
	BOOST_REQUIRE_EQUAL(table[1].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(table[1].sequence_id, 12);
	BOOST_REQUIRE_EQUAL(table[2].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(table[2].sequence_id, 13);
	BOOST_REQUIRE_EQUAL(table[3].destination_rank, 8);
	BOOST_REQUIRE_EQUAL(table[3].sequence_id, 14);
	BOOST_REQUIRE_EQUAL(table[4].destination_rank, 3);
	BOOST_REQUIRE_EQUAL(table[4].sequence_id, 15);
	BOOST_REQUIRE_EQUAL(table[5].destination_rank, 2);
	BOOST_REQUIRE_EQUAL(table[5].sequence_id, 16);
	BOOST_REQUIRE_EQUAL(table[6].destination_rank, 1);
	BOOST_REQUIRE_EQUAL(table[6].sequence_id, 17);

	coreTest.sendAck(5, 11, 17);
	coreTest.sendAck(6, 11, 17);
	coreTest.sendAck(7, 11, 17);

	core.shutdown(0ULL);

	TLOG(TLVL_INFO) << "Test case Tables END";
}

BOOST_AUTO_TEST_SUITE_END()