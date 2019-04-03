#define TRACE_NAME "RoutingMasterCore_t"

#define BOOST_TEST_MODULE RoutingMasterCore_t
#include <boost/test/auto_unit_test.hpp>

#include "artdaq/Application/RoutingMasterCore.hh"
#include "artdaq/DAQrate/RequestSender.hh"

/**
 * \brief RoutingMasterCoreTest class for testing (friend of RoutingMasterCore to collect internal state
 */
class artdaqtest::RoutingMasterCoreTest
{
public:
	explicit RoutingMasterCoreTest(artdaq::RoutingMasterCore& rm)
	    : routing_master_(&rm) {}

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

private:
	artdaq::RoutingMasterCore* routing_master_;
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
	fhicl::ParameterSet ps;
	fhicl::ParameterSet daq_ps;
	daq_ps.put<std::vector<int>>("sender_ranks", {5, 6, 7});

	fhicl::ParameterSet policy_ps;
	policy_ps.put<std::vector<int>>("receiver_ranks", {1, 2, 3, 4});
	policy_ps.put<std::string>("policy", "NoOp");
	daq_ps.put<fhicl::ParameterSet>("policy", policy_ps);
	ps.put<fhicl::ParameterSet>("daq", daq_ps);

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
	fhicl::ParameterSet ps;
	fhicl::ParameterSet daq_ps;
	daq_ps.put<std::vector<int>>("sender_ranks", {5, 6, 7});

	fhicl::ParameterSet policy_ps;
	policy_ps.put<std::vector<int>>("receiver_ranks", {1, 2, 3, 4});
	policy_ps.put<std::string>("policy", "NoOp");
	daq_ps.put<fhicl::ParameterSet>("policy", policy_ps);
	ps.put<fhicl::ParameterSet>("daq", daq_ps);

	// All of these transitions should work without exceptions
	std::unique_ptr<artdaq::RoutingMasterCore> coreptr(new artdaq::RoutingMasterCore());

	// Standard flow
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coreptr->pause(0ULL, 0ULL);
	coreptr->resume(0ULL, 0ULL);
	coreptr->stop(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);

	// Stop from pause
	coreptr.reset(new artdaq::RoutingMasterCore());
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coreptr->pause(0ULL, 0ULL);
	coreptr->stop(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);

	// Shutdown from running
	coreptr.reset(new artdaq::RoutingMasterCore());
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);

	// Shutdown from pause
	coreptr.reset(new artdaq::RoutingMasterCore());
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coreptr->pause(0ULL, 0ULL);
	coreptr->shutdown(0ULL);
	coreptr.reset(nullptr);

	// Destruct from initialized
	coreptr.reset(new artdaq::RoutingMasterCore());
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr.reset(nullptr);

	// Destruct from running
	coreptr.reset(new artdaq::RoutingMasterCore());
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coreptr.reset(nullptr);

	// Destruct from paused
	coreptr.reset(new artdaq::RoutingMasterCore());
	coreptr->initialize(ps, 0ULL, 0ULL);
	coreptr->start(art::RunID(1), 0ULL, 0ULL);
	coreptr->pause(0ULL, 0ULL);
	coreptr.reset(nullptr);

	TLOG(TLVL_INFO) << "Test case StateMachine END";
}

BOOST_AUTO_TEST_CASE(SendTokens)
{
	TLOG(TLVL_INFO) << "Test case SendTokens START";
	fhicl::ParameterSet ps;
	fhicl::ParameterSet daq_ps;
	daq_ps.put<std::vector<int>>("sender_ranks", {5, 6, 7});

	fhicl::ParameterSet policy_ps;
	policy_ps.put<std::vector<int>>("receiver_ranks", {1, 2, 3, 4});
	policy_ps.put<std::string>("policy", "NoOp");
	daq_ps.put<fhicl::ParameterSet>("policy", policy_ps);
	ps.put<fhicl::ParameterSet>("daq", daq_ps);

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
	TLOG(TLVL_INFO) << "Test case SendTokens END";
}

BOOST_AUTO_TEST_SUITE_END()