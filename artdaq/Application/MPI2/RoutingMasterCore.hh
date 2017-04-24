#ifndef artdaq_Application_MPI2_RoutingMasterCore_hh
#define artdaq_Application_MPI2_RoutingMasterCore_hh

#include <string>
#include <vector>
#include <iostream>

// Socket Includes
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>

#include "artdaq/Application/Commandable.hh"
#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/Application/MPI2/StatisticsHelper.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq/Application/Routing/RoutingMasterPolicy.hh"

namespace artdaq
{
	class RoutingMasterCore;
}

class artdaq::RoutingMasterCore
{
public:
	static const std::string TABLE_UPDATES_STAT_KEY;
	static const std::string TOKENS_RECEIVED_STAT_KEY;

	RoutingMasterCore(Commandable& parent_application, MPI_Comm local_group_comm, std::string name);

	RoutingMasterCore(RoutingMasterCore const&) = delete;

	~RoutingMasterCore();

	RoutingMasterCore& operator=(RoutingMasterCore const&) = delete;

	bool initialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

	bool start(art::RunID, uint64_t, uint64_t);

	bool stop(uint64_t, uint64_t);

	bool pause(uint64_t, uint64_t);

	bool resume(uint64_t, uint64_t);

	bool shutdown(uint64_t);

	bool soft_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

	bool reinitialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

	size_t process_event_table();
	void send_event_table(detail::RoutingPacket table, std::vector<bool> acks, int level = 0);

	std::string report(std::string const&) const;

private:
	void receive_tokens_();
	void start_recieve_token_thread_();

	Commandable& parent_application_;
	MPI_Comm local_group_comm_;
	art::RunID run_id_;
	std::string name_;

	fhicl::ParameterSet policy_pset_;
	int rt_priority_;

	size_t table_update_interval_ms_;
	size_t table_ack_wait_time_ms_;
	std::atomic<size_t> table_update_count_;
	std::atomic<size_t> received_token_count_;

	std::vector<int> br_ranks_;
	size_t num_ebs_;

	std::unique_ptr<RoutingMasterPolicy> policy_;

	std::atomic<bool> stop_requested_;
	std::atomic<bool> pause_requested_;

	// attributes and methods for statistics gathering & reporting
	artdaq::StatisticsHelper statsHelper_;

	std::string buildStatisticsString_() const;

	artdaq::MetricManager metricMan_;

	void sendMetrics_();


	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended
	int receive_token_port_;
	int send_tables_port_;
	int receive_acks_port_;
	std::string send_tables_address_;
	std::string receive_address_;
	struct sockaddr_in receive_addr_;
	struct sockaddr_in send_tables_addr_;
	std::vector<epoll_event> receive_ack_events_;
	std::vector<epoll_event> receive_token_events_;
	std::unordered_map<int, std::string> receive_token_addrs_;
	int token_epoll_fd_;

	//Socket parameters
	int token_socket_;
	int table_socket_;
	int ack_socket_;
	mutable std::mutex request_mutex_;
	std::thread ev_token_receive_thread_;

};

#endif /* artdaq_Application_MPI2_RoutingMasterCore_hh */
