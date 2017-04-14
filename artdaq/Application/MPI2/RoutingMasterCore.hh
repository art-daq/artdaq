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
	static const std::string INPUT_WAIT_STAT_KEY;
	static const std::string ACK_WAIT_STAT_KEY;
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
	void send_event_table(detail::RoutingPacket table);
	void receive_tokens();
	void start_recieve_token_thread();

	std::string report(std::string const&) const;

private:
	Commandable& parent_application_;
	MPI_Comm local_group_comm_;
	art::RunID run_id_;
	std::string name_;

	fhicl::ParameterSet policy_pset_;
	int rt_priority_;

	size_t table_update_interval_ms_;
	std::atomic<size_t> table_update_count_;
	std::atomic<size_t> received_token_count_;

	std::vector<int> br_ranks_;

	std::unique_ptr<RoutingMasterPolicy> policy_;

	std::atomic<bool> stop_requested_;
	std::atomic<bool> pause_requested_;

	// attributes and methods for statistics gathering & reporting
	artdaq::StatisticsHelper statsHelper_;

	std::string buildStatisticsString_();

	artdaq::MetricManager metricMan_;

	void sendMetrics_();


	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended
	int request_port_;
	std::string request_addr_;

	//Socket parameters
	struct sockaddr_in si_data_;
	int token_socket_;
	int table_socket_;
	int ack_socket_;
	std::mutex request_mutex_;
	std::thread ev_token_receive_thread_;

};

#endif /* artdaq_Application_MPI2_RoutingMasterCore_hh */
