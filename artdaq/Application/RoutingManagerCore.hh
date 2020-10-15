#ifndef artdaq_Application_MPI2_RoutingManagerCore_hh
#define artdaq_Application_MPI2_RoutingManagerCore_hh

#include <string>
#include <vector>

// Socket Includes
#include <netinet/in.h>
#include <sys/epoll.h>

#include "canvas/Persistency/Provenance/RunID.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-utilities/Plugins/MetricManager.hh"

#include "artdaq/DAQrate/StatisticsHelper.hh"
#include "artdaq/DAQrate/TokenReceiver.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/RoutingPolicies/RoutingManagerPolicy.hh"

namespace artdaq {
class RoutingManagerCore;
}

/**
* \brief RoutingManagerCore implements the state machine for the RoutingManager artdaq application.
* RoutingManagerCore collects tokens from receivers, and at regular intervals uses these tokens to build
* Routing Tables that are sent to the senders.
*/
class artdaq::RoutingManagerCore
{
public:
	static const std::string TABLE_UPDATES_STAT_KEY;    ///< Key for Table Update count MonnitoredQuantity
	static const std::string TOKENS_RECEIVED_STAT_KEY;  ///< Key for the Tokens Received MonitoredQuantity

	/**
	 * \brief RoutingManagerCore Constructor.
	 */
	RoutingManagerCore();

	/**
	* \brief Copy Constructor is deleted
	*/
	RoutingManagerCore(RoutingManagerCore const&) = delete;

	/**
	* Destructor.
	*/
	~RoutingManagerCore();

	/**
	* \brief Copy Assignment operator is deleted
	* \return RoutingManagerCore copy
	*/
	RoutingManagerCore& operator=(RoutingManagerCore const&) = delete;

	RoutingManagerCore(RoutingManagerCore&&) = delete;  ///< Move Constructor is deleted
	RoutingManagerCore& operator=(RoutingManagerCore&&) = delete;  ///< Move Assignment Operator is deleted

	/**
	* \brief Processes the initialize request.
	* \param pset ParameterSet used to configure the RoutingManagerCore
	* \return Whether the initialize attempt succeeded
	*
	* \verbatim
	* RoutingManagerCore accepts the following Parameters:
	* "daq" (REQUIRED): FHiCL table containing DAQ configuration
	*   "policy" (REQUIRED): FHiCL table containing the RoutingManagerPolicy configuration
	*     "policy" (Default: ""): Name of the RoutingManagerPolicy plugin to load
	*   "rt_priority" (Default: 0): Unix process priority to assign to RoutingManagerCore
	*   "sender_ranks" (REQUIRED): List of ranks (integers) for the senders (that receive table updates)
	*   "table_update_interval_ms" (Default: 1000): Maximum amount of time between table updates
	*   "senders_send_by_send_count" (Default: false): If true, senders will use the current send count to lookup routing information in the table, instead of sequence ID.
	*   "table_ack_retry_count" (Default: 5): The number of times the table will be resent while waiting for acknowledements
	*   "table_update_port" (Default: 35556): The port on which to send table updates
	*   "table_acknowledge_port" (Default: 35557): The port on which to listen for RoutingAckPacket datagrams
	*   "table_update_address" (Default: "227.128.12.28"): Multicast address to send table updates to
	*   "routing_manager_hostname" (Default: "localhost"): Hostname to send table updates from
	*   "metrics": FHiCL table containing configuration for MetricManager
	* \endverbatim
	*/
	bool initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t);

	/**
	* \brief Start the RoutingManagerCore
	* \param id Run ID of the current run
	* \return True if no exception
	*/
	bool start(art::RunID id, uint64_t, uint64_t);

	/**
	* \brief Stops the RoutingManagerCore
	* \return True if no exception
	*/
	bool stop(uint64_t, uint64_t);

	/**
	* \brief Pauses the RoutingManagerCore
	* \return True if no exception
	*/
	bool pause(uint64_t, uint64_t);

	/**
	* \brief Resumes the RoutingManagerCore
	* \return True if no exception
	*/
	bool resume(uint64_t, uint64_t);

	/**
	* \brief Shuts Down the RoutingManagerCore
	* \return If the shutdown was successful
	*/
	bool shutdown(uint64_t);

	/**
	* \brief Soft-Initializes the RoutingManagerCore.
	* \param pset ParameterSet for configuring RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Returns initialize status
	*/
	bool soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);

	/**
	* \brief Reinitializes the RoutingManagerCore.
	* \param pset ParameterSet for configuring RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Returns initialize status
	*/
	bool reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Main loop of the RoutingManagerCore. Determines when to send the next table update,
	 * asks the RoutingManagerPolicy for the table to send, and sends it.
	 */
	void process_event_table();

	/**
	 * \brief Sends a detail::RoutingPacket to the table receivers
	 * \param packet The detail::RoutingPacket to send
	 *
	 * send_event_table checks the table update socket and the acknowledge socket before
	 * sending the table update the first time. It then enters a loop where it sends the table
	 * update, then waits for acknowledgement packets. It keeps track of which senders have sent
	 * their acknowledgement packets, and discards duplicate acks. It leaves this loop once all
	 * senders have sent a valid acknowledgement packet.
	 */
	void send_event_table(detail::RoutingPacket packet);

	/**
	* \brief Send a report on the current status of the RoutingManagerCore
	* \return A string containing the report on the current status of the RoutingManagerCore
	*
	*/
	std::string report(std::string const&) const;

	/**
	* \brief Get the number of table updates sent by this RoutingManager this run
	* \return The number of table updates sent by this RoutingManager this run
	*/
	size_t get_update_count() const { return table_update_count_; }

private:
	art::RunID run_id_;

	fhicl::ParameterSet policy_pset_;
	fhicl::ParameterSet token_receiver_pset_;
	int rt_priority_;

	size_t max_table_update_interval_ms_;
	size_t max_ack_cycle_count_;
	detail::RoutingManagerMode routing_mode_;
	std::atomic<size_t> current_table_interval_ms_;
	std::atomic<size_t> table_update_count_;

	std::vector<int> sender_ranks_;
	std::set<int> active_ranks_;

	std::shared_ptr<RoutingManagerPolicy> policy_;
	std::unique_ptr<TokenReceiver> token_receiver_;

	std::atomic<bool> shutdown_requested_;
	std::atomic<bool> stop_requested_;
	std::atomic<bool> pause_requested_;

	// attributes and methods for statistics gathering & reporting
	std::shared_ptr<artdaq::StatisticsHelper> statsHelperPtr_;

	std::string buildStatisticsString_() const;

	void sendMetrics_();

	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended
	int send_tables_port_;
	int receive_acks_port_;
	std::string send_tables_address_;
	std::string multicast_out_hostname_;
	struct sockaddr_in send_tables_addr_;
	std::vector<epoll_event> receive_ack_events_;

	//Socket parameters
	int table_socket_{-1};
	int ack_socket_{-1};
	mutable std::mutex request_mutex_;
};

#endif /* artdaq_Application_MPI2_RoutingManagerCore_hh */
