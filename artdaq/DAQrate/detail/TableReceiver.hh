#ifndef ARTDAQ_DAQRATE_DETAIL_TABLERECEIVER_HH
#define ARTDAQ_DAQRATE_DETAIL_TABLERECEIVER_HH

#include <netinet/in.h>
#include <map>
#include <memory>
#include <set>

#include "fhiclcpp/fwd.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/OptionalTable.h"
#include "fhiclcpp/types/TableFragment.h"

namespace artdaq {
class TableReceiver;
}

/**
 * \brief Sends Fragment objects using TransferInterface plugins. Uses Routing Tables if confgiured,
 * otherwise will Round-Robin Fragments to the destinations.
 */
class artdaq::TableReceiver
{
public:
	/// <summary>
	/// Configuration for Routing table reception
	///
	/// This configuration should be the same for all processes receiving routing tables from a given RoutingManager.
	/// </summary>
	struct Config
	{
		///   "use_routing_manager" (Default: false): True if using the Routing Manager
		fhicl::Atom<bool> use_routing_manager{fhicl::Name{"use_routing_manager"}, fhicl::Comment{"True if using the Routing Manager"}, false};
		///   "route_on_request_mode" (Default: false): True if a request for routing information should be sent to the RoutingManager (versus RoutingManager pushing table updates).
		fhicl::Atom<bool> route_on_request_mode{fhicl::Name{"route_on_request_mode"}, fhicl::Comment{"True if a request for routing information should be sent to the RoutingManager (versus RoutingManager pushing table updates)."}, false};
		///   "use_routing_table_thread" (Default: true): True if a thread should be run to receive routing updates. Required if route_on_request_mode is false.
		fhicl::Atom<bool> use_routing_table_thread{fhicl::Name{"use_routing_table_thread"}, fhicl::Comment{"True if a thread should be run to receive routing updates. Required if route_on_request_mode is false."}, true};
		///   "table_update_port" (Default: 35556): Port to connect to for receiving table updates
		fhicl::Atom<int> table_port{fhicl::Name{"table_update_port"}, fhicl::Comment{"Port to connect to for receiving table updates"}, 35556};
		///   "routing_manager_hostname" (Default: "localhost"): RoutingManager hostname for Table connection
		fhicl::Atom<std::string> routing_manager_hostname{fhicl::Name{"routing_manager_hostname"}, fhicl::Comment{"outingManager hostname for Table connection"}, "localhost"};
		///   "routing_timeout_ms" (Default: 1000): Time to wait for a routing table update
		fhicl::Atom<int> routing_timeout_ms{fhicl::Name{"routing_timeout_ms"}, fhicl::Comment{"Time to wait (in ms) for a routing table update"}, 1000};
		///   "routing_table_max_size" (Default: 1000): Maximum number of entries in the routing table
		fhicl::Atom<size_t> routing_table_max_size{fhicl::Name{"routing_table_max_size"}, fhicl::Comment{"Maximum number of entries in the routing table"}, 1000};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	using RoutingTable = std::map<artdaq::Fragment::sequence_id_t, int>;

	static constexpr int ROUTING_FAILED = -1111;

	/**
	 * \brief TableReceiver Constructor
	 * \param ps ParameterSet used to configure the TableReceiver. See artdaq::TableReceiver::Config
	 */
	explicit TableReceiver(const fhicl::ParameterSet& ps);

	/**
	 * \brief TableReceiver Destructor
	 */
	virtual ~TableReceiver();

	RoutingTable GetRoutingTable() const;
	RoutingTable GetAndClearRoutingTable();
	int GetRoutingTableEntry(artdaq::Fragment::sequence_id_t seqID);

	/**
	 * \brief Gets the current size of the Routing Table, in case other parts of the system want to use this information
	 * \return The current size of the Routing Table.
	 */
	size_t GetRoutingTableEntryCount() const;

	/**
	 * \brief Gets the number of sends remaining in the routing table, in case other parts of the system want to use this information
	 * \return The number of sends remaining in the routing table
	 */
	size_t GetRemainingRoutingTableEntries() const;

	/**
	 * \brief Stop the TableReceiver
	 */
	void StopTableReceiver() { should_stop_ = true; }

	/**
	 * \brief Remove the given sequence ID from the routing table and sent_count lists
	 * \param seq Sequence ID to remove
	 */
	void RemoveRoutingTableEntry(Fragment::sequence_id_t seq);

	void SendMetrics() const;

	bool RoutingManagerEnabled() const { return use_routing_manager_; }

private:
	TableReceiver(TableReceiver const&) = delete;
	TableReceiver(TableReceiver&&) = delete;
	TableReceiver& operator=(TableReceiver const&) = delete;
	TableReceiver& operator=(TableReceiver&&) = delete;

	void connectToRoutingManager_();
	void disconnectFromRoutingManager_();

	void startTableReceiverThread_();

	bool receiveTableUpdate_();
	void receiveTableUpdatesLoop_();

	int sendTableUpdateRequest_(Fragment::sequence_id_t seq);

private:
	bool use_routing_manager_;
	bool route_on_request_;
	std::atomic<bool> should_stop_;
	int table_port_;
	std::string table_address_;
	int table_socket_;
	RoutingTable routing_table_;
	Fragment::sequence_id_t routing_table_last_;
	size_t routing_table_max_size_;
	mutable std::mutex routing_mutex_;
	std::unique_ptr<boost::thread> routing_thread_;
	mutable std::atomic<size_t> routing_wait_time_;
	mutable std::condition_variable routing_cv_;

	size_t routing_timeout_ms_;

	mutable std::atomic<uint64_t> highest_sequence_id_routed_;
};

#endif  //ARTDAQ_DAQRATE_DETAIL_TABLERECEIVER_HH
