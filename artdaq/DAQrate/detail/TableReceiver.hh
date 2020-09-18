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
#include "artdaq/TransferPlugins/detail/HostMap.hh"
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
		///   "table_update_port" (Default: 35556): Port that table updates should arrive on
		fhicl::Atom<int> table_port{fhicl::Name{"table_update_port"}, fhicl::Comment{"Port that table updates should arrive on"}, 35556};
		///   "table_update_address" (Default: "227.128.12.28"): Address that table updates should arrive on
		fhicl::Atom<std::string> table_address{fhicl::Name{"table_update_address"}, fhicl::Comment{"Address that table updates should arrive on"}, "227.128.12.28"};
		///   "table_update_multicast_interface" (Default: "localhost"): Network interface that table updates should arrive on
		fhicl::Atom<std::string> table_multicast_interface{fhicl::Name{"table_update_multicast_interface"}, fhicl::Comment{"Network interface that table updates should arrive on"}, "localhost"};
		///   "table_acknowledge_port" (Default: 35557): Port that acknowledgements should be sent to
		fhicl::Atom<int> ack_port{fhicl::Name{"table_acknowledge_port"}, fhicl::Comment{"Port that acknowledgements should be sent to"}, 35557};
		///   "routing_manager_hostname" (Default: "localhost"): Host that acknowledgements should be sent to
		fhicl::Atom<std::string> ack_address{fhicl::Name{"routing_manager_hostname"}, fhicl::Comment{"Host that acknowledgements should be sent to"}, "localhost"};
		///   "routing_timeout_ms" (Default: 1000): Time to wait for a routing table update if the table is exhausted
		fhicl::Atom<int> routing_timeout_ms{fhicl::Name{"routing_timeout_ms"}, fhicl::Comment{"Time to wait (in ms) for a routing table update if the table is exhausted"}, 1000};
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
	int GetRoutingTableEntry(artdaq::Fragment::sequence_id_t seqID) const;

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

private:
	TableReceiver(TableReceiver const&) = delete;
	TableReceiver(TableReceiver&&) = delete;
	TableReceiver& operator=(TableReceiver const&) = delete;
	TableReceiver& operator=(TableReceiver&&) = delete;

	void setupTableListener_();

	void startTableReceiverThread_();

	void receiveTableUpdatesLoop_();

private:

	bool use_routing_manager_;
	detail::RoutingManagerMode routing_manager_mode_;
	std::atomic<bool> should_stop_;
	int table_port_;
	std::string table_address_;
	std::string table_multicast_interface_;
	int ack_port_;
	std::string ack_address_;
	struct sockaddr_in ack_addr_;
	int ack_socket_;
	int table_socket_;
	RoutingTable routing_table_;
	Fragment::sequence_id_t routing_table_last_;
	size_t routing_table_max_size_;
	mutable std::mutex routing_mutex_;
	boost::thread routing_thread_;
	mutable std::atomic<size_t> routing_wait_time_;
	mutable std::condition_variable routing_cv_;

	size_t routing_timeout_ms_;

	mutable std::atomic<uint64_t> highest_sequence_id_routed_;
};

#endif  //ARTDAQ_DAQRATE_DETAIL_TABLERECEIVER_HH
