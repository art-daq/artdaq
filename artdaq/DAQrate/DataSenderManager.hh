#ifndef ARTDAQ_DAQRATE_DATASENDERMANAGER_HH
#define ARTDAQ_DAQRATE_DATASENDERMANAGER_HH

#include <map>
#include <set>
#include <memory>
#include <netinet/in.h>

#include "fhiclcpp/fwd.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"

namespace artdaq
{
	class DataSenderManager;
}

/**
 * \brief Sends Fragment objects using TransferInterface plugins. Uses Routing Tables if confgiured,
 * otherwise will Round-Robin Fragments to the destinations.
 */
class artdaq::DataSenderManager
{
public:

	/**
	 * \brief DataSenderManager Constructor
	 * \param ps ParameterSet used to configure the DataSenderManager
	 * 
	 * \verbatim
	 * DataSenderManager accepts the following Parameters:
	 * "broadcast_sends" (Default: false): Send all Fragments to all destinations
	 * "nonblocking_sends" (Default: false): If true, will use non-reliable mode of TransferInterface plugins
	 * "routing_table_config" (Default: Empty ParameterSet): FHiCL table for RoutingMaster parameters
	 *   "use_routing_master" (Default: false): True if using the Routing Master
	 *   "table_update_port" (Default: 35556): Port that table updates should arrive on
	 *   "table_update_address" (Default: "227.128.12.28"): Address that table updates should arrive on
	 *   "table_acknowledge_port" (Default: 35557): Port that acknowledgements should be sent to
	 *   "routing_master_hostname" (Default: "localhost"): Host that acknowledgements should be sent to
	 *   "routing_timeout_ms" (Default: 1000): Time to wait for a routing table update if the table is exhausted
	 * "destinations" (Default: Empty ParameterSet): FHiCL table for TransferInterface configurations for each destaintion
	 *   NOTE: "destination_rank" MUST be specified (and unique) for each destination!
	 * "enabled_destinations" (OPTIONAL): If specified, only the destination ranks listed will be enabled. If not specified,
	 * all destinations will be enabled.
	 * \endverbatim
	 */
	explicit DataSenderManager(const fhicl::ParameterSet& ps);

	/**
	 * \brief DataSenderManager Destructor
	 */
	virtual ~DataSenderManager();

	/**
	 * \brief Send the given Fragment. Return the rank of the destination to which the Fragment was sent.
	 * \param frag Fragment to sent
	 * \return Rank of destination for Fragment
	 */
	int sendFragment(Fragment&& frag);

	/**
	* \brief Return the count of Fragment objects sent by this DataSenderManagerq
	* \return The count of Fragment objects sent by this DataSenderManager
	*/
	size_t count() const;

	/**
	* \brief Get the count of Fragment objects sent by this DataSenderManager to a given destination
	* \param rank Destination rank to get count for
	* \return The  count of Fragment objects sent by this DataSenderManager to the destination
	*/
	size_t slotCount(size_t rank) const;

	/**
	 * \brief Get the number of configured destinations
	 * \return The number of configured destinations
	 */
	size_t destinationCount() const { return destinations_.size(); }

	/**
	 * \brief Get the list of enabled destinations
	 * \return The list of enabled destiantion ranks
	 */
	std::set<int> enabled_destinations() const { return enabled_destinations_; }

	/**
	 * \brief Gets the current size of the Routing Table, in case other parts of the system want to use this information
	 * \return The current size of the Routing Table. Note that the Routing Table is trimmed after each successful send.
	 */
	size_t GetRoutingTableEntryCount() const;
private:

	// Calculate where the fragment with this sequenceID should go.
	int calcDest_(Fragment::sequence_id_t) const;

	void setupTableListener_();

	void startTableReceiverThread_();

	void receiveTableUpdatesLoop_();
private:

	std::map<int, std::unique_ptr<artdaq::TransferInterface>> destinations_;
	std::set<int> enabled_destinations_;

	detail::FragCounter sent_frag_count_;

	bool broadcast_sends_;
	bool non_blocking_mode_;
	size_t send_timeout_us_;

	bool use_routing_master_;
	detail::RoutingMasterMode routing_master_mode_;
	std::atomic<bool> should_stop_;
	int table_port_;
	std::string table_address_;
	struct sockaddr_in table_addr_;
	int ack_port_;
	std::string ack_address_;
	struct sockaddr_in ack_addr_;
	int ack_socket_;
	int table_socket_;
	std::map<Fragment::sequence_id_t, int> routing_table_;
	mutable std::mutex routing_mutex_;
	boost::thread routing_thread_;
	mutable std::atomic<size_t> routing_wait_time_;

	int routing_timeout_ms_;
};

inline
size_t
artdaq::DataSenderManager::
count() const
{
	return sent_frag_count_.count();
}

inline
size_t
artdaq::DataSenderManager::
slotCount(size_t rank) const
{
	return sent_frag_count_.slotCount(rank);
}
#endif //ARTDAQ_DAQRATE_DATASENDERMANAGER_HH
