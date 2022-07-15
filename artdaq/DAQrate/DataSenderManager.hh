#ifndef ARTDAQ_DAQRATE_DATASENDERMANAGER_HH
#define ARTDAQ_DAQRATE_DATASENDERMANAGER_HH

#include "fhiclcpp/types/Sequence.h"  // Must pre-empt fhiclcpp/types/Atom.h

#include "TRACE/tracemf.h"  // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"

#include "artdaq/DAQdata/HostMap.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq/DAQrate/detail/TableReceiver.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Comment.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/Name.h"
#include "fhiclcpp/types/OptionalTable.h"
#include "fhiclcpp/types/TableFragment.h"

#include <netinet/in.h>
#include <atomic>
#include <map>
#include <memory>
#include <set>

namespace artdaq {
class DataSenderManager;
}

/**
 * \brief Sends Fragment objects using TransferInterface plugins. Uses Routing Tables if confgiured,
 * otherwise will Round-Robin Fragments to the destinations.
 */
class artdaq::DataSenderManager
{
public:
	static constexpr int NO_RANK_INFO = -1;  ///< Value to indicate that destination rank info is not available

	/// <summary>
	/// Configuration for transfers to destinations
	/// </summary>
	struct DestinationsConfig
	{
		/// Example Configuration for transfer to destination. See artdaq::TransferInterface::Config
		fhicl::OptionalTable<artdaq::TransferInterface::Config> dest{fhicl::Name{"d1"}, fhicl::Comment{"Configuration for transfer to destination"}};
	};

	/// <summary>
	/// Configuration of DataSenderManager. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "broadcast_sends" (Default: false): Send all Fragments to all destinations
		fhicl::Atom<bool> broadcast_sends{fhicl::Name{"broadcast_sends"}, fhicl::Comment{"Send all Fragments to all destinations"}, false};
		/// "nonblocking_sends" (Default: false): If true, will use non-reliable mode of TransferInterface plugins
		fhicl::Atom<bool> nonblocking_sends{fhicl::Name{"nonblocking_sends"}, fhicl::Comment{"Whether sends should block. Used for DL->DISP connection."}, false};
		/// "send_timeout_usec" (Default: 5000000 (5 seconds): Timeout for sends in non-reliable modes (broadcast and nonblocking)
		fhicl::Atom<size_t> send_timeout_us{fhicl::Name{"send_timeout_usec"}, fhicl::Comment{"Timeout for sends in non-reliable modes (broadcast and nonblocking)"}, 5000000};
		/// "send_retry_count" (Default: 2): Number of times to retry a send in non-reliable mode
		fhicl::Atom<size_t> send_retry_count{fhicl::Name{"send_retry_count"}, fhicl::Comment{"Number of times to retry a send in non-reliable mode"}, 2};
		fhicl::OptionalTable<artdaq::TableReceiver::Config> routing_table_config{fhicl::Name{"routing_table_config"}};  ///< Configuration for Routing Table reception. See artdaq::DataSenderManager::RoutingTableConfig
		/// "destinations" (Default: Empty ParameterSet): FHiCL table for TransferInterface configurations for each destaintion. See artdaq::DataSenderManager::DestinationsConfig
		///   NOTE: "destination_rank" MUST be specified (and unique) for each destination!
		fhicl::OptionalTable<DestinationsConfig> destinations{fhicl::Name{"destinations"}};
		/// Optional host_map configuration (Can also be specified in each DestinationsConfig entry. See artdaq::HostMap::Config
		fhicl::TableFragment<artdaq::HostMap::Config> host_map;
		/// enabled_destinations" (OPTIONAL): If specified, only the destination ranks listed will be enabled. If not specified, all destinations will be enabled.
		fhicl::Sequence<size_t> enabled_destinations{fhicl::Name{"enabled_destinations"}, fhicl::Comment{"List of destiantion ranks to activate (must be defined in destinations block)"}, std::vector<size_t>()};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
	 * \brief DataSenderManager Constructor
	 * \param ps ParameterSet used to configure the DataSenderManager. See artdaq::DataSenderManager::Config
	 */
	explicit DataSenderManager(const fhicl::ParameterSet& ps);

	/**
	 * \brief DataSenderManager Destructor
	 */
	virtual ~DataSenderManager();

	/**
	 * \brief Send the given Fragment. Return the rank of the destination to which the Fragment was sent.
	 * \param frag Fragment to sent
	 * \param dest_rank Rank Fragment should be sent to. NO_RANK_INFO (default) indicates that DataSenderManager should determine destination
	 * \return Pair containing Rank of destination for Fragment and the CopyStatus from the send call
	 */
	std::pair<int, TransferInterface::CopyStatus> sendFragment(Fragment&& frag, int dest = NO_RANK_INFO);

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
	 * \return The current size of the Routing Table.
	 */
	size_t GetRoutingTableEntryCount() const;

	/**
	 * \brief Gets the number of sends remaining in the routing table, in case other parts of the system want to use this information
	 * \return The number of sends remaining in the routing table
	 */
	size_t GetRemainingRoutingTableEntries() const;

	/**
	 * \brief Stop the DataSenderManager, aborting any sends in progress
	 */
	void StopSender() { should_stop_ = true; }

	/**
	 * \brief Remove the given sequence ID from the routing table and sent_count lists
	 * \param seq Sequence ID to remove
	 */
	void RemoveRoutingTableEntry(Fragment::sequence_id_t seq);
	/**
	 * \brief Get the number of Fragments sent with a given Sequence ID
	 * \param seq Sequence ID to query
	 * \return The number of Fragments sent with a given Sequence ID
	 */
	size_t GetSentSequenceIDCount(Fragment::sequence_id_t seq);

private:
	DataSenderManager(DataSenderManager const&) = delete;
	DataSenderManager(DataSenderManager&&) = delete;
	DataSenderManager& operator=(DataSenderManager const&) = delete;
	DataSenderManager& operator=(DataSenderManager&&) = delete;

	// Calculate where the fragment with this sequenceID should go.
	int calcDest_(Fragment::sequence_id_t) const;

private:
	std::map<int, std::unique_ptr<artdaq::TransferInterface>> destinations_;
	std::set<int> enabled_destinations_;

	detail::FragCounter sent_frag_count_;

	bool broadcast_sends_;
	bool non_blocking_mode_;
	size_t send_timeout_us_;
	size_t send_retry_count_;

	std::unique_ptr<TableReceiver> table_receiver_;
	std::atomic<bool> should_stop_;
	std::map<Fragment::sequence_id_t, size_t> sent_sequence_id_count_;

	mutable std::mutex sent_sequence_id_mutex_;

	mutable std::atomic<uint64_t> highest_sequence_id_routed_;
};

inline size_t
artdaq::DataSenderManager::
    count() const
{
	return sent_frag_count_.count();
}

inline size_t
artdaq::DataSenderManager::
    slotCount(size_t rank) const
{
	return sent_frag_count_.slotCount(rank);
}
#endif  // ARTDAQ_DAQRATE_DATASENDERMANAGER_HH
