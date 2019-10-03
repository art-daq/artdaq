#ifndef artdaq_Application_Routing_RoutingDestinationHelper_hh
#define artdaq_Application_Routing_RoutingDestinationHelper_hh

#include <deque>
#include <mutex>
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/RoutingPolicies/RoutingMasterPolicy.hh"

namespace artdaq {
/**
	 * \brief A helper class to determine individual routing destinations.
	 *
	 * This class is intended to be a helper in two ways: first, by providing
	 * a way to fetch individual Routing Destinations (rather than a full
	 * table), and second, by providing counts of available and maximum 
	 * destinations that are filtered by the routing policy in use.  (It's
	 * not much help if there are N open slots in receivers, if the
	 * routing policy is only advertizing a fraction of them.)
	 *
	 * This class is greedy in fetching new information from the policy.  
	 * That is, all operations first check to see if new routing information
	 * is available, and then they proceed from there.
	 */
class RoutingDestinationHelper
{
public:
	static int INVALID_DESTINATION;

	/**
		 * \brief RoutingDestinationHelper Constructor
		 */
	explicit RoutingDestinationHelper(std::shared_ptr<RoutingMasterPolicy> policy);

	/**
		 * \brief Default virtual Destructor
		 */
	virtual ~RoutingDestinationHelper() = default;

	/**
		 * \brief Get the next destination rank
		 * \return The rank of the receiver that should be sent the next fragment/event
		 */
	int GetNextDestinationRank();

	/**
		 * \brief Get the number of available destinations, which can be used to monitor
		 *        whether the system is getting backed up
		 * \return The number of destinations that are available to receive events
		 */
	size_t GetAvailableDestinationCount();

	/**
		 * \brief Get the maximum number of available destinations, which can be used
		 *        to monitor whether the system is getting backed up
		 * \return The maximum number of destinations that are available to receive events
		 */
	size_t GetMaximumDestinationCount();

private:
	void fetchAdditionalRoutingPackets();

	mutable std::mutex table_mutex_;
	std::shared_ptr<RoutingMasterPolicy> policy_;
	std::deque<detail::RoutingPacketEntry> working_table_;
	size_t max_destination_count_;
};
}  // namespace artdaq

#endif  // artdaq_Application_Routing_RoutingDestinationHelper_hh
