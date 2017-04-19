#ifndef ARTDAQ_DAQRATE_DATASENDERMANAGER_HH
#define ARTDAQ_DAQRATE_DATASENDERMANAGER_HH

#include <map>
#include <set>
#include <memory>
#include <netinet/in.h>

#include <fhiclcpp/fwd.h>

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"

namespace artdaq
{
	class DataSenderManager;
}

class artdaq::DataSenderManager
{
public:

	DataSenderManager(fhicl::ParameterSet);

	~DataSenderManager();

	// Send the given Fragment. Return the rank of the destination to which
	// the Fragment was sent.
	int sendFragment(Fragment&&);

	// How many fragments have been sent using this DataSenderManager object?
	size_t count() const;

	// How many fragments have been sent to a particular destination.
	size_t slotCount(size_t rank) const;

	size_t destinationCount() const { return destinations_.size(); }

	std::set<int> enabled_destinations() const { return enabled_destinations_; }
private:

	// Calculate where the fragment with this sequenceID should go.
	int calcDest(Fragment::sequence_id_t) const;

	void setupTableListener();

	void startTableReceiverThread();

	void receiveTableUpdatesLoop();
private:

	std::map<int, std::unique_ptr<artdaq::TransferInterface>> destinations_;
	std::set<int> enabled_destinations_;

	detail::FragCounter sent_frag_count_;

	bool broadcast_sends_;

	bool use_routing_master_;
	std::atomic<bool> should_stop_;
	int table_port_;
	std::string table_address_;
	struct sockaddr_in table_addr_;
	int ack_port_;
	std::string ack_address_;
	struct sockaddr_in ack_addr_;
	int ack_socket_;
	int table_socket_;
	int table_epoll_fd_;
	std::map<Fragment::sequence_id_t, int> routing_table_;
	mutable std::mutex routing_mutex_;
	std::thread routing_thread_;

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
