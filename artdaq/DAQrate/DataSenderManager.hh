#ifndef ARTDAQ_DAQRATE_DATASENDERMANAGER_HH
#define ARTDAQ_DAQRATE_DATASENDERMANAGER_HH

#include <map>
#include <set>
#include <memory>

#include <fhiclcpp/fwd.h>

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"

namespace artdaq {
	class DataSenderManager;
}

class artdaq::DataSenderManager {
public:

	DataSenderManager(fhicl::ParameterSet);
	~DataSenderManager();

	// Send the given Fragment. Return the rank of the destination to which
	// the Fragment was sent.
	int sendFragment(Fragment &&);

	// How many fragments have been sent using this DataSenderManager object?
	size_t count() const;

	// How many fragments have been sent to a particular destination.
	size_t slotCount(size_t rank) const;

	size_t destinationCount() const { return destinations_.size(); }

	std::set<int> enabled_destinations() const { return enabled_destinations_; }
private:
	// Send an EOF Fragment to the receiver at rank dest;
	// the EOF Fragment will report that numFragmentsSent
	// fragments have been sent before this one.
	void sendEODFrag(int dest, size_t numFragmentsSent);

	// Calculate where the fragment with this sequenceID should go.
	int calcDest(Fragment::sequence_id_t) const;

private:

	std::map<int, std::unique_ptr<artdaq::TransferInterface>> destinations_;
	std::set<int> enabled_destinations_;

	detail::FragCounter sent_frag_count_;

	bool broadcast_sends_;
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