#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RoutingDestinationHelper").c_str()
#include "artdaq/RoutingPolicies/RoutingDestinationHelper.hh"

int artdaq::RoutingDestinationHelper::INVALID_DESTINATION = -1;

artdaq::RoutingDestinationHelper::RoutingDestinationHelper(std::shared_ptr<RoutingMasterPolicy> policy)
    : policy_(policy)
    , working_table_(0)
    , max_destination_count_(0)
{
	if (policy_.get() == nullptr)
	{
		TLOG(TLVL_ERROR) << "NULL RoutingMasterPolicy pointer passed to RoutingDestinationHelper constructor!";
	}
}

int artdaq::RoutingDestinationHelper::GetNextDestinationRank()
{
	std::unique_lock<std::mutex> lk(table_mutex_);
	fetchAdditionalRoutingPackets();

	if (working_table_.size() == 0)
	{
		TLOG(11) << "No Destinations Available!";
		return INVALID_DESTINATION;
	}

	detail::RoutingPacketEntry packet_entry = working_table_.front();
	working_table_.pop_front();
	TLOG(TLVL_TRACE) << "Returning rank " << packet_entry.destination_rank << " as the next destination rank.";
	return packet_entry.destination_rank;
}

size_t artdaq::RoutingDestinationHelper::GetAvailableDestinationCount()
{
	std::unique_lock<std::mutex> lk(table_mutex_);
	fetchAdditionalRoutingPackets();
	size_t count = working_table_.size();
	TLOG(12) << "Available destination count is " << count;
	return count;
}

size_t artdaq::RoutingDestinationHelper::GetMaximumDestinationCount()
{
	std::unique_lock<std::mutex> lk(table_mutex_);
	fetchAdditionalRoutingPackets();
	TLOG(12) << "Maximum destination count is " << max_destination_count_;
	return max_destination_count_;
}

void artdaq::RoutingDestinationHelper::fetchAdditionalRoutingPackets()
{
	// we trust the public methods to obtain the necessary locks
	size_t initial_wt_size = working_table_.size();
	TLOG(10) << "Initial working_table_ size is " << initial_wt_size;

	if (policy_.get() != nullptr)
	{
		detail::RoutingPacket packet = policy_->GetCurrentTable();
		if (packet.size() > 0)
		{
			working_table_.resize(initial_wt_size + packet.size());
			std::copy(packet.begin(), packet.end(), working_table_.begin() + initial_wt_size);
		}
	}
	else
	{
		TLOG(TLVL_WARNING) << "NULL RoutingMasterPolicy; skipping the fetch of the current routing table.!";
	}

	if (working_table_.size() > max_destination_count_) { max_destination_count_ = working_table_.size(); }
	TLOG(10) << "Updated our working table, the new table has " << working_table_.size() << " entries.";
}
