#ifndef artdaq_Application_MPI2_StatisticsHelper_hh
#define artdaq_Application_MPI2_StatisticsHelper_hh

#include "artdaq-core/Core/StatisticsCollection.hh"
#include "fhiclcpp/ParameterSet.h"
#include <vector>

namespace artdaq
{
	class StatisticsHelper;
}

class artdaq::StatisticsHelper
{
public:
	StatisticsHelper();

	StatisticsHelper(StatisticsHelper const&) = delete;

	~StatisticsHelper();

	StatisticsHelper& operator=(StatisticsHelper const&) = delete;

	void addMonitoredQuantityName(std::string const& statKey);

	void addSample(std::string const& statKey, double value) const;

	bool createCollectors(fhicl::ParameterSet const& pset,
	                      int defaultReportIntervalFragments,
	                      double defaultReportIntervalSeconds,
	                      double defaultMonitorWindow,
	                      std::string const& primaryStatKeyName);

	void resetStatistics();

	bool readyToReport(size_t currentCount);

	bool statsRollingWindowHasMoved();

private:
	std::vector<std::string> monitored_quantity_name_list_;
	artdaq::MonitoredQuantityPtr primary_stat_ptr_;

	int reporting_interval_fragments_;
	double reporting_interval_seconds_;
	size_t previous_reporting_index_;
	MonitoredQuantityStats::TIME_POINT_T previous_stats_calc_time_;
};

#endif /* artdaq_Application_MPI2_StatisticsHelper_hh */
