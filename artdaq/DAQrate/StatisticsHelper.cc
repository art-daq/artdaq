#include "artdaq/DAQrate/StatisticsHelper.hh"

// This class is really nothing more than a collection of code that
// would be repeated throughout artdaq "application" classes if it
// weren't centralized here.  So, we should be careful not to put
// too much intelligence in this class.  (KAB, 07-Jan-2015)

artdaq::StatisticsHelper::
    StatisticsHelper()
    : monitored_quantity_name_list_(0)
    , primary_stat_ptr_(nullptr) {}

void artdaq::StatisticsHelper::
    addMonitoredQuantityName(std::string const& statKey)
{
	monitored_quantity_name_list_.push_back(statKey);
}

void artdaq::StatisticsHelper::addSample(std::string const& statKey,
                                         double value) const
{
	artdaq::MonitoredQuantityPtr mqPtr =
	    artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(statKey);
	if (mqPtr.get() != nullptr) { mqPtr->addSample(value); }
}

bool artdaq::StatisticsHelper::
    createCollectors(fhicl::ParameterSet const& pset, int defaultReportIntervalFragments,
                     double defaultReportIntervalSeconds, double defaultMonitorWindow,
                     std::string const& primaryStatKeyName)
{
	reporting_interval_fragments_ =
	    pset.get<int>("reporting_interval_fragments", defaultReportIntervalFragments);
	reporting_interval_seconds_ =
	    pset.get<double>("reporting_interval_seconds", defaultReportIntervalSeconds);

	auto monitorWindow = pset.get<double>("monitor_window", defaultMonitorWindow);
	auto monitorBinSize =
	    pset.get<double>("monitor_binsize",
	                     1.0 + static_cast<int>((monitorWindow - 1) / 100.0));

	if (monitorBinSize < 1.0) { monitorBinSize = 1.0; }
	if (monitorWindow >= 1.0)
	{
		for (const auto & idx : monitored_quantity_name_list_)
		{
			artdaq::MonitoredQuantityPtr
			    mqPtr(new artdaq::MonitoredQuantity(monitorBinSize,
			                                        monitorWindow));
			artdaq::StatisticsCollection::getInstance().addMonitoredQuantity(idx, mqPtr);
		}
	}

	primary_stat_ptr_ = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(primaryStatKeyName);
	return (primary_stat_ptr_.get() != nullptr);
}

void artdaq::StatisticsHelper::resetStatistics()
{
	previous_reporting_index_ = 0;
	previous_stats_calc_time_ = 0.0;
	for (const auto & idx : monitored_quantity_name_list_)
	{
		artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(idx);
		if (mqPtr.get() != nullptr) { mqPtr->reset(); }
	}
}

bool artdaq::StatisticsHelper::
    readyToReport()
{
	if (primary_stat_ptr_.get() != nullptr)
	{
		double fullDuration = primary_stat_ptr_->getFullDuration();
		auto reportIndex = static_cast<size_t>(fullDuration / reporting_interval_seconds_);
		if (reportIndex > previous_reporting_index_)
		{
			previous_reporting_index_ = reportIndex;
			return true;
		}
	}

	return false;
}

bool artdaq::StatisticsHelper::statsRollingWindowHasMoved()
{
	if (primary_stat_ptr_.get() != nullptr)
	{
		auto lastCalcTime = primary_stat_ptr_->getLastCalculationTime();
		if (lastCalcTime > previous_stats_calc_time_)
		{
			auto now = MonitoredQuantity::getCurrentTime();
			previous_stats_calc_time_ = std::min(lastCalcTime, now);
			return true;
		}
	}

	return false;
}
