#include "artdaq/Application/MPI2/StatisticsHelper.hh"

artdaq::StatisticsHelper::
StatisticsHelper() : monitored_quantity_name_list_(0)
{
}

artdaq::StatisticsHelper::~StatisticsHelper()
{
}

void artdaq::StatisticsHelper::
addMonitoredQuantityName(std::string const& statKey)
{
  monitored_quantity_name_list_.push_back(statKey);
}

void artdaq::StatisticsHelper::addSample(std::string const& statKey,
			                double value)
{
  artdaq::MonitoredQuantityPtr mqPtr =
    artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(statKey);
  if (mqPtr.get() != 0) {mqPtr->addSample(value);}
}

void artdaq::StatisticsHelper::
createCollectors(fhicl::ParameterSet const& pset, int defaultReportIntervalFragments,
                 double defaultReportIntervalSeconds, double defaultMonitorWindow)
{
  reporting_interval_fragments_ =
    pset.get<int>("reporting_interval_fragments", defaultReportIntervalFragments);
  reporting_interval_seconds_ =
    pset.get<double>("reporting_interval_seconds", defaultReportIntervalSeconds);

  double monitorWindow = pset.get<double>("monitor_window", defaultMonitorWindow);
  double monitorBinSize =
    pset.get<double>("monitor_binsize",
                     1.0 + ((int) ((monitorWindow-1) / 100.0)));

  if (monitorBinSize < 1.0) {monitorBinSize = 1.0;}
  if (monitorWindow >= 1.0) {
    for (size_t idx = 0; idx < monitored_quantity_name_list_.size(); ++idx) {
      artdaq::MonitoredQuantityPtr
        mqPtr(new artdaq::MonitoredQuantity(monitorBinSize,
                                            monitorWindow));
      artdaq::StatisticsCollection::getInstance().
        addMonitoredQuantity(monitored_quantity_name_list_[idx], mqPtr);
    }
  }
}

void artdaq::StatisticsHelper::resetStatistics()
{
  previous_reporting_index_ = 0;
  for (size_t idx = 0; idx < monitored_quantity_name_list_.size(); ++idx) {
    artdaq::MonitoredQuantityPtr mqPtr = artdaq::StatisticsCollection::getInstance().
      getMonitoredQuantity(monitored_quantity_name_list_[idx]);
    if (mqPtr.get() != 0) {mqPtr->reset();}
  }
}

bool artdaq::StatisticsHelper::
readyToReport(std::string const& primaryStatKeyName, size_t currentCount)
{
  artdaq::MonitoredQuantityPtr mqPtr =
    artdaq::StatisticsCollection::getInstance().getMonitoredQuantity(primaryStatKeyName);

  if (mqPtr.get() != 0 && (currentCount % reporting_interval_fragments_) == 0) {
    artdaq::MonitoredQuantity::Stats stats;
    mqPtr->getStats(stats);
    size_t reportIndex = (size_t) (stats.fullDuration / reporting_interval_seconds_);
    if (reportIndex > previous_reporting_index_) {
      previous_reporting_index_ = reportIndex;
      return true;
    }
  }

  return false;
}
