////////////////////////////////////////////////////////////////////////
// Class:       FragmentWatcher
// Module Type: analyzer
// File:        FragmentWatcher_module.cc
// Description: Collects and reports statistics on missing and empty fragments
////////////////////////////////////////////////////////////////////////

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq/DAQdata/Globals.hh"

#include "messagefacility/MessageLogger/MessageLogger.h"

#include <iostream>
#include <bitset>

#define TRACE_NAME (app_name + "_FragmentWatcher").c_str()

namespace artdaq
{
  class FragmentWatcher;
}

class artdaq::FragmentWatcher : public art::EDAnalyzer
{
public:
  explicit FragmentWatcher(fhicl::ParameterSet const & pset);
  virtual ~FragmentWatcher();

  virtual void analyze(art::Event const & evt);

private:
  std::bitset<3> mode_bitset_;
  int metrics_reporting_level_;

  int events_processed_;
  int expected_number_of_fragments_;

  int events_with_missing_fragments_;
  int events_with_empty_fragments_;

  int events_with_10pct_missing_fragments_;
  int events_with_10pct_empty_fragments_;
  int events_with_50pct_missing_fragments_;
  int events_with_50pct_empty_fragments_;

  const int BASIC_COUNTS_MODE = 0;
  const int FRACTIONAL_COUNTS_MODE = 1;
  const int DETAILED_COUNTS_MODE = 2;
};

artdaq::FragmentWatcher::FragmentWatcher(fhicl::ParameterSet const & pset)
  : EDAnalyzer(pset),
    mode_bitset_(std::bitset<3>(pset.get<int>("mode_bitmask", 0x1))),
    metrics_reporting_level_(pset.get<int>("metrics_reporting_level", 1)),
    events_processed_(0), expected_number_of_fragments_(0),
    events_with_missing_fragments_(0), events_with_empty_fragments_(0),
    events_with_10pct_missing_fragments_(0), events_with_10pct_empty_fragments_(0),
    events_with_50pct_missing_fragments_(0), events_with_50pct_empty_fragments_(0)
{
  fhicl::ParameterSet metric_pset;
  try
  {
    metric_pset = pset.get<fhicl::ParameterSet>("metrics");
    if (metricMan != nullptr)
    {
      metricMan->initialize(metric_pset, "FragmentWatcher.");
      metricMan->do_start();
    }
  }
  catch (...)
  {
    TLOG(TLVL_INFO) << "Unable to find the metrics parameters in the metrics "
                    << "ParameterSet: \"" + pset.to_string() + "\".";
  }
}

artdaq::FragmentWatcher::~FragmentWatcher()
{
  if (metricMan != nullptr)
  {
    metricMan->do_stop();
    metricMan->shutdown();
  }
}

void artdaq::FragmentWatcher::analyze(art::Event const & evt)
{
  events_processed_++;

  // get all the artdaq fragment collections in the event.
  std::vector< art::Handle< std::vector<artdaq::Fragment> > > fragmentHandles;
  evt.getManyByType(fragmentHandles);

  // count total fragments
  int total_fragments = 0;
  for (auto const& hndl : fragmentHandles)
  {
    total_fragments += hndl->size();
  }

  // update the expected number of fragments, if needed
  if (total_fragments > expected_number_of_fragments_) {expected_number_of_fragments_ = total_fragments;}

  // check if this event has fewer fragments than expected
  int missing_fragments = expected_number_of_fragments_ - total_fragments;

  // check if this event has any Empty fragments
  int empty_fragments = 0; 
  for (auto const& hndl : fragmentHandles)
  {
    std::string instance_name = hndl.provenance()->productInstanceName();
    std::size_t found = instance_name.find("Empty");
    if (found != std::string::npos)
    {
      empty_fragments += hndl->size();
    }
  }

  // provide diagnostic TRACE message(s) about this event
  TLOG(TLVL_TRACE) << "Event " << evt.event() << ": total_fragments=" << total_fragments << ", missing_fragments="
                   << missing_fragments << ", empty_fragments=" << empty_fragments << " (" << events_processed_
                   << " events processed)";

  // common metric reporting for multiple modes
  if (metricMan != nullptr && (mode_bitset_.test(BASIC_COUNTS_MODE) || mode_bitset_.test(FRACTIONAL_COUNTS_MODE)))
  {
    metricMan->sendMetric("EventsProcessed", events_processed_, "events", metrics_reporting_level_,
                          artdaq::MetricMode::LastPoint);
  }

  // metric reporting for the BASIC_COUNTS_MODE
  if (metricMan != nullptr && mode_bitset_.test(BASIC_COUNTS_MODE))
  {
    if (missing_fragments > 0) {++events_with_missing_fragments_;}
    if (empty_fragments > 0) {++events_with_empty_fragments_;}

    metricMan->sendMetric("EventsWithMissingFragments", events_with_missing_fragments_, "events",
                          metrics_reporting_level_, artdaq::MetricMode::LastPoint);
    metricMan->sendMetric("EventsWithEmptyFragments", events_with_empty_fragments_, "events",
                          metrics_reporting_level_, artdaq::MetricMode::LastPoint);

    TLOG(TLVL_TRACE) << "Event " << evt.event() << ": events_with_missing_fragments=" << events_with_missing_fragments_
                     << ", events_with_empty_fragments=" << events_with_empty_fragments_;
  }

  // metric reporting for the FRACTIONAL_COUNTS_MODE
  if (metricMan != nullptr && mode_bitset_.test(FRACTIONAL_COUNTS_MODE))
  {
    if (((static_cast<double>(missing_fragments) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 10.0)
    {
      ++events_with_10pct_missing_fragments_;
    }
    if (((static_cast<double>(missing_fragments) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 50.0)
    {
      ++events_with_50pct_missing_fragments_;
    }

    if (((static_cast<double>(empty_fragments) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 10.0)
    {
      ++events_with_10pct_empty_fragments_;
    }
    if (((static_cast<double>(empty_fragments) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 50.0)
    {
      ++events_with_50pct_empty_fragments_;
    }

    metricMan->sendMetric("EventsWith10PctMissingFragments", events_with_10pct_missing_fragments_, "events",
                          metrics_reporting_level_, artdaq::MetricMode::LastPoint);
    metricMan->sendMetric("EventsWith50PctMissingFragments", events_with_50pct_missing_fragments_, "events",
                          metrics_reporting_level_, artdaq::MetricMode::LastPoint);

    metricMan->sendMetric("EventsWith10PctEmptyFragments", events_with_10pct_empty_fragments_, "events",
                          metrics_reporting_level_, artdaq::MetricMode::LastPoint);
    metricMan->sendMetric("EventsWith50PctEmptyFragments", events_with_50pct_empty_fragments_, "events",
                          metrics_reporting_level_, artdaq::MetricMode::LastPoint);

    TLOG(TLVL_TRACE) << "Event " << evt.event() << ": events_with_10pct_missing_fragments=" << events_with_10pct_missing_fragments_
                     << ", events_with_10pct_empty_fragments=" << events_with_10pct_empty_fragments_;
    TLOG(TLVL_TRACE) << "Event " << evt.event() << ": events_with_50pct_missing_fragments=" << events_with_50pct_missing_fragments_
                     << ", events_with_50pct_empty_fragments=" << events_with_50pct_empty_fragments_;
  }

  // still to be done: implement the 'detailed_counts' mode

#if 0
=====================================================

event_builder_snapshot : {
  name: "EventBuilder5"
  timestamp: "20190408T124433"
  events_built: 105

  sender_list: [ "felix501", "felix501", "ssp101", "ssp102" ]
  valid_fragment_counts: [ 105, 105, 102, 104 ]
  empty_fragment_counts: [ 0, 0, 2, 0 ]
  missing_fragment_counts: [ 0, 0, 1, 1 ]
}

=====================================================

<event_builder_snapshot name="EventBuilder5">
  <timestamp>20190408T124433</timestamp>
  <events_built>105</events_built

  <sender_list>
    <sender index=0>felix501</sender>
    <sender index=1>felix502</sender>
    <sender index=2>ssp101</sender>
    <sender index=3>ssp102</sender>
  </sender_list>

  <valid_fragment_counts>
    <count index=0>105</count>
    <count index=1>105</count>
    <count index=2>102</count>
    <count index=3>104</count>
  </valid_fragment_counts>

  <empty_fragment_counts>
    <count index=2>2</count>
  </empty_fragment_counts>

  <missing_fragment_counts>
    <count index=2>1</count>
    <count index=3>1</count>
  </missing_fragment_counts>
</event_builder_snapshot>

=====================================================
#endif

}

DEFINE_ART_MODULE(artdaq::FragmentWatcher)
