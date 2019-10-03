////////////////////////////////////////////////////////////////////////
// Class:       FragmentWatcher
// Module Type: analyzer
// File:        FragmentWatcher_module.cc
// Description: Collects and reports statistics on missing and empty fragments
//
// The model that is followed here is to publish to the metrics system
// the full history of what has happened so far.  In that way, each update
// is self-contained.  So, the map of fragment IDs that have missing or
// empty fragments will contain the total number of events in which each
// fragment ID was missing or empty.
//
// TRACE messages, though, contain a mix of per-event and overall results.
// To enable TLVL_TRACE messages that have overall resuts (for debugging),
// use 'tonM -n <appname>_FragmentWatcher 4'.
////////////////////////////////////////////////////////////////////////

#define TRACE_NAME (app_name + "_FragmentWatcher").c_str()
#include "artdaq/DAQdata/Globals.hh"

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"

#include <bitset>
#include <iostream>
#include <map>

#define TLVL_BAD_FRAGMENTS TLVL_WARNING
#define TLVL_EVENT_SUMMARY TLVL_TRACE
#define TLVL_EXPECTED_FRAGIDS 5
#define TLVL_BASIC_MODE 6
#define TLVL_FRACTIONAL_MODE 7

namespace artdaq {
class FragmentWatcher;
}

/// <summary>
/// An art::EDAnalyzer module which checks events for certain error conditions (missing fragments, empty fragments, etc)
/// </summary>
class artdaq::FragmentWatcher : public art::EDAnalyzer
{
public:
	/**
	 * \brief FragmentWatcher Constructor
	 * \param pset ParameterSet used to configure FragmentWatcher
	 *
	 * FragmentWatcher accepts the following Parameters:
	 * mode_bitmask (default: 0x1): Mask of modes to use. BASIC_COUNTS_MODE = 0, FRACTIONAL_COUNTS_MODE = 1, DETAILED_COUNTS_MODE = 2
	 * metrics_reporting_level (default: 1): Level to use for metrics reporting
	 * metrics: A artdaq::MetricManager::Config ParameterSet used to configure MetricManager reporting for this module
	 */
	explicit FragmentWatcher(fhicl::ParameterSet const& pset);
	/**
	 * \brief Virtual Destructor. Shuts down MetricManager if one is present
	 */
	virtual ~FragmentWatcher();

	/**
   * \brief Analyze each event, using the configured mode bitmask
   * \param evt art::Event to analyze
   */
	virtual void analyze(art::Event const& evt);

private:
	std::bitset<3> mode_bitset_;
	int metrics_reporting_level_;

	int events_processed_;
	int expected_number_of_fragments_;
	std::set<int> expected_fragmentID_list_;

	int events_with_missing_fragments_;
	int events_with_empty_fragments_;

	int events_with_10pct_missing_fragments_;
	int events_with_10pct_empty_fragments_;
	int events_with_50pct_missing_fragments_;
	int events_with_50pct_empty_fragments_;

	std::map<int, int> missing_fragments_by_fragmentID_;
	std::map<int, int> empty_fragments_by_fragmentID_;

	const int BASIC_COUNTS_MODE = 0;
	const int FRACTIONAL_COUNTS_MODE = 1;
	const int DETAILED_COUNTS_MODE = 2;
};

artdaq::FragmentWatcher::FragmentWatcher(fhicl::ParameterSet const& pset)
    : EDAnalyzer(pset), mode_bitset_(std::bitset<3>(pset.get<int>("mode_bitmask", 0x1))), metrics_reporting_level_(pset.get<int>("metrics_reporting_level", 1)), events_processed_(0), expected_number_of_fragments_(0), expected_fragmentID_list_(), events_with_missing_fragments_(0), events_with_empty_fragments_(0), events_with_10pct_missing_fragments_(0), events_with_10pct_empty_fragments_(0), events_with_50pct_missing_fragments_(0), events_with_50pct_empty_fragments_(0), missing_fragments_by_fragmentID_(), empty_fragments_by_fragmentID_()
{
}

artdaq::FragmentWatcher::~FragmentWatcher()
{
}

void artdaq::FragmentWatcher::analyze(art::Event const& evt)
{
	events_processed_++;

	// get all the artdaq fragment collections in the event.
	std::vector<art::Handle<std::vector<artdaq::Fragment> > > fragmentHandles;
	evt.getManyByType(fragmentHandles);

	// count total fragments
	int total_fragments_this_event = 0;
	for (auto const& hndl : fragmentHandles)
	{
		total_fragments_this_event += hndl->size();
	}

	// update the expected number of fragments, if needed
	if (total_fragments_this_event > expected_number_of_fragments_)
	{
		expected_number_of_fragments_ = total_fragments_this_event;

		// also update the expected fragment IDs
		for (auto const& hndl : fragmentHandles)
		{
			for (auto const& fragment : *hndl)
			{
				int fragID = fragment.fragmentID();
				TLOG(TLVL_EXPECTED_FRAGIDS) << "Inserting fragment ID " << fragID << " into the list of expected_fragmentIDs.";
				expected_fragmentID_list_.insert(fragID);
			}
		}
	}

	// check if this event has fewer fragments than expected
	int missing_fragment_count_this_event = expected_number_of_fragments_ - total_fragments_this_event;
	std::set<int> missing_fragmentID_list_this_event;
	if (missing_fragment_count_this_event > 0)
	{
		// determine the IDs of the missing fragments
		missing_fragmentID_list_this_event = expected_fragmentID_list_;
		for (auto const& hndl : fragmentHandles)
		{
			for (auto const& fragment : *hndl)
			{
				int fragID = fragment.fragmentID();
				missing_fragmentID_list_this_event.erase(fragID);
			}
		}

		// track the number of missing fragments by fragment ID
		for (int const& fragID : missing_fragmentID_list_this_event)
		{
			if (missing_fragments_by_fragmentID_.count(fragID) == 0)
			{
				missing_fragments_by_fragmentID_[fragID] = 1;
			}
			else
			{
				missing_fragments_by_fragmentID_[fragID] += 1;
			}
		}
	}

	// check if this event has any Empty fragments
	int empty_fragment_count_this_event = 0;
	std::set<int> empty_fragmentID_list_this_event;
	for (auto const& hndl : fragmentHandles)
	{
		std::string instance_name = hndl.provenance()->productInstanceName();
		std::size_t found = instance_name.find("Empty");
		if (found != std::string::npos)
		{
			empty_fragment_count_this_event += hndl->size();

			// track the number of empty fragments by fragment ID
			for (auto const& fragment : *hndl)
			{
				int fragID = fragment.fragmentID();
				if (empty_fragments_by_fragmentID_.count(fragID) == 0)
				{
					empty_fragments_by_fragmentID_[fragID] = 1;
				}
				else
				{
					empty_fragments_by_fragmentID_[fragID] += 1;
				}
				empty_fragmentID_list_this_event.insert(fragID);
			}
		}
	}

	// common reporting
	if (metricMan != nullptr && (mode_bitset_.test(BASIC_COUNTS_MODE) || mode_bitset_.test(FRACTIONAL_COUNTS_MODE)))
	{
		metricMan->sendMetric("EventsProcessed", events_processed_, "events", metrics_reporting_level_,
		                      artdaq::MetricMode::LastPoint);
	}
	TLOG(TLVL_EVENT_SUMMARY) << "Event " << evt.event() << ": this event: total_fragments=" << total_fragments_this_event
	                         << ", missing_fragments=" << missing_fragment_count_this_event << ", empty_fragments="
	                         << empty_fragment_count_this_event << " (" << events_processed_ << " events processed)";
	// log TRACE message if there are missing fragments
	if (missing_fragmentID_list_this_event.size() > 0)
	{
		std::ostringstream oss;
		bool firstLoop = true;
		for (auto const& fragID : missing_fragmentID_list_this_event)
		{
			if (!firstLoop) { oss << ", "; }
			oss << fragID;
			firstLoop = false;
		}
		TLOG(TLVL_BAD_FRAGMENTS) << "Event " << evt.event() << ": total_fragments=" << total_fragments_this_event
		                         << ", fragmentIDs for missing_fragments: " << oss.str();
	}
	// log TRACE message if there are empty fragments
	if (empty_fragmentID_list_this_event.size() > 0)
	{
		std::ostringstream oss;
		bool firstLoop = true;
		for (auto const& fragID : empty_fragmentID_list_this_event)
		{
			if (!firstLoop) { oss << ", "; }
			oss << fragID;
			firstLoop = false;
		}
		TLOG(TLVL_BAD_FRAGMENTS) << "Event " << evt.event() << ": total_fragments=" << total_fragments_this_event
		                         << ", fragmentIDs for empty_fragments: " << oss.str();
	}

	// reporting for the BASIC_COUNTS_MODE
	if (metricMan != nullptr && mode_bitset_.test(BASIC_COUNTS_MODE))
	{
		if (missing_fragment_count_this_event > 0) { ++events_with_missing_fragments_; }
		if (empty_fragment_count_this_event > 0) { ++events_with_empty_fragments_; }

		metricMan->sendMetric("EventsWithMissingFragments", events_with_missing_fragments_, "events",
		                      metrics_reporting_level_, artdaq::MetricMode::LastPoint);
		metricMan->sendMetric("EventsWithEmptyFragments", events_with_empty_fragments_, "events",
		                      metrics_reporting_level_, artdaq::MetricMode::LastPoint);

		TLOG(TLVL_BASIC_MODE) << "Event " << evt.event() << ": events_with_missing_fragments=" << events_with_missing_fragments_
		                      << ", events_with_empty_fragments=" << events_with_empty_fragments_;
	}

	// reporting for the FRACTIONAL_COUNTS_MODE
	if (metricMan != nullptr && mode_bitset_.test(FRACTIONAL_COUNTS_MODE))
	{
		if (((static_cast<double>(missing_fragment_count_this_event) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 10.0)
		{
			++events_with_10pct_missing_fragments_;
		}
		if (((static_cast<double>(missing_fragment_count_this_event) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 50.0)
		{
			++events_with_50pct_missing_fragments_;
		}

		if (((static_cast<double>(empty_fragment_count_this_event) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 10.0)
		{
			++events_with_10pct_empty_fragments_;
		}
		if (((static_cast<double>(empty_fragment_count_this_event) * 100.0) / static_cast<double>(expected_number_of_fragments_)) >= 50.0)
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

		TLOG(TLVL_FRACTIONAL_MODE) << "Event " << evt.event() << ": events_with_10pct_missing_fragments=" << events_with_10pct_missing_fragments_
		                           << ", events_with_10pct_empty_fragments=" << events_with_10pct_empty_fragments_;
		TLOG(TLVL_FRACTIONAL_MODE) << "Event " << evt.event() << ": events_with_50pct_missing_fragments=" << events_with_50pct_missing_fragments_
		                           << ", events_with_50pct_empty_fragments=" << events_with_50pct_empty_fragments_;
	}

	// reporting for the DETAILED_COUNTS_MODE
	if (metricMan != nullptr && mode_bitset_.test(DETAILED_COUNTS_MODE))
	{
		// only send an update when the missing or empty fragment counts, by FragmentID, changed,
		// as indicated by a non-zero number of missing or empty fragments in this event
		if (missing_fragment_count_this_event > 0 || empty_fragment_count_this_event > 0)
		{
			std::ostringstream oss;
			oss << "<eventbuilder_snapshot app_name=\"" << app_name << "\"><events_processed>" << events_processed_
			    << "</events_processed>";
			oss << "<missing_fragment_counts>";
			for (auto const& mapIter : missing_fragments_by_fragmentID_)
			{
				oss << "<count fragment_id=" << mapIter.first << ">" << mapIter.second << "</count>";
			}
			oss << "</missing_fragment_counts>";
			oss << "<empty_fragment_counts>";
			for (auto const& mapIter : empty_fragments_by_fragmentID_)
			{
				oss << "<count fragment_id=" << mapIter.first << ">" << mapIter.second << "</count>";
			}
			oss << "</empty_fragment_counts>";
			oss << "</eventbuilder_snapshot>";

			metricMan->sendMetric("EmptyFragmentSnapshot", oss.str(), "xml_string",
			                      metrics_reporting_level_, artdaq::MetricMode::LastPoint);
		}
	}

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
