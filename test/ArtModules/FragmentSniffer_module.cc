#include "TRACE/tracemf.h"
#define TRACE_NAME "FragmentSniffer"

#include <memory>
#include <string>

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/Run.h"
#include "art/Framework/Principal/SubRun.h"
#include "canvas/Persistency/Provenance/BranchType.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/Globals.hh"

namespace artdaq {
/**
 * \brief This art::EDAnalyzer plugin tries to get Fragments from each event, asserting that the correct number of Fragments were present
 */
class FragmentSniffer : public art::EDAnalyzer
{
public:
	/**
	 * \brief FragmentSniffer Constructor
	 * \param p ParameterSet used to configure FragmentSniffer
	 *
	 * \verbatim
	 * FragmentSniffer accepts the following Parameters:
	 * "raw_label" (Default: "daq"): Label under which Fragments are stored
	 * "product_instance_name" (REQUIRED): Instance name under which Fragments are stored (Should be Fragment type name)
	 * "num_frags_per_event" (REQUIRED): Expected number of Fragments in each event
	 * "num_events_expected" (Default: 0): Expected number of events in the job. If 0, will not perform end-of-job test
	 * \endverbatim
	 */
	explicit FragmentSniffer(fhicl::ParameterSet const& p);

	/**
	 * \brief Default destructor
	 */
	~FragmentSniffer() override = default;

	/**
	 * \brief Called for each event. Asserts that Fragment objects are present in the event and that the correct number of Fragments were found
	 * \param e Event to analyze
	 */
	void analyze(art::Event const& e) override;

	/**
	 * \brief Called at the end of the job. Asserts that the number of events processed was equal to the expected number
	 */
	void endJob() override;

private:
	FragmentSniffer(FragmentSniffer const&) = delete;
	FragmentSniffer(FragmentSniffer&&) = delete;
	FragmentSniffer& operator=(FragmentSniffer const&) = delete;
	FragmentSniffer& operator=(FragmentSniffer&&) = delete;

	std::string raw_label_;
	std::string product_instance_name_;
	std::size_t num_frags_per_event_;
	std::size_t num_events_expected_;
	std::size_t num_events_processed_;
};

FragmentSniffer::FragmentSniffer(fhicl::ParameterSet const& p)
    : art::EDAnalyzer(p)
    , raw_label_(p.get<std::string>("raw_label", "daq"))
    , product_instance_name_(p.get<std::string>("product_instance_name"))
    , num_frags_per_event_(p.get<size_t>("num_frags_per_event"))
    , num_events_expected_(p.get<size_t>("num_events_expected", 0))
    , num_events_processed_() {}

void FragmentSniffer::analyze(art::Event const& e)
{
	TLOG(TLVL_DEBUG) << "Analyze called for event " << e.event();
	art::Handle<Fragments> handle;
	e.getByLabel(raw_label_, product_instance_name_, handle);
	assert(!handle->empty() && "getByLabel returned empty handle");
	assert(handle->size() == num_frags_per_event_);
	++num_events_processed_;
	TLOG(TLVL_DEBUG) << "End of FragmentSniffer::analyze";
}

void FragmentSniffer::endJob()
{
	TLOG(TLVL_INFO) << "events processed: "
	                << num_events_processed_
	                << "\nevents expected:  "
	                << num_events_expected_;
	if (num_events_expected_ > 0) { assert(num_events_processed_ == num_events_expected_); }
}

DEFINE_ART_MODULE(FragmentSniffer)  // NOLINT(performance-unnecessary-value-param)
}  // namespace artdaq
