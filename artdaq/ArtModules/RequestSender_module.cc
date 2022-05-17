////////////////////////////////////////////////////////////////////////
// Class:       RequestSender
// Module Type: analyzer
// File:        RequestSender_module.cc
// Description: Sends artdaq requests for events
////////////////////////////////////////////////////////////////////////

#define TRACE_NAME "RequestSenderModule"
#include "artdaq/DAQdata/Globals.hh"

#include "art/Framework/Core/EDAnalyzer.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/Run.h"

#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/detail/RequestSender.hh"

#include <set>

namespace artdaq {
class RequestSenderModule;
}

/// <summary>
/// An art::EDAnalyzer module which sends requests for events (e.g. for the Mu2e CRV system)
/// </summary>
class artdaq::RequestSenderModule : public art::EDAnalyzer
{
public:
	/**
	 * \brief RequestSender Constructor
	 * \param pset ParameterSet used to configure RequestSender
	 *
	 * See artdaq/DAQrate/detail/RequestSender.hh for RequestSender configuration
	 * Additional Parameters:
	 * "request_list_max_size": Maximum number of requests to send at once
	 */
	explicit RequestSenderModule(fhicl::ParameterSet const& pset);
	/**
	 * \brief Virtual Destructor. Shuts down MetricManager if one is present
	 */
	~RequestSenderModule() override;

	/**
   * \brief Analyze each event, using the configured mode bitmask
   * \param evt art::Event to analyze
   */
	void analyze(art::Event const& evt) override;

	/**
	 * @brief Perform begin Run actions
	 * @param run Run object
	 */
	void beginRun(art::Run const& run) override;
	/**
	 * @brief PErform end Run actions
	 * @param run Run object
	 */
	void endRun(art::Run const& run) override;

private:
	RequestSenderModule(RequestSenderModule const&) = delete;
	RequestSenderModule(RequestSenderModule&&) = delete;
	RequestSenderModule& operator=(RequestSenderModule const&) = delete;
	RequestSenderModule& operator=(RequestSenderModule&&) = delete;

	RequestSender the_sender_;
	std::set<artdaq::Fragment::sequence_id_t> active_requests_;
	size_t max_active_requests_;

	void clear_active_requests();
	void trim_active_requests();
};

artdaq::RequestSenderModule::RequestSenderModule(fhicl::ParameterSet const& pset)
    : EDAnalyzer(pset)
    , the_sender_(pset)
    , max_active_requests_(pset.get<size_t>("request_list_max_size", 1000))
{
}

artdaq::RequestSenderModule::~RequestSenderModule()
{
}

void artdaq::RequestSenderModule::analyze(art::Event const& evt)
{
	// get all the artdaq fragment collections in the event.
	std::vector<art::Handle<std::vector<artdaq::Fragment>>> fragmentHandles;
#if ART_HEX_VERSION < 0x30900
	evt.getManyByType(fragmentHandles);
#else
	fragmentHandles = evt.getMany<std::vector<artdaq::Fragment>>();
#endif

	artdaq::Fragment::sequence_id_t seq = 0;
	artdaq::Fragment::timestamp_t timestamp = 0;

	for (auto& handle : fragmentHandles)
	{
		if (!handle.isValid()) continue;

		auto frag = handle->at(0);
		seq = frag.sequenceID();
		timestamp = frag.timestamp();

		if (seq != 0 && timestamp != 0) break;
	}

	if (seq != 0 && timestamp != 0)
	{
		TLOG(TLVL_DEBUG + 35) << "Adding request for sequence ID " << seq << ", timestamp " << timestamp;
		the_sender_.AddRequest(seq, timestamp);
		active_requests_.insert(seq);
		trim_active_requests();
	}
}

void artdaq::RequestSenderModule::beginRun(art::Run const& run)
{
	the_sender_.SetRunNumber(run.run());
	the_sender_.SetRequestMode(detail::RequestMessageMode::Normal);
	clear_active_requests();
}

void artdaq::RequestSenderModule::endRun(art::Run const&)
{
	the_sender_.SetRequestMode(detail::RequestMessageMode::EndOfRun);
	the_sender_.SendRequest();
	clear_active_requests();
}

void artdaq::RequestSenderModule::clear_active_requests()
{
	for (auto& req : active_requests_)
	{
		the_sender_.RemoveRequest(req);
	}
}

void artdaq::RequestSenderModule::trim_active_requests()
{
	TLOG(TLVL_DEBUG + 33) << "Going to remove extra active requests";
	while (active_requests_.size() > max_active_requests_)
	{
		TLOG(TLVL_DEBUG + 36) << "Removing request with sequence ID " << *active_requests_.begin();
		the_sender_.RemoveRequest(*active_requests_.begin());
		active_requests_.erase(active_requests_.begin());
	}
}

DEFINE_ART_MODULE(artdaq::RequestSenderModule)  // NOLINT(performance-unnecessary-value-param)
