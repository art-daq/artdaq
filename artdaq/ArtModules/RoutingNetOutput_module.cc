#define TRACE_NAME (app_name + "_RoutingNetOutput").c_str()
#include "artdaq/DAQdata/Globals.hh"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/Selector.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/RoutingPolicies/makeRoutingMasterPolicy.hh"
#include "artdaq/RoutingPolicies/RoutingDestinationHelper.hh"
#include "artdaq/DAQrate/TokenReceiver.hh"

#include <unistd.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace art {
	class RoutingNetOutput;
}

using art::RoutingNetOutput;
using fhicl::ParameterSet;

/**
 * \brief An art::OutputModule which sends Fragments using DataSenderManager.
 * This module produces output identical to that of a BoardReader, for use in
 * systems which have multiple layers of EventBuilders.
 */
class art::RoutingNetOutput final : public OutputModule {
public:
	/**
	 * \brief RoutingNetOutput Constructor
	 * \param ps ParameterSet used to configure RoutingNetOutput
	 *
	 * RoutingNetOutput forwards its ParameterSet to art::OutputModule,
	 * so any Parameters it requires are also required by RoutingNetOutput.
	 * RoutingNetOutput also forwards its ParameterSet to DataSenderManager,
	 * so any Parameters *it* requires are *also* required by RoutingNetOutput.
	 * Finally, RoutingNetOutput accpets the following parameters:
	 * "rt_priority" (Default: 0): Priority for this thread
	 * "module_name" (Default: RoutingNetOutput): Friendly name for this module (MessageFacility Category)
	 */
	explicit RoutingNetOutput(ParameterSet const& ps);

	/**
	 * \brief RoutingNetOutput Destructor
	 */
	virtual ~RoutingNetOutput();

private:
	void beginJob() override;
	void endJob() override;

	void beginRun(RunPrincipal const&) override;
	void endRun(RunPrincipal const&) override;

	void write(EventPrincipal&) override;

	void writeRun(RunPrincipal&) override {};
	void writeSubRun(SubRunPrincipal&) override {};

	void initialize_MPI_();

	void deinitialize_MPI_();

	bool readParameterSet_(fhicl::ParameterSet const& pset);

private:
	ParameterSet full_pset_;
	fhicl::ParameterSet policy_pset_;
	fhicl::ParameterSet token_receiver_pset_;
	std::string name_ = "RoutingNetOutput";
	int rt_priority_ = 0;
	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_ = {nullptr};
	std::shared_ptr<artdaq::RoutingMasterPolicy> policy_ = {nullptr};
	std::unique_ptr<artdaq::TokenReceiver> token_receiver_ = {nullptr};
	std::unique_ptr<artdaq::RoutingDestinationHelper> destination_helper_ = {nullptr};
};

art::RoutingNetOutput::RoutingNetOutput(ParameterSet const& ps) : OutputModule(ps) {
	TLOG(TLVL_DEBUG) << "Begin: RoutingNetOutput::RoutingNetOutput(ParameterSet const& ps)\n";
	readParameterSet_(ps);
	TLOG(TLVL_DEBUG) << "End: RoutingNetOutput::RoutingNetOutput(ParameterSet const& ps)\n";
}

art::RoutingNetOutput::~RoutingNetOutput() { TLOG(TLVL_DEBUG) << "Begin/End: RoutingNetOutput::~RoutingNetOutput()\n"; }

void art::RoutingNetOutput::beginJob() {
	TLOG(TLVL_DEBUG) << "Begin: RoutingNetOutput::beginJob()\n";
	initialize_MPI_();
	TLOG(TLVL_DEBUG) << "End:   RoutingNetOutput::beginJob()\n";
}

void art::RoutingNetOutput::endJob() {
	TLOG(TLVL_DEBUG) << "Begin: RoutingNetOutput::endJob()\n";
	deinitialize_MPI_();
	TLOG(TLVL_DEBUG) << "End:   RoutingNetOutput::endJob()\n";
}

void art::RoutingNetOutput::beginRun(art::RunPrincipal const& rp)
{
	if (token_receiver_.get() != nullptr)
	{
		token_receiver_->setRunNumber(rp.run());
		token_receiver_->resumeTokenReception();
	}
}

void art::RoutingNetOutput::endRun(art::RunPrincipal const& rp)
{
	if (token_receiver_.get() != nullptr)
	{
	  TLOG(TLVL_INFO) << "Stopping run " << rp.run() << " after " << token_receiver_->getReceivedTokenCount() << " received tokens." ;
		token_receiver_->pauseTokenReception();
	}
}

void art::RoutingNetOutput::initialize_MPI_() {
  if (rt_priority_ > 0) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
    if (status != 0) {
      TLOG(TLVL_ERROR) << name_ << "Failed to set realtime priority to " << rt_priority_
				<< ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	sender_ptr_ = std::make_unique<artdaq::DataSenderManager>(full_pset_);
	assert(sender_ptr_);

	auto policy_plugin_spec = policy_pset_.get<std::string>("policy", "");
	if (policy_plugin_spec.length() == 0)
	{
		TLOG(TLVL_ERROR)
			<< "No policy type (parameter name = \"policy\") was "
			<< "specified in the policy ParameterSet.  The full "
			<< "initialization PSet was \"" << full_pset_.to_string() << "\"." ;
	}
	else
	{
		try
		{
			policy_ = artdaq::makeRoutingMasterPolicy(policy_plugin_spec, policy_pset_);
		}
		catch (...)
		{
			TLOG(TLVL_ERROR) << "Unable to create a Routing Policy of type " << policy_plugin_spec << " from ParameterSet: \"" + policy_pset_.to_string() + "\"." ;
		}

		if (policy_.get() != nullptr)
		{
			token_receiver_.reset(new artdaq::TokenReceiver(token_receiver_pset_, policy_, artdaq::detail::RoutingMasterMode::RouteBySequenceID, 1, 100));
			token_receiver_->startTokenReception();
			token_receiver_->pauseTokenReception();

			destination_helper_.reset(new artdaq::RoutingDestinationHelper(policy_));
		}
	}
}

void art::RoutingNetOutput::deinitialize_MPI_() {
	sender_ptr_.reset(nullptr);

	if (token_receiver_.get() != nullptr)
	{
		token_receiver_->stopTokenReception(true);
	}
}

bool art::RoutingNetOutput::readParameterSet_(fhicl::ParameterSet const& pset) {
	TLOG(TLVL_DEBUG) << name_ << "RoutingNetOutput::readParameterSet_ method called with "
	           << "ParameterSet = \"" << pset.to_string() << "\".";

	// determine the data sending parameters
	full_pset_ = pset;
	name_ = pset.get<std::string>("module_name", "RoutingNetOutput");
	rt_priority_ = pset.get<int>("rt_priority", 0);

	try
	{
		policy_pset_ = full_pset_.get<fhicl::ParameterSet>("policy");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR) << "Unable to find the policy parameters in the RoutingNetOutput initialization ParameterSet: \"" + full_pset_.to_string() + "\"." ;
	}

	try
	{
		token_receiver_pset_ = full_pset_.get<fhicl::ParameterSet>("token_receiver");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR) << "Unable to find the token_receiver parameters in the RoutingNetOutput initialization ParameterSet: \"" + full_pset_.to_string() + "\"." ;
	}

	TLOG(TLVL_TRACE) << "RoutingNetOutput::readParameterSet()";

	return true;
}

void art::RoutingNetOutput::write(EventPrincipal& ep) {
	assert(sender_ptr_);

	using RawEvent = artdaq::Fragments;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHandles = std::vector<RawEventHandle>;

	auto result_handles = std::vector<art::GroupQueryResult>();

	auto const& wrapped = art::WrappedTypeID::make<RawEvent>();
#if ART_HEX_VERSION >= 0x30000
  ModuleContext const mc{moduleDescription()};
  ProcessTag const processTag{"", mc.moduleDescription().processName()};

  result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);
#else
	result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif

  for (auto const& result_handle : result_handles) {
		auto const raw_event_handle = RawEventHandle(result_handle);

    if (!raw_event_handle.isValid()) continue;

    for (auto const& fragment : *raw_event_handle) {
			auto fragment_copy = fragment;
			auto fragid_id = fragment_copy.fragmentID();
			auto sequence_id = fragment_copy.sequenceID();

			// 12-Jun-2019, KAB: TODO: handle case(s) in which we never get a valid destination

			int loop_counter = 100;
			int dest_rank = destination_helper_->GetNextDestinationRank();
			while (dest_rank == artdaq::RoutingDestinationHelper::INVALID_DESTINATION && loop_counter-- > 0)
			{
				TLOG(TLVL_DEBUG) << "RoutingNetOutput::write waiting for valid destination rank";
				usleep(10000);
				dest_rank = destination_helper_->GetNextDestinationRank();
			}

			if (dest_rank != artdaq::RoutingDestinationHelper::INVALID_DESTINATION)
			{
				TLOG(TLVL_DEBUG) << "RoutingNetOutput::write seq=" << sequence_id << " frag=" << fragid_id << " dest_rank=" << dest_rank << " start";
				sender_ptr_->sendFragment(std::move(fragment_copy), dest_rank);
				TLOG(TLVL_DEBUG) << "RoutingNetOutput::write seq=" << sequence_id << " frag=" << fragid_id << " dest_rank=" << dest_rank << " done";
				// Events are unique in art, so this will be the only send with this sequence ID!
				sender_ptr_->RemoveRoutingTableEntry(sequence_id);
			}
			else
			{
				TLOG(TLVL_ERROR) << "Unable to determin a valid destination rank! This event has been lost: " << sequence_id;
			}
		}
	}

	return;
}

DEFINE_ART_MODULE(art::RoutingNetOutput)
