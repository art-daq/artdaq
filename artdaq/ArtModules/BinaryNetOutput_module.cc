#define TRACE_NAME "BinaryNetOutput"
#include "artdaq/DAQdata/Globals.hh"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/Selector.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "art/Persistency/Provenance/ModuleContext.h"
#include "canvas/Persistency/Common/Wrapper.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "canvas/Persistency/Common/WrappedTypeID.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"

#include <unistd.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace art {
class BinaryNetOutput;
}

using art::BinaryNetOutput;
using fhicl::ParameterSet;

/**
 * \brief An art::OutputModule which sends Fragments using DataSenderManager.
 * This module produces output identical to that of a BoardReader, for use in
 * systems which have multiple layers of EventBuilders.
 */
class art::BinaryNetOutput final : public OutputModule
{
public:
	/**
	 * \brief BinaryNetOutput Constructor
	 * \param ps ParameterSet used to configure BinaryNetOutput
	 *
	 * BinaryNetOutput forwards its ParameterSet to art::OutputModule,
	 * so any Parameters it requires are also required by BinaryNetOutput.
	 * BinaryNetOutput also forwards its ParameterSet to DataSenderManager,
	 * so any Parameters *it* requires are *also* required by BinaryMPIOuptut.
	 * Finally, BinaryNetOutput accpets the following parameters:
	 * "rt_priority" (Default: 0): Priority for this thread
	 * "module_name" (Default: BinaryNetOutput): Friendly name for this module (MessageFacility Category)
	 */
	explicit BinaryNetOutput(ParameterSet const& ps);

	/**
	 * \brief BinaryNetOutput Destructor
	 */
	~BinaryNetOutput() override;

private:
	BinaryNetOutput(BinaryNetOutput const&) = delete;
	BinaryNetOutput(BinaryNetOutput&&) = delete;
	BinaryNetOutput& operator=(BinaryNetOutput const&) = delete;
	BinaryNetOutput& operator=(BinaryNetOutput&&) = delete;

	void beginJob() override;

	void endJob() override;

	void write(EventPrincipal& /*ep*/) override;

	void writeRun(RunPrincipal& /*r*/) override{};
	void writeSubRun(SubRunPrincipal& /*sr*/) override{};

	void initialize_MPI_();

	void deinitialize_MPI_();

	bool readParameterSet_(fhicl::ParameterSet const& pset);

private:
	ParameterSet data_pset_;
	std::string name_ = "BinaryNetOutput";
	int rt_priority_ = 0;
	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_ = {nullptr};
};

art::BinaryNetOutput::BinaryNetOutput(ParameterSet const& ps)
    : OutputModule(ps)
{
	TLOG(TLVL_DEBUG + 32) << "Begin: BinaryNetOutput::BinaryNetOutput(ParameterSet const& ps)\n";
	readParameterSet_(ps);
	TLOG(TLVL_DEBUG + 32) << "End: BinaryNetOutput::BinaryNetOutput(ParameterSet const& ps)\n";
}

art::BinaryNetOutput::~BinaryNetOutput() { TLOG(TLVL_DEBUG + 32) << "Begin/End: BinaryNetOutput::~BinaryNetOutput()\n"; }

void art::BinaryNetOutput::beginJob()
{
	TLOG(TLVL_DEBUG + 32) << "Begin: BinaryNetOutput::beginJob()\n";
	initialize_MPI_();
	TLOG(TLVL_DEBUG + 32) << "End:   BinaryNetOutput::beginJob()\n";
}

void art::BinaryNetOutput::endJob()
{
	TLOG(TLVL_DEBUG + 32) << "Begin: BinaryNetOutput::endJob()\n";
	deinitialize_MPI_();
	TLOG(TLVL_DEBUG + 32) << "End:   BinaryNetOutput::endJob()\n";
}

void art::BinaryNetOutput::initialize_MPI_()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
		if (status != 0)
		{
			TLOG(TLVL_ERROR) << name_ << "Failed to set realtime priority to " << rt_priority_
			                 << ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	sender_ptr_ = std::make_unique<artdaq::DataSenderManager>(data_pset_);
	assert(sender_ptr_);
}

void art::BinaryNetOutput::deinitialize_MPI_() { sender_ptr_.reset(nullptr); }

bool art::BinaryNetOutput::readParameterSet_(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG + 32) << name_ << "BinaryNetOutput::readParameterSet_ method called with "
	                 << "ParameterSet = \"" << pset.to_string() << "\".";

	// determine the data sending parameters
	data_pset_ = pset;
	name_ = pset.get<std::string>("module_name", "BinaryNetOutput");
	rt_priority_ = pset.get<int>("rt_priority", 0);

	TLOG(TLVL_DEBUG + 33) << "BinaryNetOutput::readParameterSet()";

	return true;
}

void art::BinaryNetOutput::write(EventPrincipal& ep)
{
	assert(sender_ptr_);

	using RawEvent = artdaq::Fragments;
	;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHandles = std::vector<RawEventHandle>;

	auto result_handles = std::vector<art::GroupQueryResult>();

	auto const& wrapped = art::WrappedTypeID::make<RawEvent>();

	ModuleContext const mc{moduleDescription()};
	ProcessTag const processTag{"", mc.moduleDescription().processName()};

	result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);

	artdaq::Fragment::sequence_id_t sequence_id = 0;

	for (auto const& result_handle : result_handles)
	{
		auto const raw_event_handle = RawEventHandle(result_handle);

		if (!raw_event_handle.isValid())
		{
			continue;
		}

		for (auto const& fragment : *raw_event_handle)
		{
			auto fragment_copy = fragment;
			auto fragid_id = fragment_copy.fragmentID();
			sequence_id = fragment_copy.sequenceID();
			TLOG(TLVL_DEBUG + 32) << "BinaryNetOutput::write seq=" << sequence_id << " frag=" << fragid_id << " start";
			sender_ptr_->sendFragment(std::move(fragment_copy));
			TLOG(TLVL_DEBUG + 32) << "BinaryNetOutput::write seq=" << sequence_id << " frag=" << fragid_id << " done";
		}
	}

	// Events are unique in art, so this will be the only send with this sequence ID!
	// ELF 1/23/2020: Only remove routing entry AFTER all Fragments have been sent!
	sender_ptr_->RemoveRoutingTableEntry(sequence_id);
}

DEFINE_ART_MODULE(art::BinaryNetOutput)  // NOLINT(performance-unnecessary-value-param)
