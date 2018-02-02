#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/Selector.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Data/Fragment.hh"

#define TRACE_NAME "BinaryNetOutput"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include <unistd.h>

namespace art
{
	class BinaryNetOutput;
}

using art::BinaryNetOutput;
using fhicl::ParameterSet;

/**
 * \brief An art::OutputModule which sends Fragments using DataSenderManager.
 * This module produces output identical to that of a BoardReader, for use in
 * systems which have multiple layers of EventBuilders.
 */
class art::BinaryNetOutput final: public OutputModule
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
	virtual ~BinaryNetOutput();

private:
	void beginJob() override;

	void endJob() override;

	void write(EventPrincipal&) override;

	void writeRun(RunPrincipal&) override {};
	void writeSubRun(SubRunPrincipal&) override {};

	void initialize_MPI_();

	void deinitialize_MPI_();

	bool readParameterSet_(fhicl::ParameterSet const& pset);

private:
	ParameterSet data_pset_;
	std::string name_ = "BinaryNetOutput";
	int rt_priority_ = 0;
	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_ = {nullptr};
};

art::BinaryNetOutput::
BinaryNetOutput(ParameterSet const& ps)
	: OutputModule(ps)
{
	FDEBUG(1) << "Begin: BinaryNetOutput::BinaryNetOutput(ParameterSet const& ps)\n";
	readParameterSet_(ps);
	FDEBUG(1) << "End: BinaryNetOutput::BinaryNetOutput(ParameterSet const& ps)\n";
}

art::BinaryNetOutput::
~BinaryNetOutput()
{
	FDEBUG(1) << "Begin/End: BinaryNetOutput::~BinaryNetOutput()\n";
}

void
art::BinaryNetOutput::
beginJob()
{
	FDEBUG(1) << "Begin: BinaryNetOutput::beginJob()\n";
	initialize_MPI_();
	FDEBUG(1) << "End:   BinaryNetOutput::beginJob()\n";
}

void
art::BinaryNetOutput::
endJob()
{
	FDEBUG(1) << "Begin: BinaryNetOutput::endJob()\n";
	deinitialize_MPI_();
	FDEBUG(1) << "End:   BinaryNetOutput::endJob()\n";
}


void
art::BinaryNetOutput::
initialize_MPI_()
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
			TLOG_ERROR(name_)
				<< "Failed to set realtime priority to " << rt_priority_
				<< ", return code = " << status << TLOG_ENDL;
		}
#pragma GCC diagnostic pop
	}

	sender_ptr_ = std::make_unique<artdaq::DataSenderManager>(data_pset_);
	assert(sender_ptr_);
}

void
art::BinaryNetOutput::
deinitialize_MPI_()
{
	sender_ptr_.reset(nullptr);
}

bool
art::BinaryNetOutput::
readParameterSet_(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "BinaryNetOutput::readParameterSet_ method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;

	// determine the data sending parameters
	data_pset_ = pset;
	name_ = pset.get<std::string>("module_name", "BinaryNetOutput");
	rt_priority_ = pset.get<int>("rt_priority", 0);

	TRACE(4, "BinaryNetOutput::readParameterSet()");

	return true;
}

void
art::BinaryNetOutput::
write(EventPrincipal& ep)
{
	assert(sender_ptr_);

	using RawEvent = artdaq::Fragments;;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHandles = std::vector<RawEventHandle>;

	auto result_handles = std::vector<art::GroupQueryResult>();
	
#if ART_HEX_VERSION < 0x20906
	ep.getManyByType(art::TypeID(typeid(RawEvent)), result_handles);
#else
	auto const& wrapped = art::WrappedTypeID::make<RawEvent>();
	result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif

	for (auto const& result_handle : result_handles)
	{
		auto const raw_event_handle = RawEventHandle(result_handle);

		if (!raw_event_handle.isValid())
			continue;

		for (auto const& fragment : *raw_event_handle)
		{
			auto fragment_copy = fragment;
			auto fragid_id = fragment_copy.fragmentID();
			auto sequence_id = fragment_copy.sequenceID();
			TRACE(1, "BinaryNetOutput::write seq=%lu frag=%i start", sequence_id, fragid_id);
			sender_ptr_->sendFragment(std::move(fragment_copy));
			TRACE(2, "BinaryNetOutput::write seq=%lu frag=%i done", sequence_id, fragid_id);
		}
	}

	return;
}

DEFINE_ART_MODULE(art::BinaryNetOutput)
