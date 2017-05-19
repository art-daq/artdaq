#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Data/Fragment.hh"

#define TRACE_NAME "BinaryMPIOutput"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include "unistd.h"

namespace art
{
	class BinaryMPIOutput;
}

using art::BinaryMPIOutput;
using fhicl::ParameterSet;

/**
 * \brief An art::OutputModule which sends Fragments using DataSenderManager.
 * This module produces output identical to that of a BoardReader, for use in
 * systems which have multiple layers of EventBuilders.
 */
class art::BinaryMPIOutput final: public OutputModule
{
public:
	/**
	 * \brief BinaryMPIOutput Constructor
	 * \param ps ParameterSet used to configure BinaryMPIOutput
	 * 
	 * BinaryMPIOutput forwards its ParameterSet to art::OutputModule, 
	 * so any Parameters it requires are also required by BinaryMPIOutput.
	 * BinaryMPIOutput also forwards its ParameterSet to DataSenderManager,
	 * so any Parameters *it* requires are *also* required by BinaryMPIOuptut.
	 * Finally, BinaryMPIOutput accpets the following parameters:
	 * "rt_priority" (Default: 0): Priority for this thread
	 * "module_name" (Default: BinaryMPIOutput): Friendly name for this module (MessageFacility Category)
	 */
	explicit BinaryMPIOutput(ParameterSet const& ps);

	/**
	 * \brief BinaryMPIOutput Destructor
	 */
	virtual ~BinaryMPIOutput();

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
	std::string name_ = "BinaryMPIOutput";
	int rt_priority_ = 0;
	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_ = {nullptr};
};

art::BinaryMPIOutput::
BinaryMPIOutput(ParameterSet const& ps)
	: OutputModule(ps)
{
	FDEBUG(1) << "Begin: BinaryMPIOutput::BinaryMPIOutput(ParameterSet const& ps)\n";
	readParameterSet_(ps);
	FDEBUG(1) << "End: BinaryMPIOutput::BinaryMPIOutput(ParameterSet const& ps)\n";
}

art::BinaryMPIOutput::
~BinaryMPIOutput()
{
	FDEBUG(1) << "Begin/End: BinaryMPIOutput::~BinaryMPIOutput()\n";
}

void
art::BinaryMPIOutput::
beginJob()
{
	FDEBUG(1) << "Begin: BinaryMPIOutput::beginJob()\n";
	initialize_MPI_();
	FDEBUG(1) << "End:   BinaryMPIOutput::beginJob()\n";
}

void
art::BinaryMPIOutput::
endJob()
{
	FDEBUG(1) << "Begin: BinaryMPIOutput::endJob()\n";
	deinitialize_MPI_();
	FDEBUG(1) << "End:   BinaryMPIOutput::endJob()\n";
}


void
art::BinaryMPIOutput::
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
art::BinaryMPIOutput::
deinitialize_MPI_()
{
	sender_ptr_.reset(nullptr);
}

bool
art::BinaryMPIOutput::
readParameterSet_(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "BinaryMPIOutput::readParameterSet_ method called with "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;

	// determine the data sending parameters
	data_pset_ = pset;
	name_ = pset.get<std::string>("module_name", "BinaryMPIOutput");
	rt_priority_ = pset.get<int>("rt_priority", 0);

	TRACE(4, "BinaryMPIOutput::readParameterSet()");

	return true;
}

void
art::BinaryMPIOutput::
write(EventPrincipal& ep)
{
	assert(sender_ptr_);

	using RawEvent = artdaq::Fragments;;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHandles = std::vector<RawEventHandle>;

	auto result_handles = std::vector<art::GroupQueryResult>();
	ep.getManyByType(art::TypeID(typeid(RawEvent)), result_handles);

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
			TRACE(1, "BinaryMPIOutput::write seq=%lu frag=%i start", sequence_id, fragid_id);
			sender_ptr_->sendFragment(std::move(fragment_copy));
			TRACE(2, "BinaryMPIOutput::write seq=%lu frag=%i done", sequence_id, fragid_id);
		}
	}

	return;
}

DEFINE_ART_MODULE(art::BinaryMPIOutput)
