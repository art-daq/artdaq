#include "canvas/Utilities/Exception.h"
#include "art/Framework/Art/artapp.h"

#define TRACE_NAME "EventBuilderCore"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/Application/EventBuilderCore.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

#include <iomanip>

artdaq::EventBuilderCore::EventBuilderCore(int rank, std::string name)
	: DataReceiverCore(rank, name)
{
}

artdaq::EventBuilderCore::~EventBuilderCore()
{
	TLOG_DEBUG(name_) << "Destructor" << TLOG_ENDL;
}

bool artdaq::EventBuilderCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "initialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string() << "\"." << TLOG_ENDL;

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG_ERROR(name_)
			<< "Unable to find the DAQ parameters in the initialization "
			<< "ParameterSet: \"" + pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}
	fhicl::ParameterSet evb_pset;
	try
	{
		evb_pset = daq_pset.get<fhicl::ParameterSet>("event_builder");
	}
	catch (...)
	{
		TLOG_ERROR(name_)
			<< "Unable to find the event_builder parameters in the DAQ "
			<< "initialization ParameterSet: \"" + daq_pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}

	fhicl::ParameterSet metric_pset;
	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL

	return initializeDataReceiver(pset,evb_pset, metric_pset);
}
