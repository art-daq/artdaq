#include <errno.h>
#include <bitset>
#include <iomanip>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include "art/Framework/Art/artapp.h"
#include "cetlib/BasicPluginFactory.h"

#include "artdaq/DAQdata/Globals.hh"  // include these 2 first -
#define TRACE_NAME (app_name + "_DataLoggerCore").c_str()
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/Application/DataLoggerCore.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

artdaq::DataLoggerCore::DataLoggerCore()
    : DataReceiverCore()
{
}

artdaq::DataLoggerCore::~DataLoggerCore()
{
	TLOG(TLVL_DEBUG) << "Destructor";
}

bool artdaq::DataLoggerCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG) << "initialize method called with DAQ "
	                 << "ParameterSet = \"" << pset.to_string() << "\".";

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the DAQ parameters in the initialization "
		    << "ParameterSet: \"" + pset.to_string() + "\".";
		return false;
	}
	fhicl::ParameterSet agg_pset;
	try
	{
		agg_pset = daq_pset.get<fhicl::ParameterSet>("datalogger", daq_pset.get<fhicl::ParameterSet>("aggregator"));
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the DataLogger parameters in the DAQ "
		    << "initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
		return false;
	}

	// initialize the MetricManager and the names of our metrics
	fhicl::ParameterSet metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet());

	return initializeDataReceiver(pset, agg_pset, metric_pset);
}
