#include <errno.h>
#include <sstream>
#include <iomanip>
#include <bitset>

#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include "art/Framework/Art/artapp.h"
#include "cetlib/BasicPluginFactory.h"

#define TRACE_NAME "DataLoggerCore"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq/Application/DataLoggerCore.hh"
#include "artdaq/DAQrate/detail/FragCounter.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

artdaq::DataLoggerCore::DataLoggerCore()
	: DataReceiverCore()
{
}

artdaq::DataLoggerCore::~DataLoggerCore()
{
	TLOG_DEBUG(app_name) << "Destructor" << TLOG_ENDL;
}

bool artdaq::DataLoggerCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(app_name) << "initialize method called with DAQ " << "ParameterSet = \"" << pset.to_string() << "\"." << TLOG_ENDL;

	// pull out the relevant parts of the ParameterSet
	fhicl::ParameterSet daq_pset;
	try
	{
		daq_pset = pset.get<fhicl::ParameterSet>("daq");
	}
	catch (...)
	{
		TLOG_ERROR(app_name)
			<< "Unable to find the DAQ parameters in the initialization "
			<< "ParameterSet: \"" + pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}
	fhicl::ParameterSet agg_pset;
	try
	{
		agg_pset = daq_pset.get<fhicl::ParameterSet>("datalogger", daq_pset.get<fhicl::ParameterSet>("aggregator"));
	}
	catch (...)
	{
		TLOG_ERROR(app_name)
			<< "Unable to find the DataLogger parameters in the DAQ "
			<< "initialization ParameterSet: \"" + daq_pset.to_string() + "\"." << TLOG_ENDL;
		return false;
	}

	// initialize the MetricManager and the names of our metrics
	fhicl::ParameterSet metric_pset;

	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL                                    


	return initializeDataReceiver(pset, agg_pset, metric_pset);
}
