#include "TRACE/tracemf.h"
#include "artdaq/DAQdata/Globals.hh"  // include these 2 first -
#define TRACE_NAME (app_name + "_DataLoggerCore").c_str()

#include "artdaq/Application/DataLoggerCore.hh"

#include "fhiclcpp/ParameterSet.h"

bool artdaq::DataLoggerCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG + 32) << "initialize method called with DAQ "
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
