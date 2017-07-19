#include <errno.h>
#include <sstream>
#include <iomanip>
#include <bitset>

#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq/Application/DispatcherCore.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"



artdaq::DispatcherCore::DispatcherCore(int rank, std::string name)
	: DataReceiverCore(rank, name)
{
}

artdaq::DispatcherCore::~DispatcherCore()
{
	TLOG_DEBUG(name_) << "Destructor" << TLOG_ENDL;
}

bool artdaq::DispatcherCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "initialize method called with DAQ " << "ParameterSet = \"" << pset.to_string() << "\"." << TLOG_ENDL;

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
	fhicl::ParameterSet agg_pset;
	try
	{
		agg_pset = daq_pset.get<fhicl::ParameterSet>("dispatcher", daq_pset.get<fhicl::ParameterSet>("aggregator"));
	}
	catch (...)
	{
		TLOG_ERROR(name_)
			<< "Unable to find the Dispatcher parameters in the DAQ "
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

	return initializeDataReceiver(agg_pset, metric_pset);
}

std::string artdaq::DispatcherCore::register_monitor(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "DispatcherCore::register_monitor called with argument \"" << pset.to_string() << "\"" << TLOG_ENDL;
	std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);

	try
	{
		auto label = pset.get<std::string>("label");
		registered_monitors_[label] = pset;
		if (event_store_ptr_ != nullptr)
		{
			event_store_ptr_->ReconfigureArt(generate_filter_fhicl_());
		}
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered";
		return errmsg.str();
	}

	return "Success";
}

std::string artdaq::DispatcherCore::unregister_monitor(std::string const& label)
{
	TLOG_DEBUG(name_) << "DispatcherCore::unregister_monitor called with argument \"" << label << "\"" << TLOG_ENDL;
	std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);

	try
	{
		if (registered_monitors_.count(label) == 0)
		{
			std::stringstream errmsg;
			errmsg << "Warning in DispatcherCore::unregister_monitor: unable to find requested transfer plugin with "
				<< "label \"" << label << "\"";
			TLOG_WARNING(name_) << errmsg.str() << TLOG_ENDL;
			return errmsg.str();
		}

		registered_monitors_.erase(label);
		if (event_store_ptr_ != nullptr)
		{
			event_store_ptr_->ReconfigureArt(generate_filter_fhicl_());
		}
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Unable to unregister transfer plugin with label \"" << label << "\"";
		return errmsg.str();
	}

	return "Success";
}

fhicl::ParameterSet artdaq::DispatcherCore::generate_filter_fhicl_()
{
	fhicl::ParameterSet generated_pset;

	/**
	 * \todo Magic code to generate proper art configuration for monitors!
	 */

	return generated_pset;
}