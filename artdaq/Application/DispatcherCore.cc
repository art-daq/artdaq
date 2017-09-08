#include <errno.h>
#include <sstream>
#include <iomanip>
#include <bitset>

#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>

#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq/Application/DispatcherCore.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"



artdaq::DispatcherCore::DispatcherCore(int rank, std::string name)
	: DataReceiverCore(rank, name)
{}

artdaq::DispatcherCore::~DispatcherCore()
{
	TLOG_DEBUG(name_) << "Destructor" << TLOG_ENDL;
}

bool artdaq::DispatcherCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "initialize method called with DAQ " << "ParameterSet = \"" << pset.to_string() << "\"." << TLOG_ENDL;

	pset_ = pset;
	pset_.erase("outputs");
	pset_.erase("physics");
	pset_.erase("daq");

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

	broadcast_mode_ = agg_pset.get<bool>("broadcast_mode", true);
	if (broadcast_mode_ && !agg_pset.has_key("broadcast_mode"))
	{
		agg_pset.put<bool>("broadcast_mode", true);
		agg_pset.put<int>("art_analyzer_count", 0);
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

std::string artdaq::DispatcherCore::register_monitor(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "DispatcherCore::register_monitor called with argument \"" << pset.to_string() << "\"" << TLOG_ENDL;
	std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
	
	try
	{
		TLOG_DEBUG(name_) << "Getting unique_label from input ParameterSet" << TLOG_ENDL;
		auto label = pset.get<std::string>("unique_label");
		TLOG_DEBUG(name_) << "Unique label is " << label << TLOG_ENDL;
		if (registered_monitors_.count(label))
		{
			throw cet::exception("DispatcherCore") << "Unique label already exists!";
		}
		
		registered_monitors_[label] = pset;
		if (event_store_ptr_ != nullptr)
		{
			if (broadcast_mode_) {
				fhicl::ParameterSet ps = merge_parameter_sets_(pset_, label, pset);
				TLOG_DEBUG(name_) << "Starting art process with received fhicl" << TLOG_ENDL;
				registered_monitor_pids_[label] = event_store_ptr_->StartArtProcess(ps);
			}
			else
			{
				TLOG_DEBUG(name_) << "Generating new fhicl and reconfiguring art" << TLOG_ENDL;
				event_store_ptr_->ReconfigureArt(generate_filter_fhicl_());
			}
		}
		else
		{
			TLOG_ERROR(name_) << "Unable to add monitor as there is no SharedMemoryEventManager instance!" << TLOG_ENDL;
		}
	}
	catch (const cet::exception& e)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered" << std::endl;
		errmsg << "Exception: " << e.what();
		TLOG_ERROR(name_) << errmsg.str() << TLOG_ENDL;
		return errmsg.str();
	}
	catch (const boost::exception& e)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered" << std::endl;
		errmsg << "Exception: " << boost::diagnostic_information(e);
		TLOG_ERROR(name_) << errmsg.str() << TLOG_ENDL;
		return errmsg.str();
	}
	catch (const std::exception& e)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered" << std::endl;
		errmsg << "Exception: " << e.what();
		TLOG_ERROR(name_) << errmsg.str() << TLOG_ENDL;
		return errmsg.str();
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered";
		TLOG_ERROR(name_) << errmsg.str() << TLOG_ENDL;
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
			if(broadcast_mode_) {
				std::set<pid_t> pids;
				pids.insert(registered_monitor_pids_[label]);
				event_store_ptr_->ShutdownArtProcesses(pids);
				registered_monitor_pids_.erase(label);
			}
			else
			{
				event_store_ptr_->ReconfigureArt(generate_filter_fhicl_());
			}
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

fhicl::ParameterSet artdaq::DispatcherCore::merge_parameter_sets_(fhicl::ParameterSet skel,std::string label, fhicl::ParameterSet pset)
{
	fhicl::ParameterSet generated_pset = skel;
	fhicl::ParameterSet generated_outputs;
	fhicl::ParameterSet generated_physics;
	fhicl::ParameterSet generated_physics_analyzers;
	fhicl::ParameterSet generated_physics_producers;
	fhicl::ParameterSet generated_physics_filters;

	TLOG_DEBUG(name_) << "merge_parameter_sets_: Generating fhicl for monitor " << label << TLOG_ENDL;

	try
	{
		auto path = pset.get<std::vector<std::string>>("path");

		// outputs section
		auto outputs = pset.get<fhicl::ParameterSet>("outputs");
		if (outputs.get_pset_names().size() > 1 || outputs.get_pset_names().size() == 0)
		{
			// Only one output allowed per monitor
		}
		auto output_name = outputs.get_pset_names()[0];
		auto output_pset = outputs.get<fhicl::ParameterSet>(output_name);
		generated_outputs.put(label + output_name, output_pset);
		bool outputInPath = false;
		for (size_t ii = 0; ii < path.size(); ++ii)
		{
			if (path[ii] == output_name)
			{
				path[ii] = label + output_name;
				outputInPath = true;
			}
		}
		if (!outputInPath)
		{
			path.push_back(label + output_name);
		}

		//physics section
		auto physics_pset = pset.get<fhicl::ParameterSet>("physics");
		if (physics_pset.has_key("analyzers"))
		{
			auto analyzers = physics_pset.get<fhicl::ParameterSet>("analyzers");
			for (auto key : analyzers.get_pset_names())
			{
				if (generated_physics_analyzers.has_key(key) && analyzers.get<fhicl::ParameterSet>(key) == generated_physics_analyzers.get<fhicl::ParameterSet>(key))
				{
					// Module is already configured
					continue;
				}
				else if (generated_physics_analyzers.has_key(key))
				{
					// Module already exists with name, rename
					auto newkey = label + key;
					generated_physics_analyzers.put<fhicl::ParameterSet>(newkey, analyzers.get<fhicl::ParameterSet>(key));
					for (size_t ii = 0; ii < path.size(); ++ii)
					{
						if (path[ii] == key)
						{
							path[ii] = newkey;
						}
					}
				}
				else
				{
					generated_physics_analyzers.put<fhicl::ParameterSet>(key, analyzers.get<fhicl::ParameterSet>(key));
				}
			}
		}
		if (physics_pset.has_key("producers"))
		{
			auto producers = physics_pset.get<fhicl::ParameterSet>("producers");
			for (auto key : producers.get_pset_names())
			{
				if (generated_physics_producers.has_key(key) && producers.get<fhicl::ParameterSet>(key) == generated_physics_producers.get<fhicl::ParameterSet>(key))
				{
					// Module is already configured
					continue;
				}
				else if (generated_physics_producers.has_key(key))
				{
					// Module already exists with name, rename
					auto newkey = label + key;
					generated_physics_producers.put<fhicl::ParameterSet>(newkey, producers.get<fhicl::ParameterSet>(key));
					for (size_t ii = 0; ii < path.size(); ++ii)
					{
						if (path[ii] == key)
						{
							path[ii] = newkey;
						}
					}
				}
				else
				{
					generated_physics_producers.put<fhicl::ParameterSet>(key, producers.get<fhicl::ParameterSet>(key));
				}
			}
		}
		if (physics_pset.has_key("filters"))
		{
			auto filters = physics_pset.get<fhicl::ParameterSet>("filters");
			for (auto key : filters.get_pset_names())
			{
				if (generated_physics_filters.has_key(key) && filters.get<fhicl::ParameterSet>(key) == generated_physics_filters.get<fhicl::ParameterSet>(key))
				{
					// Module is already configured
					continue;
				}
				else if (generated_physics_filters.has_key(key))
				{
					// Module already exists with name, rename
					auto newkey = label + key;
					generated_physics_filters.put<fhicl::ParameterSet>(newkey, filters.get<fhicl::ParameterSet>(key));
					for (size_t ii = 0; ii < path.size(); ++ii)
					{
						if (path[ii] == key)
						{
							path[ii] = newkey;
						}
					}
				}
				else
				{
					generated_physics_filters.put<fhicl::ParameterSet>(key, filters.get<fhicl::ParameterSet>(key));
				}
			}
		}
		generated_physics.put<std::vector<std::string>>(label, path);
	}
	catch (cet::exception e)
	{
		// Error in parsing input fhicl
		TLOG_ERROR(name_) << "merge_parameter_sets_: Error processing input fhicl: " << e.what() << TLOG_ENDL;
	}

	TLOG_DEBUG(name_) << "merge_parameter_sets_: Building final ParameterSet" << TLOG_ENDL;
	generated_pset.put("outputs", generated_outputs);

	generated_physics.put("analyzers", generated_physics_analyzers);
	generated_physics.put("producers", generated_physics_producers);
	generated_physics.put("filters", generated_physics_filters);

	generated_pset.put("physics", generated_physics);

	return generated_pset;
}

fhicl::ParameterSet artdaq::DispatcherCore::generate_filter_fhicl_()
{
	TLOG_DEBUG(name_) << "generate_filter_fhicl_ BEGIN" << TLOG_ENDL;
	fhicl::ParameterSet generated_pset = pset_;
	
	for (auto& monitor : registered_monitors_)
	{
		auto label = monitor.first;
		auto pset = monitor.second;
		generated_pset = merge_parameter_sets_(generated_pset, label, pset);
	}


	TLOG_DEBUG(name_) << "generate_filter_fhicl_ returning ParameterSet: " << generated_pset.to_string() << TLOG_ENDL;
	return generated_pset;
}
