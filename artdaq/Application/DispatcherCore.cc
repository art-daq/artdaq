#include <bitset>
#include <cerrno>
#include <iomanip>
#include <sstream>

#include <csignal>

#include <boost/algorithm/string.hpp>
#include <boost/exception/all.hpp>
#include <boost/filesystem.hpp>
#include <boost/throw_exception.hpp>
#include <boost/tokenizer.hpp>
#include <utility>

#include "fhiclcpp/ParameterSet.h"

#include "artdaq/DAQdata/Globals.hh"  // include these 2 first -
#define TRACE_NAME (app_name + "_DispatcherCore").c_str()

#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Data/RawEvent.hh"

#include "artdaq/Application/DispatcherCore.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

bool artdaq::DispatcherCore::initialize(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG) << "initialize method called with DAQ "
	                 << "ParameterSet = \"" << pset.to_string() << "\".";

	pset_ = pset;
	// 04-Apr-2019, KAB: added support for art config params to be in an "art" block
	if (pset_.has_key("art"))
	{
		pset_ = pset_.get<fhicl::ParameterSet>("art");
	}
	pset_.erase("outputs");
	pset_.erase("physics");
	pset_.erase("daq");

	TLOG(TLVL_DEBUG) << "Pieces of the input pset that are saved for later: \"" << pset_.to_string() << "\".";

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
		agg_pset = daq_pset.get<fhicl::ParameterSet>("dispatcher", daq_pset.get<fhicl::ParameterSet>("aggregator"));
	}
	catch (...)
	{
		TLOG(TLVL_ERROR)
		    << "Unable to find the Dispatcher parameters in the DAQ "
		    << "initialization ParameterSet: \"" + daq_pset.to_string() + "\".";
		return false;
	}

	broadcast_mode_ = agg_pset.get<bool>("broadcast_mode", true);
	if (broadcast_mode_ && !agg_pset.has_key("broadcast_mode"))
	{
		agg_pset.put<bool>("broadcast_mode", true);
		if (!agg_pset.has_key("non_reliable_mode"))
		{
			agg_pset.put<bool>("non_reliable_mode", true);
		}
		if (!agg_pset.has_key("non_reliable_mode_retry_count"))
		{
			agg_pset.put<int>("non_reliable_mode_retry_count", 2);
		}
	}

	agg_pset.erase("restart_crashed_art_processes");
	agg_pset.put<bool>("restart_crashed_art_processes", false);

	agg_pset.erase("art_analyzer_count");
	agg_pset.put<int>("art_analyzer_count", 0);

	// initialize the MetricManager and the names of our metrics
	fhicl::ParameterSet metric_pset;

	try
	{
		metric_pset = daq_pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...)
	{}  // OK if there's no metrics table defined in the FHiCL

	return initializeDataReceiver(pset, agg_pset, metric_pset);
}

std::string artdaq::DispatcherCore::register_monitor(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG) << "DispatcherCore::register_monitor called with argument \"" << pset.to_string() << "\"";
	check_filters_();

	try
	{
		TLOG(TLVL_DEBUG) << "Getting unique_label from input ParameterSet";
		auto const& label = pset.get<std::string>("unique_label");
		TLOG(TLVL_DEBUG) << "Unique label is " << label;
		{
			std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
			if (registered_monitors_.count(label) != 0u)
			{
				throw cet::exception("DispatcherCore") << "Unique label already exists!";  // NOLINT(cert-err60-cpp)
			}

			registered_monitors_[label] = pset;
		}

		// ELF, Jul 21, 2020: This can take a long time, and we don't want to block the XMLRPC thread
		boost::thread thread([=] { start_art_process_(label); });
		thread.detach();
	}
	catch (const cet::exception& e)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered" << std::endl;
		errmsg << "Exception: " << e.what();
		TLOG(TLVL_ERROR) << errmsg.str();
		return errmsg.str();
	}
	catch (const boost::exception& e)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered" << std::endl;
		errmsg << "Exception: " << boost::diagnostic_information(e);
		TLOG(TLVL_ERROR) << errmsg.str();
		return errmsg.str();
	}
	catch (const std::exception& e)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered" << std::endl;
		errmsg << "Exception: " << e.what();
		TLOG(TLVL_ERROR) << errmsg.str();
		return errmsg.str();
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Unable to create a Transfer plugin with the FHiCL code \"" << pset.to_string() << "\", a new monitor has not been registered";
		TLOG(TLVL_ERROR) << errmsg.str();
		return errmsg.str();
	}

	TLOG(TLVL_DEBUG) << "Successfully registered monitor";
	return "Success";
}

std::string artdaq::DispatcherCore::unregister_monitor(std::string const& label)
{
	TLOG(TLVL_DEBUG) << "DispatcherCore::unregister_monitor called with argument \"" << label << "\"";
	check_filters_();

	try
	{
		{
			std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
			if (registered_monitors_.count(label) == 0)
			{
				std::stringstream errmsg;
				errmsg << "Warning in DispatcherCore::unregister_monitor: unable to find requested transfer plugin with "
				       << "label \"" << label << "\"";
				TLOG(TLVL_WARNING) << errmsg.str();
				return errmsg.str();
			}

			registered_monitors_.erase(label);
		}

		// ELF, Jul 21, 2020: This can take a long time, and we don't want to block the XMLRPC thread
		boost::thread thread([=] { stop_art_process_(label); });
		thread.detach();
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Unable to unregister transfer plugin with label \"" << label << "\"";
		TLOG(TLVL_ERROR) << errmsg.str();
		return errmsg.str();
	}

	TLOG(TLVL_DEBUG) << "unregister_monitor completed successfully";
	return "Success";
}

fhicl::ParameterSet artdaq::DispatcherCore::merge_parameter_sets_(fhicl::ParameterSet const& skel, const std::string& label, const fhicl::ParameterSet& pset)
{
	fhicl::ParameterSet generated_pset(skel);
	fhicl::ParameterSet generated_outputs;
	fhicl::ParameterSet generated_physics;
	fhicl::ParameterSet generated_physics_analyzers;
	fhicl::ParameterSet generated_physics_producers;
	fhicl::ParameterSet generated_physics_filters;
	std::unordered_map<std::string, std::vector<std::string>> generated_physics_filter_paths;

	TLOG(TLVL_DEBUG) << "merge_parameter_sets_: Generating fhicl for monitor " << label;

	try
	{
		auto path = pset.get<std::vector<std::string>>("path");

		auto filters = pset.get<std::vector<fhicl::ParameterSet>>("filter_paths", std::vector<fhicl::ParameterSet>());
		for (auto& filter : filters)
		{
			try
			{
				auto name = filter.get<std::string>("name");
				auto path = filter.get<std::vector<std::string>>("path");
				if (generated_physics_filter_paths.count(name) != 0u)
				{
					bool matched = generated_physics_filter_paths[name].size() == path.size();
					for (size_t ii = 0; matched && ii < generated_physics_filter_paths[name].size(); ++ii)
					{
						matched = matched && path[ii] == generated_physics_filter_paths[name][ii];
					}

					if (matched)
					{
						// Path is already configured
						continue;
					}

					auto newname = label + name;
					generated_physics_filter_paths[newname] = path;
				}
				else
				{
					generated_physics_filter_paths[name] = path;
				}
			}
			catch (...)
			{
			}
		}

		// outputs section
		auto outputs = pset.get<fhicl::ParameterSet>("outputs");
		if (outputs.get_pset_names().size() > 1 || outputs.get_pset_names().empty())
		{
			// Only one output allowed per monitor
		}
		auto output_name = outputs.get_pset_names()[0];
		auto output_pset = outputs.get<fhicl::ParameterSet>(output_name);
		generated_outputs.put(label + output_name, output_pset);
		bool outputInPath = false;
		for (auto& ii : path)
		{
			if (ii == output_name)
			{
				ii = label + output_name;
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
			for (const auto& key : analyzers.get_pset_names())
			{
				if (generated_physics_analyzers.has_key(key) && analyzers.get<fhicl::ParameterSet>(key) == generated_physics_analyzers.get<fhicl::ParameterSet>(key))
				{
					// Module is already configured
					continue;
				}
				if (generated_physics_analyzers.has_key(key))
				{
					// Module already exists with name, rename
					auto newkey = label + key;
					generated_physics_analyzers.put<fhicl::ParameterSet>(newkey, analyzers.get<fhicl::ParameterSet>(key));
					for (auto& ii : path)
					{
						if (ii == key)
						{
							ii = newkey;
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
			for (const auto& key : producers.get_pset_names())
			{
				if (generated_physics_producers.has_key(key) && producers.get<fhicl::ParameterSet>(key) == generated_physics_producers.get<fhicl::ParameterSet>(key))
				{
					// Module is already configured
					continue;
				}
				if (generated_physics_producers.has_key(key))
				{
					// Module already exists with name, rename
					auto newkey = label + key;
					generated_physics_producers.put<fhicl::ParameterSet>(newkey, producers.get<fhicl::ParameterSet>(key));
					for (auto& ii : path)
					{
						if (ii == key)
						{
							ii = newkey;
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
			for (const auto& key : filters.get_pset_names())
			{
				if (generated_physics_filters.has_key(key) && filters.get<fhicl::ParameterSet>(key) == generated_physics_filters.get<fhicl::ParameterSet>(key))
				{
					// Module is already configured
					continue;
				}
				if (generated_physics_filters.has_key(key))
				{
					// Module already exists with name, rename
					auto newkey = label + key;
					generated_physics_filters.put<fhicl::ParameterSet>(newkey, filters.get<fhicl::ParameterSet>(key));
					for (auto& ii : path)
					{
						if (ii == key)
						{
							ii = newkey;
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
	catch (cet::exception& e)
	{
		// Error in parsing input fhicl
		TLOG(TLVL_ERROR) << "merge_parameter_sets_: Error processing input fhicl: " << e.what();
	}

	TLOG(TLVL_DEBUG) << "merge_parameter_sets_: Building final ParameterSet";
	generated_pset.put("outputs", generated_outputs);

	generated_physics.put("analyzers", generated_physics_analyzers);
	generated_physics.put("producers", generated_physics_producers);
	generated_physics.put("filters", generated_physics_filters);

	for (auto& path : generated_physics_filter_paths)
	{
		generated_physics.put(path.first, path.second);
	}

	generated_pset.put("physics", generated_physics);

	return generated_pset;
}

fhicl::ParameterSet artdaq::DispatcherCore::generate_filter_fhicl_()
{
	TLOG(TLVL_DEBUG) << "generate_filter_fhicl_ BEGIN";
	std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
	fhicl::ParameterSet generated_pset = pset_;

	for (auto& monitor : registered_monitors_)
	{
		auto label = monitor.first;
		auto pset = monitor.second;
		generated_pset = merge_parameter_sets_(generated_pset, label, pset);
	}

	TLOG(TLVL_DEBUG) << "generate_filter_fhicl_ returning ParameterSet: " << generated_pset.to_string();
	return generated_pset;
}

void artdaq::DispatcherCore::check_filters_()
{
	std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
	auto it = registered_monitors_.begin();
	while (it != registered_monitors_.end())
	{
		if (registered_monitor_pids_.count(it->first) != 0u)
		{
			if (!event_store_ptr_)
			{
				registered_monitor_pids_.erase(it->first);
			}
			else
			{
				auto pid = registered_monitor_pids_[it->first];
				auto sts = kill(pid, 0);
				if (sts < 0)
				{
					registered_monitor_pids_.erase(it->first);
				}
			}
		}
		++it;
	}
}

void artdaq::DispatcherCore::start_art_process_(std::string const& label)
{
	if (event_store_ptr_ != nullptr)
	{
		if (broadcast_mode_)
		{
			fhicl::ParameterSet pset;
			{
				std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
				pset = registered_monitors_[label];
			}
			fhicl::ParameterSet ps = merge_parameter_sets_(pset_, label, pset);
			TLOG(TLVL_DEBUG) << "Starting art process with received fhicl";
			auto pid = event_store_ptr_->StartArtProcess(ps);
			{
				std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
				registered_monitor_pids_[label] = pid;
			}
		}
		else
		{
			TLOG(TLVL_DEBUG) << "Calling ReconfigureArt";
			fhicl::ParameterSet generated_fhicl;
			{
				std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
				generated_fhicl = generate_filter_fhicl_();
			}
			event_store_ptr_->ReconfigureArt(generated_fhicl);
			TLOG(TLVL_DEBUG) << "Done with ReconfigureArt";
		}
	}
	else
	{
		TLOG(TLVL_ERROR) << "Unable to add monitor as there is no SharedMemoryEventManager instance!";
	}
}

void artdaq::DispatcherCore::stop_art_process_(std::string const& label)
{
	if (event_store_ptr_ != nullptr)
	{
		if (broadcast_mode_)
		{
			if (registered_monitor_pids_.count(label) != 0u)
			{
				std::set<pid_t> pids;
				{
					std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
					pids.insert(registered_monitor_pids_[label]);
					registered_monitor_pids_.erase(label);
				}
				TLOG(TLVL_DEBUG) << "Calling ShutdownArtProcesses";
				event_store_ptr_->ShutdownArtProcesses(pids);
				TLOG(TLVL_DEBUG) << "Done with ShutdownArtProcesses";
			}
		}
		else
		{
			TLOG(TLVL_DEBUG) << "Calling ReconfigureArt";
			fhicl::ParameterSet generated_fhicl;
			{
				std::lock_guard<std::mutex> lock(dispatcher_transfers_mutex_);
				generated_fhicl = generate_filter_fhicl_();
			}
			event_store_ptr_->ReconfigureArt(generated_fhicl);
			TLOG(TLVL_DEBUG) << "Done with ReconfigureArt";
		}
	}
}
