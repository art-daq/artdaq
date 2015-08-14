// MetricManager.cc: MetricManager class implementation file
// Author: Eric Flumerfelt
// Last Modified: 11/14/2014
//
// MetricManager loads a user-specified set of plugins, sends them their configuration,
// and sends them data as it is recieved. It also maintains the state of the plugins
// relative to the application state.

#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/DAQrate/MetricManager.hh"
#include "artdaq-utilities/Plugins/makeMetricPlugin.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "fhiclcpp/ParameterSet.h"

#include <sstream>

artdaq::MetricManager::
MetricManager() : metric_plugins_(0), initialized_(false), running_(false) { }

artdaq::MetricManager::~MetricManager()
{
  shutdown();
}

void artdaq::MetricManager::initialize(fhicl::ParameterSet const& pset, std::string prefix)
{
  prefix_ = prefix;
  if(initialized_)
  {
    shutdown();
  }
  mf::LogDebug("MetricManager") << "Confiugring metrics with parameter set:\n" << pset.to_string();
  std::vector<std::string> names = pset.get_pset_keys();
  for(auto name : names)
    {
      try {
      mf::LogDebug("MetricManager") << "Constructing metric plugin with name " << name;
      fhicl::ParameterSet plugin_pset = pset.get<fhicl::ParameterSet>(name);
      metric_plugins_.push_back(makeMetricPlugin(
          plugin_pset.get<std::string>("metricPluginType",""), plugin_pset));
      }
      catch (...) {
	std::stringstream errorstream;
	errorstream << "Exception caught in MetricManager::initialize, error loading plugin with name " << name;
	ExceptionHandler(ExceptionHandlerRethrow::no, errorstream.str());
      }
    }

  initialized_ = true;
}

void artdaq::MetricManager::do_start()
{
  if(!running_) {
    mf::LogDebug("MetricManager") << "Starting MetricManager";
    for(auto & metric : metric_plugins_)
    {
      try{
      metric->startMetrics();
        mf::LogDebug("MetricManager") << "Metric Plugin " << metric->getLibName() << " started.";
      } catch (...) {
	std::stringstream errorstream;
	errorstream << 
	  "Exception caught in MetricManager::do_start(), error starting plugin with name " << 
	  metric->getLibName();
	ExceptionHandler(ExceptionHandlerRethrow::no, errorstream.str());
      }
    }
    running_ = true;
  }
}

void artdaq::MetricManager::do_stop()
{
  if(running_) {
    for(auto & metric : metric_plugins_)
    {
      try {
        metric->stopMetrics();
        mf::LogDebug("MetricManager") << "Metric Plugin " << metric->getLibName() << " stopped.";
      }
      catch(...) {
	std::stringstream errorstream;
	errorstream << 
	  "Exception caught in MetricManager::do_stop(), error stopping plugin with name " << 
	  metric->getLibName();
	ExceptionHandler(ExceptionHandlerRethrow::no, errorstream.str());
      }
    }
    running_ = false;
    mf::LogDebug("MetricManager") << "MetricManager has been stopped.";
  }
}

void artdaq::MetricManager::do_pause() { /*do_stop();*/ }
  void artdaq::MetricManager::do_resume() { /*do_start();*/ }

void artdaq::MetricManager::reinitialize(fhicl::ParameterSet const& pset, std::string prefix)
{
  shutdown();
  initialize(pset, prefix);
}

void artdaq::MetricManager::shutdown()
{
  mf::LogDebug("MetricManager") << "MetricManager is shutting down...";
  do_stop();

  if(initialized_)
  {
    for(auto & i : metric_plugins_)
    {
      try {
        std::string name = i->getLibName();
        i.reset(nullptr);
        mf::LogDebug("MetricManager") << "Metric Plugin " << name << " shutdown.";
      } catch(...) {
	std::stringstream errorstream;
	errorstream << 
	  "Exception caught in MetricManager::shutdown(), error shutting down metric with name " << 
	  i->getLibName();
	ExceptionHandler(ExceptionHandlerRethrow::no, errorstream.str());
      }
    }
    initialized_ = false;
  }
}
