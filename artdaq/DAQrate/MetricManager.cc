// MetricManager.cc: MetricManager class implementation file
// Author: Eric Flumerfelt
// Last Modified: 11/14/2014
//
// MetricManager loads a user-specified set of plugins, sends them their configuration,
// and sends them data as it is recieved. It also maintains the state of the plugins
// relative to the application state.

#include "artdaq/DAQrate/MetricManager.hh"
#include "artdaq/Plugins/makeMetricPlugin.hh"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "fhiclcpp/ParameterSet.h"

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
      catch(...) {
        mf::LogWarning("MetricManager") << "Error loading plugin with name " << name;
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
      }
      catch(...) {
        mf::LogWarning("MetricManager") << "Error starting plugin with name " << metric->getLibName();
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
        mf::LogWarning("MetricManager") << "Error stopping plugin with name " << metric->getLibName();
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
      }
      catch(...) {
        mf::LogError("MetricManager") << "Error Shutting down metric with name " << i->getLibName();
      }
    }
    initialized_ = false;
  }
}
