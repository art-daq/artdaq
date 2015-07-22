// MetricPlugin.hh: Metric Plugin Interface
// Author: Eric Flumerfelt
// Last Modified: 11/05/2014 (Created)
//
// Defines the interface that any ARTDAQ metric plugin must implement


#ifndef __METRIC_INTERFACE__
#define __METRIC_INTERFACE__

#include <string>
#include <cstdint>
#include <sys/times.h>
#include <unordered_map>
#include "fhiclcpp/ParameterSet.h"

namespace artdaq {
  class MetricPlugin {
  public:
    MetricPlugin(fhicl::ParameterSet const & ps) : pset(ps) {
      runLevel_ = pset.get<int>("level",0);
      accumulationTime_ = pset.get<double>("reporting_interval",15.0);
    }
    virtual ~MetricPlugin() = default;

    ///////////////////////////////////////////////////////////////////////////
    //
    // Interface Functions: These should be reimplemented in plugin classes!
    //
    ///////////////////////////////////////////////////////////////////////////
  public:
    virtual std::string getLibName() { return "ERROR"; }
  protected:
    // These methods dispatch the metric to the metric storage (file, Graphite, Ganglia, etc.)
    // Metric plugins should override them
    virtual void sendMetric_(std::string name, std::string value, std::string unit) =0;
    virtual void sendMetric_(std::string name, int value, std::string unit) =0;
    virtual void sendMetric_(std::string name, double value, std::string unit) =0;
    virtual void sendMetric_(std::string name, float value, std::string unit) =0;
    virtual void sendMetric_(std::string name, long unsigned int value, std::string unit) =0;

    //Run Control -> Clean-up and start-up methods for metric plugins
    virtual void startMetrics_() =0;
    virtual void stopMetrics_() =0;

    /////////////////////////////////////////////////////////////////////////////////
    //
    // Implementation Functions: These should be called from ARTDAQ code!
    //
    /////////////////////////////////////////////////////////////////////////////////
  public:
    // Methods for aggregating metrics. These methods should be called from ARTDAQ and derived code.
    virtual void sendMetric(std::string name, std::string value, std::string unit, bool accumulate = true)
    {
      if(accumulate) {
	// There's no sensible way to accumulate string values, just pass them through...
	sendMetric_(name, value, unit);
      }
      else {
	sendMetric_(name,value,unit);
      }
    }
    virtual void sendMetric(std::string name, int value, std::string unit, bool accumulate = true)
    {
      // 22-Jul-2015, KAB - moved push_back here so that we always get the name
      // added to the map, even if accumulate is false. This helps ensure that a
      // zero is sent at stop time.
      intAccumulator_[name].push_back(value);

      if(!accumulate) { 
        sendMetric_(name, value, unit);
        intAccumulator_[name].clear();
        struct tms ctime;
        lastSendTime_[name] = times(&ctime);
        return;
      }
  
      clock_t thisSendTime = 0;
      double deltaT = _timeSinceLastSend(thisSendTime, name);
      if(deltaT >= accumulationTime_)
	{
	  double sendValue = 0;
	  for(auto val : intAccumulator_[name])
	    {
	      sendValue += val / (double)intAccumulator_[name].size();
	    }

	  sendMetric_(name, sendValue, unit);

	  intAccumulator_[name].clear();
	  lastSendTime_[name] = thisSendTime;
	}
    }
    virtual void sendMetric(std::string name, double value, std::string unit, bool accumulate = true)
    {
      // 22-Jul-2015, KAB - moved push_back here so that we always get the name
      // added to the map, even if accumulate is false. This helps ensure that a
      // zero is sent at stop time.
      doubleAccumulator_[name].push_back(value);

      if(!accumulate) { 
        sendMetric_(name, value, unit);
        doubleAccumulator_[name].clear();
        struct tms ctime;
        lastSendTime_[name] = times(&ctime);
        return;
      }

      clock_t thisSendTime = 0;
      double deltaT = _timeSinceLastSend(thisSendTime, name);
      if(deltaT >= 15)
	{
	  double sendValue = 0;
	  for(auto val : doubleAccumulator_[name])
	    {
	      sendValue += val / doubleAccumulator_[name].size();
	    }
	  
	  sendMetric_(name, sendValue, unit);
      
	  doubleAccumulator_[name].clear();
	  lastSendTime_[name] = thisSendTime;
	}
    }
    virtual void sendMetric(std::string name, float value, std::string unit, bool accumulate = true)
    {
      // 22-Jul-2015, KAB - moved push_back here so that we always get the name
      // added to the map, even if accumulate is false. This helps ensure that a
      // zero is sent at stop time.
      floatAccumulator_[name].push_back(value);

      if(!accumulate) { 
        sendMetric_(name, value, unit);
        floatAccumulator_[name].clear();
        struct tms ctime;
        lastSendTime_[name] = times(&ctime);
        return;
      }

      clock_t thisSendTime = 0;
      double deltaT = _timeSinceLastSend(thisSendTime, name);
      if(deltaT >= 15)
	{
	  float sendValue = 0;
	  for(auto val : floatAccumulator_[name])
	    {
	      sendValue += val / floatAccumulator_[name].size();
	    }
	  
	  sendMetric_(name, sendValue, unit);

	  floatAccumulator_[name].clear();
	  lastSendTime_[name] = thisSendTime;
	}
    }
    virtual void sendMetric(std::string name, long unsigned int value, std::string unit, bool accumulate = true)
    {
      // 22-Jul-2015, KAB - moved push_back here so that we always get the name
      // added to the map, even if accumulate is false. This helps ensure that a
      // zero is sent at stop time.
      uint32_t uvalue = static_cast<uint32_t>(value);
      uintAccumulator_[name].push_back(uvalue);

      if(!accumulate) { 
        sendMetric_(name, value, unit);
        uintAccumulator_[name].clear();
        struct tms ctime;
        lastSendTime_[name] = times(&ctime);
        return;
      }

      clock_t thisSendTime = 0;
      double deltaT = _timeSinceLastSend(thisSendTime, name);
      if(deltaT >= accumulationTime_)
	{
	  double sendValue = 0;
	  for(auto val : uintAccumulator_[name])
	    {
	      sendValue += val / (double)uintAccumulator_[name].size();
	    }
      
	  sendMetric_(name, sendValue, unit);
      
	  uintAccumulator_[name].clear();
	  lastSendTime_[name] = thisSendTime;
	}
    }


    //Run Control
    virtual void startMetrics() { startMetrics_(); }
    virtual void stopMetrics()
    {
      for(auto dv : doubleAccumulator_)
	{
	  static_cast<std::vector<double>>(dv.second).clear();
	  lastSendTime_[dv.first] = CLOCKS_PER_SEC * (accumulationTime_ + 1);
          // 22-Jul-2015, KAB - added cast to get correct call and false to get immediate zero
	  sendMetric(dv.first,(double)0.0,"",false);
	}
      for(auto iv : intAccumulator_)
	{
	  static_cast<std::vector<int>>(iv.second).clear();
	  lastSendTime_[iv.first] = CLOCKS_PER_SEC * (accumulationTime_ + 1);
          // 22-Jul-2015, KAB - added cast to get correct call and false to get immediate zero
	  sendMetric(iv.first,(int)0,"",false);
	}
      for(auto fv : floatAccumulator_)
	{
	  static_cast<std::vector<float>>(fv.second).clear();
	  lastSendTime_[fv.first] = CLOCKS_PER_SEC * (accumulationTime_ + 1);
          // 22-Jul-2015, KAB - added cast to get correct call and false to get immediate zero
	  sendMetric(fv.first,(float)0.0,"",false);
	}
      for(auto uv : uintAccumulator_)
	{
	  static_cast<std::vector<uint32_t>>(uv.second).clear();
	  lastSendTime_[uv.first] = CLOCKS_PER_SEC * (accumulationTime_ + 1);
          // 22-Jul-2015, KAB - added cast to get correct call and false to get immediate zero
	  sendMetric(uv.first,(long unsigned int)0,"",false);
	}
      stopMetrics_();
    }

    void setRunLevel(int level) { runLevel_ = level; }
    int getRunLevel() { return runLevel_; }

  protected:
    int runLevel_;
    fhicl::ParameterSet pset;
    double accumulationTime_;

  private:
    std::unordered_map<std::string, std::vector<double> >   doubleAccumulator_;
    std::unordered_map<std::string, std::vector<int> >      intAccumulator_;
    std::unordered_map<std::string, std::vector<float> >    floatAccumulator_;
    std::unordered_map<std::string, std::vector<uint32_t> > uintAccumulator_;
    std::unordered_map<std::string, clock_t> lastSendTime_;
    double _timeSinceLastSend(clock_t& currenttime, std::string name )
    {
      struct tms ctime;
      currenttime = times(&ctime);
      double deltaw = ((double)(currenttime - lastSendTime_[name]))*10000./CLOCKS_PER_SEC;
      return deltaw;
    }
};

} //End namespace artdaq

#endif //End ifndef __METRIC_INTERFACE__
