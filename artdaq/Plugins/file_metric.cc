// FileMetric.h: File Metric Plugin
// Author: Eric Flumerfelt
// Last Modified: 11/06/2014
//
// An implementation of the MetricPlugin for Log Files

#include "artdaq/Plugins/MetricMacros.hh"
#include "fhiclcpp/ParameterSet.h"

#include <fstream>
#include <ctime>
#include <string>
#include <sys/types.h>
#include <unistd.h>

namespace artdaq {
  class FileMetric : public MetricPlugin {
  private:
    std::string outputFile_;
    bool uniquify_file_name_;
    std::ofstream outputStream_;
    std::ios_base::openmode mode_;
    bool stopped_;
  public:
    FileMetric(fhicl::ParameterSet config) : MetricPlugin(config),
				     outputFile_(pset.get<std::string>("fileName","FileMetric.out")),
					     uniquify_file_name_(pset.get<bool>("uniquify", false)),
                                     stopped_(true)
    {
      std::string modeString = pset.get<std::string>("fileMode", "append");
      
      mode_ = std::ofstream::out | std::ofstream::app;
      if(modeString == "Overwrite" || modeString == "Create" || modeString == "Write") {
          mode_ = std::ofstream::out | std::ofstream::trunc;
      }

      if(uniquify_file_name_) {
	std::string unique_id = std::to_string(getpid());
        if(outputFile_.find("%UID%") != std::string::npos) {
	  outputFile_ = outputFile_.replace(outputFile_.find("%UID%"), 5, unique_id);
        }
        else {
	  if(outputFile_.rfind(".") != std::string::npos) {
            outputFile_ = outputFile_.insert(outputFile_.rfind("."), "_" + unique_id);
          }
          else {
	    outputFile_ = outputFile_.append("_" + unique_id);
          }
        }
      }
      openFile_();
      startMetrics();
    }
    ~FileMetric() {
      stopMetrics();
      closeFile_();
    }
    virtual std::string getLibName() { return "file"; }
    virtual void sendMetric_(std::string name, std::string value, std::string unit ) 
    {
      if(!stopped_) {
        const std::time_t result = std::time(NULL);
        outputStream_ << std::ctime(&result) << "FileMetric: " << name << ": " << value << " " << unit << "." << std::endl;
      }
    }
    virtual void sendMetric_(std::string name, int value, std::string unit ) 
    { 
      sendMetric(name, std::to_string(value), unit);
    }
    virtual void sendMetric_(std::string name, double value, std::string unit ) 
    { 
      sendMetric(name, std::to_string(value), unit);
    }
    virtual void sendMetric_(std::string name, float value, std::string unit ) 
    {
      sendMetric(name, std::to_string(value), unit);
    }
    virtual void sendMetric_(std::string name, unsigned long int value, std::string unit ) 
    { 
      sendMetric(name, std::to_string(value), unit);
    }
    virtual void startMetrics_()
    {
      stopped_ = false;
      const std::time_t result = std::time(NULL);
      outputStream_ << std::ctime(&result) << "FileMetric plugin started." << std::endl;
    }
    virtual void stopMetrics_()
    {
      stopped_ = true;
      const std::time_t result = std::time(NULL);
      outputStream_ << std::ctime(&result) << "FileMetric plugin has been stopped!" << std::endl;
    }
  private:
    void openFile_()
    {
        outputStream_.open(outputFile_.c_str(),mode_);
        const std::time_t result = std::time(NULL);
	outputStream_ << std::ctime(&result) << "FileMetric plugin file opened." << std::endl;

    }
    void closeFile_()
    {
        const std::time_t result = std::time(NULL);
	outputStream_ << std::ctime(&result) << "FileMetric closing file stream." << std::endl;
        outputStream_.close();
    }
  };

} //End namespace artdaq

DEFINE_ARTDAQ_METRIC(artdaq::FileMetric)
