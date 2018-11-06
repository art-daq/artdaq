#include "swig_artdaq.h"

#include "artdaq/Application/LoadParameterSet.hh"

swig_artdaq::swig_artdaq(std::string config_string)
{
	fhicl::ParameterSet config_ps = LoadParameterSet(config_string);
	app_name = config_ps.get<std::string>("application_name", "external");
	std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id", 0));
	artdaq::configureMessageFacility(mf_app_name.c_str());
	metricMan->initialize(config_ps.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), app_name);
}

swig_artdaq::~swig_artdaq()
{
	artdaq::Globals::CleanUpGlobals();
}

void swig_artdaq::send_metric(std::string name, int level, std::string value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
}

void swig_artdaq::send_metric(std::string name, int level, int value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
}

void swig_artdaq::send_metric(std::string name, int level, double value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
}


void swig_artdaq::send_sum_metric(std::string name, int level, std::string value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Accumulate);
}
void swig_artdaq::send_sum_metric(std::string name, int level, int value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Accumulate);
}
void swig_artdaq::send_sum_metric(std::string name, int level, double value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Accumulate);
}

void swig_artdaq::send_rate_metric(std::string name, int level, std::string value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::AccumulateAndRate);
}
void swig_artdaq::send_rate_metric(std::string name, int level, int value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::AccumulateAndRate);
}
void swig_artdaq::send_rate_metric(std::string name, int level, double value, std::string unit)
{
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::AccumulateAndRate);
}

void swig_artdaq::write_error(std::string name, std::string message)
{
	TLOG(TLVL_ERROR, name) << message;
}

void swig_artdaq::write_warning(std::string name, std::string message)
{
	TLOG(TLVL_WARNING, name) << message;
}

void swig_artdaq::write_info(std::string name, std::string message)
{
	TLOG(TLVL_INFO, name) << message;
}

void swig_artdaq::write_debug(std::string name, std::string message)
{
	TLOG(TLVL_DEBUG, name) << message;
}

void swig_artdaq::write_trace(int level, std::string name, std::string message)
{
	TLOG(level, name) << message;
}
