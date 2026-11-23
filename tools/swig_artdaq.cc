#include "swig_artdaq.h"
#include "TRACE/tracemf.h"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQdata/Globals.hh"

swig_artdaq::swig_artdaq(std::string const& config_string)
{
	fhicl::ParameterSet config_ps = LoadParameterSet(config_string);
	app_name = config_ps.get<std::string>("application_name", "external");
	std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id", 0));
	artdaq::configureMessageFacility(mf_app_name.c_str(), config_ps.get<bool>("debug_logging", false), config_ps.get<bool>("log_to_console", true));
	metricMan->initialize(config_ps.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), app_name);
	initialized_ = true;
}

swig_artdaq::~swig_artdaq()
{
	artdaq::Globals::CleanUpGlobals();
	initialized_ = false;
}

void swig_artdaq::send_metric(std::string const& name, int level, std::string const& value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
}

void swig_artdaq::send_metric(std::string const& name, int level, int value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
}

void swig_artdaq::send_metric(std::string const& name, int level, double value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
}

void swig_artdaq::send_sum_metric(std::string const& name, int level, std::string const& value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Accumulate);
}
void swig_artdaq::send_sum_metric(std::string const& name, int level, int value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Accumulate);
}
void swig_artdaq::send_sum_metric(std::string const& name, int level, double value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Accumulate);
}

void swig_artdaq::send_rate_metric(std::string const& name, int level, std::string const& value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Rate);
}
void swig_artdaq::send_rate_metric(std::string const& name, int level, int value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Rate);
}
void swig_artdaq::send_rate_metric(std::string const& name, int level, double value, std::string const& unit)
{
	if (!initialized_) return;
	metricMan->sendMetric(name, value, unit, level, artdaq::MetricMode::Rate);
}

void swig_artdaq::write_error(std::string const& name, std::string const& message)
{
	if (!initialized_) return;
	TLOG(TLVL_ERROR, name) << message;
}

void swig_artdaq::write_warning(std::string const& name, std::string const& message)
{
	if (!initialized_) return;
	TLOG(TLVL_WARNING, name) << message;
}

void swig_artdaq::write_info(std::string const& name, std::string const& message)
{
	if (!initialized_) return;
	TLOG(TLVL_INFO, name) << message;
}

void swig_artdaq::write_debug(std::string const& name, std::string const& message)
{
	if (!initialized_) return;
	TLOG(TLVL_DEBUG, name) << message;
}

void swig_artdaq::write_trace(int level, std::string const& name, std::string const& message)
{
	if (!initialized_) return;
	TLOG(level, name) << message;
}
