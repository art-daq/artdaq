#ifndef ARTDAQ_TOOLS_ARTDAQ_H
#define ARTDAQ_TOOLS_ARTDAQ_H

#include "artdaq/DAQdata/Globals.hh"

/**
 * \file swig_artdaq.h This file serves as an entry point for a SWIG module which can be used to send MessageFacility messages or metrics
 */

/**
 * \brief Simple class exposing methods for TRACEing and sending metrics that can be wrapped with SWIG
 */
class swig_artdaq
{
public:
	/**
	 * \brief swig_artdaq constructor
	 * \param config_string FHiCL-formatted configuration, including application_name ("external"), id (0), and metrics ({})
	 */
	explicit swig_artdaq(std::string const& config_string);

	/**
	 * \brief swig_artdaq destructor
	 */
	~swig_artdaq();

	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. Only the last point in a reporting_interval will be sent to the backend.
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_metric(std::string const& name, int level, std::string const& value, std::string const& unit);
	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. Only the last point in a reporting_interval will be sent to the backend.
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_metric(std::string const& name, int level, int value, std::string const& unit);
	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. Only the last point in a reporting_interval will be sent to the backend.
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_metric(std::string const& name, int level, double value, std::string const& unit);

	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. Metric instances will be summed for the configured reporting_intervals.
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_sum_metric(std::string const& name, int level, std::string const& value, std::string const& unit);
	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. Metric instances will be summed for the configured reporting_intervals.
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_sum_metric(std::string const& name, int level, int value, std::string const& unit);
	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. Metric instances will be summed for the configured reporting_intervals.
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_sum_metric(std::string const& name, int level, double value, std::string const& unit);

	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. 
	          Metric instances will be summed for the configured reporting_intervals, and the result will be reported as a rate (sum / reporting interval).
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_rate_metric(std::string const& name, int level, std::string const& value, std::string const& unit);
	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. 
	          Metric instances will be summed for the configured reporting_intervals, and the result will be reported as a rate (sum / reporting interval).
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_rate_metric(std::string const& name, int level, int value, std::string const& unit);
	/** 
	 * \brief Send a metric using the artdaq MetricManager to the configured backends. 
	          Metric instances will be summed for the configured reporting_intervals, and the result will be reported as a rate (sum / reporting interval).
	 * \param name Name of the metric
	 * \param level Level to report at
	 * \param value Value of the metric
	 * \param unit Units of the metric, should be consistent across all calls of the metric with this name
	 */
	void send_rate_metric(std::string const& name, int level, double value, std::string const& unit);

	/**
	 * \brief Write an error message to TRACE, using the TLVL_ERROR level
	 * \param name TRACE name to write to
	 * \param message Message to write
	 */
	void write_error(std::string const& name, std::string const& message);
	/**
	 * \brief Write an error message to TRACE, using the TLVL_WARNING level
	 * \param name TRACE name to write to
	 * \param message Message to write
	 */
	void write_warning(std::string const& name, std::string const& message);
	/**
	 * \brief Write an error message to TRACE, using the TLVL_INFO level
	 * \param name TRACE name to write to
	 * \param message Message to write
	 */
	void write_info(std::string const& name, std::string const& message);
	/**
	 * \brief Write an error message to TRACE, using the TLVL_DEBUG level
	 * \param name TRACE name to write to
	 * \param message Message to write
	 */
	void write_debug(std::string const& name, std::string const& message);
	/**
	 * \brief Write an error message to TRACE, using the provided level
	 * \param level TRACE level to write at
	 * \param name TRACE name to write to
	 * \param message Message to write
	 */
	void write_trace(int level, std::string const& name, std::string const& message);
};

#endif  //ARTDAQ_TOOLS_ARTDAQ_H
