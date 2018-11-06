#ifndef ARTDAQ_TOOLS_ARTDAQ_H
#define ARTDAQ_TOOLS_ARTDAQ_H

#include "artdaq/DAQdata/Globals.hh"

/**
 * \file artdaq.h This file serves as an entry point for a SWIG module which can be used to send MessageFacility messages or metrics
 */

class swig_artdaq {
public:
	explicit swig_artdaq(std::string config_string);
	~swig_artdaq();

	void send_metric(std::string name, int level, std::string value, std::string unit);
	void send_metric(std::string name, int level, int value, std::string unit);
	void send_metric(std::string name, int level, double value, std::string unit);

	void send_sum_metric(std::string name, int level, std::string value, std::string unit);
	void send_sum_metric(std::string name, int level, int value, std::string unit);
	void send_sum_metric(std::string name, int level, double value, std::string unit);

	void send_rate_metric(std::string name, int level, std::string value, std::string unit);
	void send_rate_metric(std::string name, int level, int value, std::string unit);
	void send_rate_metric(std::string name, int level, double value, std::string unit);

	void write_error(std::string name, std::string message);
	void write_warning(std::string name, std::string message);
	void write_info(std::string name, std::string message);
	void write_debug(std::string name, std::string message);
	void write_trace(int level, std::string name, std::string message);
};

#endif //ARTDAQ_TOOLS_ARTDAQ_H
