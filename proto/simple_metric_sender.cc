#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "fhiclcpp/types/TableFragment.h"
#include <iostream>

struct Config
{
	fhicl::TableFragment<artdaq::MetricManager::Config> metricmanager_config;
};

int main(int argc, char* argv[])
{

	auto config_ps = LoadParameterSet<Config>(argc, argv, "simple_metric_sender", "A simple application that can be used to send artdaq Metrics from the command line.");
	artdaq::MetricManager mm;
	mm.initialize(config_ps, config_ps.get<std::string>("application_name", "SimpleMetric"));
	mm.do_start();

	int level = config_ps.get<int>("metric_level", 1);

	std::cout << "Enter metrics in <name> <value> <units> format. Ctrl-D to end" << std::endl;
	std::string name, unit;
	double value;
	while (std::cin >> name >> value >> unit)
	{
		mm.sendMetric(name, value, unit, level, artdaq::MetricMode::LastPoint);
	}

	mm.do_stop();
}