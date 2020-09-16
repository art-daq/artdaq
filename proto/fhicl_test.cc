// Show that the string representation of numbers in fhicl is not what you'd expect.

#include "fhiclcpp/ParameterSet.h"

#include <iomanip>
#include <iostream>
#include "artdaq/Application/LoadParameterSet.hh"

int main(int argc, char* argv[]) try
{
	struct Config
	{};
	auto pset = LoadParameterSet<Config>(argc, argv, "test_fhicl", "A test application to ensure that FHiCL numeric values are converted properly to/from hexadecimal values");

	for (auto& p : pset.get_all_keys())
	{
		std::cout << "Key " << p << " has string value " << pset.get<std::string>(p)
		          << " and uint64_t value " << pset.get<uint64_t>(p)
		          << " ( hex 0x" << std::hex << pset.get<uint64_t>(p) << " )."
		          << std::endl;
	}

	return 0;
}
catch (...)
{
	return -1;
}
