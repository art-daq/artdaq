// Show that the string representation of numbers in fhicl is not what you'd expect.

#include "fhiclcpp/ParameterSet.h"

#include <iostream>
#include "LoadParameterSet.hh"

namespace  bpo = boost::program_options;


int main(int argc, char * argv[])
{
	auto pset = LoadParameterSet(argc, argv);

	std::string test_int_string = pset.get<std::string>("test_int");
	std::string test_uint_string = pset.get<std::string>("test_uint");
    std::string test_hex_string = pset.get<std::string>("test_hex");
	int test_int = pset.get<int>("test_int");
	uint64_t test_uint = pset.get<uint64_t>("test_uint");
    uint64_t test_hex = pset.get<uint64_t>("test_hex");

	std::cout << "Int values: " << std::to_string(test_int) << ", " << std::to_string(test_uint) << std::endl;
	std::cout << "String values: " << test_int_string << ", " << test_uint_string << std::endl;
    std::cout << "Test hex string: " << test_hex_string << ", uint64_t: 0x" << std::hex << test_hex << std::endl;
}
