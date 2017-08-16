// Show that the string representation of numbers in fhicl is not what you'd expect.

#include "fhiclcpp/ParameterSet.h"

#include <iostream>
#include <iomanip>
#include "LoadParameterSet.hh"

namespace  bpo = boost::program_options;


int main(int argc, char * argv[])
{
	auto pset = LoadParameterSet(argc, argv);

    for(auto& p : pset.get_all_keys()) {
	  std::cout << "Key " << p << " has string value " << pset.get<std::string>(p) 
          << " and uint64_t value " << std::to_string(pset.get<uint64_t>(p)) 
          << " ( hex 0x" << std::hex << pset.get<uint64_t>(p) << " )."
          << std::endl;
    }
}
