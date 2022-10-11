#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/DAQdata/Globals.hh"

#include <boost/program_options.hpp>
namespace bpo = boost::program_options;

#include <sstream>

int main(int argc, char* argv[])
try
{
	artdaq::configureMessageFacility("PrintSharedMemory");

	std::ostringstream descstr;
	descstr << *argv
	        << " [-M <shared memory>] <-e>";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("key,M", bpo::value<std::string>(), "Shared Memory to attach to")
		("events,e", "Print event information")
		("help,h", "produce help message");
	bpo::variables_map vm;
	try
	{
		bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const& e)
	{
		std::cerr << "Exception from command line processing in " << *argv
		          << ": " << e.what() << "\n";
		return -1;
	}
	if (vm.count("help") != 0u)
	{
		std::cout << desc << std::endl;
		return 1;
	}

	fhicl::ParameterSet pset;
	if (vm.count("key") != 0u)
	{
		pset.put("shared_memory_key", vm["key"].as<std::string>());
		auto bkey = vm["key"].as<std::string>();
		if (bkey[2] == 'e' && bkey[3] == 'e')
		{
			bkey[2] = 'b';
			bkey[3] = 'b';
			pset.put("broadcast_shared_memory_key", bkey);
			pset.put("read_event_info", true);
		}
		else
		{
			pset.put("read_event_info", vm.count("events") > 0);
		}
	}
	else
	{
		std::cerr << "You must specify a shared_memory_key in FHiCL or provide one on the command line!" << std::endl;
		return -2;
	}

	TLOG() << "Going to read shared memory " << std::showbase << std::hex << pset.get<uint32_t>("shared_memory_key") << " Event Mode: " << std::boolalpha << pset.get<bool>("read_event_info");
	if (pset.get<bool>("read_event_info", false))
	{
		artdaq::SharedMemoryEventReceiver t(pset.get<uint32_t>("shared_memory_key"), pset.get<uint32_t>("broadcast_shared_memory_key", pset.get<uint32_t>("shared_memory_key")));
		std::cout << t.toString() << std::endl;
	}
	else
	{
		artdaq::SharedMemoryManager t(pset.get<uint32_t>("shared_memory_key"), 0, 0, 0);
		std::cout << t.toString() << std::endl;
	}

	return 0;
}
catch (...)
{
	return -1;
}
