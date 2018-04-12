#deinfe TRACE_NAME "requestReceiver"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include "artdaq/DAQrate/RequestReceiver.hh"
#include "artdaq-core/Utilities/configureMessageFacility.hh"

int main(int argc, char* argv[])
{

	artdaq::configureMessageFacility("requestReceiver");

	std::ostringstream descstr;
	descstr << argv[0]
		<< " <-c <config-file>>";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("config,c", bpo::value<std::string>(), "Configuration file.")
		("help,h", "produce help message");
	bpo::variables_map vm;
	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const & e) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": " << e.what() << "\n";
		return -1;
	}
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 1;
	}
	if (!vm.count("config")) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": no configuration file given.\n"
			<< "For usage and an options list, please do '"
			<< argv[0] << " --help"
			<< "'.\n";
		return 2;
	}

	fhicl::ParameterSet pset;
	if (getenv("FHICL_FILE_PATH") == nullptr) {
		std::cerr
			<< "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
		setenv("FHICL_FILE_PATH", ".", 0);
	}
	cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
	fhicl::make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);

	int rc = 0;

	artdaq::RequestReceiver recvr(pset);
	recvr.startRequestReceiverThread();

	while (true)
	{
		for (auto req : recvr.GetRequests())
		{
			TLOG(TLVL_INFO) << "Received Request for Sequence ID " << std::to_string(req.first) << ", timestamp " << std::to_string(req.second) ;
		}
		recvr.ClearRequests();
		usleep(10000);
	}

	return rc;
}
