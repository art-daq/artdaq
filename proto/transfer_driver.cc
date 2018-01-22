#include "proto/TransferTest.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "fhiclcpp/make_ParameterSet.h"


int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("transfer_driver");
	TLOG_ARB(10, "transfer_driver") << "BEGIN" << TLOG_ENDL;

	std::cout << "argc:" << argc << std::endl;
	for (int i = 0; i < argc; ++i)
	{
		std::cout << "argv[" << i << "]: " << argv[i] << std::endl;
	}

	if (argc != 4)
	{
		std::cerr << argv[0] << " requires 3 arguments, " << argc - 1 << " provided" << std::endl;
		std::cerr << "Usage: " << argv[0] << " <my_rank> <shm_key> <fhicl_document>" << std::endl;
		return 1;
	}

	my_rank = atoi(argv[1]);
	uint32_t key = atoi(argv[2]);

	cet::filepath_lookup lookup_policy("FHICL_FILE_PATH");
	fhicl::ParameterSet ps;

	auto fhicl = std::string(argv[3]);
	make_ParameterSet(fhicl, lookup_policy, ps);

	artdaq::TransferTest theTest(ps, key);
	theTest.runTest();

	TLOG_ARB(11, "transfer_driver") << "END" << TLOG_ENDL;
	return 0;
}
