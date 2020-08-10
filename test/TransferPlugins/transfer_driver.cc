#define TRACE_NAME "transfer_driver"

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/TransferTest.hh"
#include "fhiclcpp/make_ParameterSet.h"

int main(int argc, char* argv[])
try
{
	artdaq::configureMessageFacility("transfer_driver");
	TLOG(TLVL_INFO) << "BEGIN";

	std::cout << "argc:" << argc << std::endl;
	for (int i = 0; i < argc; ++i)
	{
		std::cout << "argv[" << i << "]: " << argv[i] << std::endl;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	}

	if (argc != 3)
	{
		std::cerr << *argv << " requires 2 arguments, " << argc - 1 << " provided" << std::endl;
		std::cerr << "Usage: " << *argv << " <my_rank> <fhicl_document>" << std::endl;
		return 1;
	}

	std::string rankString(argv[1]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	my_rank = std::stoi(rankString); 

	cet::filepath_lookup lookup_policy("FHICL_FILE_PATH");
	fhicl::ParameterSet ps;

	auto fhicl = std::string(argv[2]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	make_ParameterSet(fhicl, lookup_policy, ps);

	if (ps.has_key("partition_number"))
	{
		artdaq::Globals::partition_number_ = ps.get<int>("partition_number");
	}

	artdaq::TransferTest theTest(ps);
	theTest.runTest();

	TLOG(TLVL_INFO) << "END";
	return 0;
}
catch (...)
{
	return -1;
}