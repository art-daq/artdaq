#define TRACE_NAME "transfer_driver"

#include "artdaq-utilities/Plugins/MakeParameterSet.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/TransferTest.hh"

int main(int argc, char* argv[]) try
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

	auto fhicl = std::string(argv[2]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
	auto ps = artdaq::make_pset(fhicl, lookup_policy);

	if (ps.has_key("partition_number"))
	{
		artdaq::Globals::partition_number_ = ps.get<int>("partition_number");
	}

	artdaq::TransferTest theTest(ps);
	auto ret = theTest.runTest();

	TLOG(TLVL_INFO) << "END: retcode=" << ret;
	return ret;
}
catch (...)
{
	return -1;
}