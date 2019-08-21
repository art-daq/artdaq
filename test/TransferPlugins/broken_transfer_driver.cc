#include "BrokenTransferTest.hh"

#define TRACE_NAME "broken_transfer_driver"

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/LoadParameterSet.hh"

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("broken_transfer_driver", false);
	TLOG(TLVL_INFO) << "BEGIN";

	auto ps = LoadParameterSet<artdaqtest::BrokenTransferTest::Config>(argc, argv, "broken_transfer_test", "Test for misbehaving transfer plugins");

	if (ps.has_key("partition_number")) artdaq::Globals::partition_number_ = ps.get<int>("partition_number");

	artdaqtest::BrokenTransferTest theTest(ps);

	theTest.TestSenderPause();
	theTest.TestReceiverPause();
	theTest.TestSenderReconnect();
	theTest.TestReceiverReconnect();

	TLOG(TLVL_INFO) << "END";
	return 0;
}
