#include "BrokenTransferTest.hh"

#define TRACE_NAME "broken_transfer_driver"

#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQdata/Globals.hh"

int main(int argc, char* argv[])
try
{
	artdaq::configureMessageFacility("broken_transfer_driver", true, true);
	TLOG(TLVL_INFO) << "BEGIN";

	auto ps = LoadParameterSet<artdaqtest::BrokenTransferTest::Config>(argc, argv, "broken_transfer_test", "Test for misbehaving transfer plugins");

	if (ps.has_key("partition_number"))
	{
		artdaq::Globals::partition_number_ = ps.get<int>("partition_number");
	}

	artdaqtest::BrokenTransferTest theTest(ps);

	theTest.TestSenderPause();
	theTest.TestReceiverPause();
	theTest.TestSenderReconnect();
	theTest.TestReceiverReconnect();
	theTest.TestReceiverReconnect(5);

	TLOG(TLVL_INFO) << "END";
	return 0;
}
catch (...)
{
	return -1;
}
