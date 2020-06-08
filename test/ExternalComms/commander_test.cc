#define TRACE_NAME "commander_test"

#include "artdaq/ExternalComms/CommanderInterface.hh"

#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"

#include <boost/thread.hpp>
#include "artdaq/DAQdata/Globals.hh"

int main(int argc, char** argv)
{
	struct Config
	{
		fhicl::TableFragment<artdaq::CommanderInterface::Config> commanderPluginConfig;  ///< Configuration for the CommanderInterface. See artdaq::CommanderInterface::Config

		fhicl::Atom<std::string> partition_number{fhicl::Name{"partition_number"}, fhicl::Comment{"Partition to run in"}, ""};
	};
	artdaq::configureMessageFacility("commander_test", true, true);
	fhicl::ParameterSet config_ps = LoadParameterSet<Config>(argc, argv, "commander_test", "A test driver for CommanderInterface plugins");

	artdaq::Globals::partition_number_ = config_ps.get<int>("partition_number", 1);

	auto id_rand = seedAndRandom();
	if (config_ps.has_key("id"))
	{
		TLOG(TLVL_DEBUG) << "Ignoring set id and using random!";
		config_ps.erase("id");
	}
	config_ps.put("id", artdaq::Globals::partition_number_ * 1000 + (id_rand % 1000));

	std::unique_ptr<artdaq::Commandable> cmdble(new artdaq::Commandable());

	auto commander = artdaq::MakeCommanderPlugin(config_ps, *cmdble);

	// Start server thread
	boost::thread commanderThread([&] { commander->run_server(); });
	while (!commander->GetStatus()) { usleep(10000);
}
	sleep(1);

	uint64_t arg = 0;
	fhicl::ParameterSet pset;

	TLOG(TLVL_INFO) << "START";

	TLOG(TLVL_DEBUG) << "Sending init";
	std::string sts = commander->send_init(pset, arg, arg);
	TLOG(TLVL_DEBUG) << "init res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "init returned " << sts << ", exiting with error";
		exit(1);
	}

	TLOG(TLVL_DEBUG) << "Sending soft_init";
	sts = commander->send_soft_init(pset, arg, arg);
	TLOG(TLVL_DEBUG) << "soft_init res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "soft_init returned " << sts << ", exiting with error";
		exit(2);
	}

	TLOG(TLVL_DEBUG) << "Sending legal_commands";
	sts = commander->send_legal_commands();
	TLOG(TLVL_DEBUG) << "legal_commands res=" << sts;
	if (sts.empty() || sts.find("Exception", 0) != std::string::npos || sts.find("Error", 0) != std::string::npos)
	{
		TLOG(TLVL_ERROR) << "legal_commands returned " << sts << ", exiting with error";
		exit(3);
	}

	TLOG(TLVL_DEBUG) << "Sending meta_command";
	sts = commander->send_meta_command("test", "test");
	TLOG(TLVL_DEBUG) << "meta_command res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "meta_command returned " << sts << ", exiting with error";
		exit(4);
	}

	TLOG(TLVL_DEBUG) << "Sending report";
	sts = commander->send_report("test");
	TLOG(TLVL_DEBUG) << "report res=" << sts;
	if (sts.empty() || sts.find("Exception", 0) != std::string::npos || sts.find("Error", 0) != std::string::npos)
	{
		TLOG(TLVL_ERROR) << "report returned " << sts << ", exiting with error";
		exit(5);
	}

	TLOG(TLVL_DEBUG) << "Sending start";
	sts = commander->send_start(art::RunID(0x7357), arg, arg);
	TLOG(TLVL_DEBUG) << "start res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "start returned " << sts << ", exiting with error";
		exit(6);
	}

	TLOG(TLVL_DEBUG) << "Sending status";
	sts = commander->send_status();
	TLOG(TLVL_DEBUG) << "status res=" << sts;
	if (sts.empty() || sts.find("Exception", 0) != std::string::npos || sts.find("Error", 0) != std::string::npos)
	{
		TLOG(TLVL_ERROR) << "status returned " << sts << ", exiting with error";
		exit(7);
	}

	TLOG(TLVL_DEBUG) << "Sending pause";
	sts = commander->send_pause(arg, arg);
	TLOG(TLVL_DEBUG) << "pause res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "pause returned " << sts << ", exiting with error";
		exit(8);
	}

	TLOG(TLVL_DEBUG) << "Sending resume";
	sts = commander->send_resume(arg, arg);
	TLOG(TLVL_DEBUG) << "resume res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "resume returned " << sts << ", exiting with error";
		exit(9);
	}

	TLOG(TLVL_DEBUG) << "Sending rollover_subrun";
	sts = commander->send_rollover_subrun(arg, static_cast<uint32_t>(arg));
	TLOG(TLVL_DEBUG) << "rollover_subrun res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "rollover_subrun returned " << sts << ", exiting with error";
		exit(10);
	}

	TLOG(TLVL_DEBUG) << "Sending stop";
	sts = commander->send_stop(arg, arg);
	TLOG(TLVL_DEBUG) << "stop res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "stop returned " << sts << ", exiting with error";
		exit(11);
	}

	TLOG(TLVL_DEBUG) << "Sending reinit";
	sts = commander->send_reinit(pset, arg, arg);
	TLOG(TLVL_DEBUG) << "reinit res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "reinit returned " << sts << ", exiting with error";
		exit(12);
	}

	TLOG(TLVL_DEBUG) << "Sending trace_set";
	sts = commander->send_trace_set("TRACE", "M", "0x7357AAAABBBBCCCC");
	TLOG(TLVL_DEBUG) << "trace_set res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "trace_set returned " << sts << ", exiting with error";
		exit(13);
	}

	TLOG(TLVL_DEBUG) << "Sending trace_get";
	sts = commander->send_trace_get("TRACE");
	TLOG(TLVL_DEBUG) << "trace_get res=" << sts;
	if (sts.empty() || sts.find("Exception", 0) != std::string::npos || sts.find("Error", 0) != std::string::npos)
	{
		TLOG(TLVL_ERROR) << "trace_get returned " << sts << ", exiting with error";
		exit(14);
	}

	TLOG(TLVL_DEBUG) << "Sending register_monitor";
	sts = commander->send_register_monitor("unqiue_label: test");
	TLOG(TLVL_DEBUG) << "register_monitor res=" << sts;
	if (sts.empty() || sts.find("Exception", 0) != std::string::npos || sts.find("Error", 0) != std::string::npos)
	{
		TLOG(TLVL_ERROR) << "register_monitor returned " << sts << ", exiting with error";
		exit(15);
	}

	TLOG(TLVL_DEBUG) << "Sending unregister_monitor";
	sts = commander->send_unregister_monitor("test");
	TLOG(TLVL_DEBUG) << "unregister_monitor res=" << sts;
	if (sts.empty() || sts.find("Exception", 0) != std::string::npos || sts.find("Error", 0) != std::string::npos)
	{
		TLOG(TLVL_ERROR) << "unregister_monitor returned " << sts << ", exiting with error";
		exit(16);
	}

	TLOG(TLVL_DEBUG) << "Sending shutdown";
	sts = commander->send_shutdown(arg);
	TLOG(TLVL_DEBUG) << "shutdown res=" << sts;
	if (sts != "Success")
	{
		TLOG(TLVL_ERROR) << "shutdown returned " << sts << ", exiting with error";
		exit(17);
	}

	TLOG(TLVL_INFO) << "DONE";

	if (commanderThread.joinable()) { commanderThread.join();
}
	artdaq::Globals::CleanUpGlobals();
}
