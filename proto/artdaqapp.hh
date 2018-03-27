#ifndef ARTDAQ_PROTO_ARTDAQAPP_HH
#define ARTDAQ_PROTO_ARTDAQAPP_HH

#include "artdaq/DAQdata/Globals.hh"

#include "artdaq/Application/TaskType.hh"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Utilities/configureMessageFacility.hh"
#include "artdaq/BuildInfo/GetPackageBuildInfo.hh"
#include "artdaq/Application/BoardReaderApp.hh"
#include "artdaq/Application/EventBuilderApp.hh"
#include "artdaq/Application/DataLoggerApp.hh"
#include "artdaq/Application/DispatcherApp.hh"
#include "artdaq/Application/RoutingMasterApp.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"

#include <sys/prctl.h>

namespace artdaq {
	class artdaqapp {
	public:
		static void runArtdaqApp(detail::TaskType task, fhicl::ParameterSet const& config_ps)
		{
			app_name = config_ps.get<std::string>("application_name", detail::TaskTypeToString(task));

			if (config_ps.get<bool>("replace_image_name", config_ps.get<bool>("rin", false)))
			{
				int s;
				s = prctl(PR_SET_NAME, app_name.c_str(), NULL, NULL, NULL);
				if (s != 0)
				{
					std::cerr << "Could not replace process image name with " << app_name << "!" << std::endl;
					exit(1);
				}
			}

			std::string mf_app_name = artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));
			artdaq::configureMessageFacility(mf_app_name.c_str());

			if (config_ps.has_key("rank")) {
				my_rank = config_ps.get<int>("rank");
			}
			TLOG_DEBUG(app_name + "Main") << "Setting application name to " << app_name ;

			TLOG_DEBUG(app_name + "Main") << "artdaq version " <<
				artdaq::GetPackageBuildInfo::getPackageBuildInfo().getPackageVersion()
				<< ", built " <<
				artdaq::GetPackageBuildInfo::getPackageBuildInfo().getBuildTimestamp() ;

			artdaq::setMsgFacAppName(app_name, config_ps.get<int>("id"));

			std::unique_ptr<artdaq::Commandable> comm(nullptr);
			switch (task)
			{
			case(detail::BoardReaderTask):
				comm.reset(new BoardReaderApp());
				break;
			case(detail::EventBuilderTask):
				comm.reset(new EventBuilderApp());
				break;
			case(detail::DataLoggerTask):
				comm.reset(new DataLoggerApp());
				break;
			case(detail::DispatcherTask):
				comm.reset(new DispatcherApp());
				break;
			case(detail::RoutingMasterTask):
				comm.reset(new RoutingMasterApp());
				break;
			default:
				return;
			}

			auto auto_run = config_ps.get<bool>("auto_run", false);
			if (auto_run) {
				int run = config_ps.get<int>("run_number", 101);
				uint64_t timeout = config_ps.get<uint64_t>("transition_timeout", 30);
				uint64_t timestamp = 0;

				comm->do_initialize(config_ps, timeout, timestamp);
				comm->do_start(art::RunID(run), timeout, timestamp);

				TLOG_INFO(app_name + "Main") << "Running XMLRPC Commander. To stop, either Control-C or " << std::endl
					<< "xmlrpc http://`hostname`:" << config_ps.get<int>("id") << "/RPC2 daq.stop" << std::endl
					<< "xmlrpc http://`hostname`:" << config_ps.get<int>("id") << "/RPC2 daq.shutdown" ;
			}

			auto commander = artdaq::MakeCommanderPlugin(config_ps, *comm.get());
			commander->run_server();
		}
	};
}

#endif // ARTDAQ_PROTO_ARTDAQAPP_HH
