#define TRACE_NAME "RoutingReceiver"
#include "artdaq/DAQdata/Globals.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include <csignal>
#include <thread>
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/DAQrate/detail/TableReceiver.hh"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/OptionalTable.h"
#include "fhiclcpp/types/TableFragment.h"
#include "proto/artdaqapp.hh"

namespace artdaq {
/**
 * \brief Class which receives routing tables and prints updates
 */
struct RoutingReceiverConfig
{
	/// "collection_time_ms": Time to collect routing table updates between printing summaries
	fhicl::Atom<size_t> collection_time_ms{fhicl::Name{"collection_time_ms"}, fhicl::Comment{"Time to collect routing table updates between printing summaries"}, 1000};
	/// "print_verbose_info" (Default: true): Print verbose information about each receiver detected in routing tables
	fhicl::Atom<bool> print_verbose_info{fhicl::Name{"print_verbose_info"}, fhicl::Comment{"Print verbose information about each receiver detected in routing tables"}, true};
	/// "graph_width": Width of the summary graph
	fhicl::Atom<size_t> graph_width{fhicl::Name{"graph_width"}, fhicl::Comment{"Width of the summary graph"}, 40};
	/// Configuration for the TableReceiver. See artdaq::TableReceiver::Config
	fhicl::OptionalTable<artdaq::TableReceiver::Config> routingTableConfig{fhicl::Name{"routing_table_config"}, fhicl::Comment{"Configuration for the TableReceiver"}};
	fhicl::TableFragment<artdaq::artdaqapp::Config> artdaqAppConfig;  ///< Configuration for artdaq Application (BoardReader, etc)
};

}  // namespace artdaq

static bool sighandler_init = false;
static bool should_stop = false;
static void signal_handler(int signum)
{
	// Messagefacility may already be gone at this point, TRACE ONLY!
#if TRACE_REVNUM < 1459
	TRACE_STREAMER(TLVL_ERROR, &("routingReceiver")[0], 0, 0, 0)
#else
	TRACE_STREAMER(TLVL_ERROR, TLOG2("routingReceiver", 0), 0)
#endif
	    << "A signal of type " << signum << " was caught by routingReceiver. Stopping receive loop!";

	should_stop = true;

	sigset_t set;
	pthread_sigmask(SIG_UNBLOCK, nullptr, &set);
	pthread_sigmask(SIG_UNBLOCK, &set, nullptr);
}

int main(int argc, char* argv[])
try
{
	artdaq::configureMessageFacility("RoutingReceiver", false, false);
	static std::mutex sighandler_mutex;
	std::unique_lock<std::mutex> lk(sighandler_mutex);

	if (!sighandler_init)  //&& manager_id_ == 0) // ELF 3/22/18: Taking out manager_id_==0 requirement as I think kill(getpid()) is enough protection
	{
		sighandler_init = true;
		std::vector<int> signals = {SIGINT, SIGTERM, SIGUSR1, SIGUSR2};  // SIGQUIT is used by art in normal operation
		for (auto signal : signals)
		{
			struct sigaction old_action;
			sigaction(signal, nullptr, &old_action);

			// If the old handler wasn't SIG_IGN (it's a handler that just
			//  "ignore" the signal)
			if (old_action.sa_handler != SIG_IGN)  // NOLINT(cppcoreguidelines-pro-type-cstyle-cast)
			{
				struct sigaction action;
				action.sa_handler = signal_handler;
				sigemptyset(&action.sa_mask);
				for (auto sigblk : signals)
				{
					sigaddset(&action.sa_mask, sigblk);
				}
				action.sa_flags = 0;

				// Replace the signal handler of SIGINT with the one described by new_action
				sigaction(signal, &action, nullptr);
			}
		}
	}

	fhicl::ParameterSet init_ps = LoadParameterSet<artdaq::RoutingReceiverConfig>(argc, argv, "routingReceiver", "This application receives Routing Tables, and calculates statistics about the usage of the receivers");
	auto config_ps = init_ps.get<fhicl::ParameterSet>("daq", init_ps);
	auto metric_ps = config_ps.get<fhicl::ParameterSet>("metrics", config_ps);
	auto fr_ps = config_ps.get<fhicl::ParameterSet>("fragment_receiver", config_ps);
	auto rmConfig = fr_ps.get<fhicl::ParameterSet>("routing_table_config", fhicl::ParameterSet());
	artdaq::TableReceiver rr(rmConfig);

	auto host_map = artdaq::MakeHostMap(fr_ps);

	auto collection_time_ms = init_ps.get<size_t>("collection_time_ms", 1000);
	auto max_graph_width = init_ps.get<size_t>("max_graph_width", 100);
	bool print_verbose = init_ps.get<bool>("print_verbose_info", true);
	bool verbose_clear_screen = init_ps.get<bool>("clear_screen", true);

	auto blue = "\033[34m";
	auto cyan = "\033[36m";
	auto green = "\033[32m";
	auto yellow = "\033[93m";
	auto red = "\033[31m";

	metricMan->initialize(metric_ps, "RoutingReceiver");
	metricMan->do_start();
	if (print_verbose && verbose_clear_screen)
	{
		std::cout << "\033[2J";
	}

	std::map<int, int> receiver_table = std::map<int, int>();

	while (!should_stop)
	{
		auto start_time = std::chrono::steady_clock::now();

		auto this_table = rr.GetAndClearRoutingTable();

		if (!this_table.empty())
		{
			auto graph_width = this_table.size();
			auto n = 1;  // n becomes entries per graph character
			auto graph_width_orig = graph_width;
			while (graph_width > max_graph_width)
			{
				n++;
				graph_width = graph_width_orig / n;
			}

			for (auto& entry : this_table)
			{
				receiver_table[entry.second]++;
			}

			auto average_entries_per_receiver = this_table.size() / receiver_table.size();
			auto offset = 2 * n;  // Offset is 2 characters, in entries

			auto cyan_threshold = ((average_entries_per_receiver - offset) / 2) / n;
			auto green_threshold = (average_entries_per_receiver - offset) / n;
			auto yellow_threshold = (average_entries_per_receiver + offset) / n;
			auto red_threshold = (2 * average_entries_per_receiver) / n;

			TLOG(TLVL_TRACE) << "CT: " << cyan_threshold << ", GT: " << green_threshold << ", YT: " << yellow_threshold << ", RT: " << red_threshold;

			std::ostringstream report;
			std::ostringstream verbose_report;

			if (print_verbose && verbose_clear_screen)
			{
				std::cout << "\033[;H\033[J";
			}

			report << artdaq::TimeUtils::gettimeofday_us() << ": " << this_table.size() << " Entries, ";
			for (auto& receiver : receiver_table)
			{
				auto percent = static_cast<int>(receiver.second * 100 / this_table.size());
				report << receiver.first << ": " << receiver.second << " (" << percent << "%), ";
				if (print_verbose)
				{
					verbose_report << receiver.first << ": " << receiver.second << " (" << percent << "%)\t[";

					size_t graph_characters = receiver.second / n;

					for (size_t ii = 0; ii < graph_characters; ++ii)
					{
						if (ii < cyan_threshold)
						{
							verbose_report << blue;
						}
						else if (ii < green_threshold)
						{
							verbose_report << cyan;
						}
						else if (ii < yellow_threshold)
						{
							verbose_report << green;
						}
						else if (ii < red_threshold)
						{
							verbose_report << yellow;
						}
						else
						{
							verbose_report << red;
						}
						verbose_report << "|";
					}
					std::string spaces = std::string(graph_width - graph_characters, ' ');
					verbose_report << "\033[0m" << spaces << "]" << std::endl;
				}
				receiver.second = 0;
			}
			TLOG(TLVL_INFO) << report.str();
			std::cout << report.str() << std::endl;
			if (print_verbose)
			{
				std::cout << verbose_report.str() << std::endl;
			}
		}
		std::this_thread::sleep_until(start_time + std::chrono::milliseconds(collection_time_ms));
	}

	metricMan->do_stop();
	artdaq::Globals::CleanUpGlobals();

	return 0;
}
catch (...)
{
	return -1;
}
