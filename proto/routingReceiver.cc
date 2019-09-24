#define TRACE_NAME "RoutingReceiver"
#include "artdaq/DAQdata/Globals.hh"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <chrono>
#include <thread>
#include "artdaq/Application/LoadParameterSet.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQrate/detail/RoutingPacket.hh"
#include "artdaq/TransferPlugins/detail/HostMap.hh"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/OptionalTable.h"
#include "fhiclcpp/types/TableFragment.h"
#include "proto/artdaqapp.hh"

namespace artdaq {
/**
	 * \brief Class which receives routing tables and prints updates
	 */
class RoutingReceiver
{
public:
	/// Accepted configuration parameters for RoutingReceiver
	struct Config
	{
		/// "collection_time_ms": Time to collect routing table updates between printing summaries
		fhicl::Atom<size_t> collection_time_ms{fhicl::Name{"collection_time_ms"}, fhicl::Comment{"Time to collect routing table updates between printing summaries"}, 1000};
		/// "print_verbose_info" (Default: true): Print verbose information about each receiver detected in routing tables
		fhicl::Atom<bool> print_verbose_info{fhicl::Name{"print_verbose_info"}, fhicl::Comment{"Print verbose information about each receiver detected in routing tables"}, true};
		/// "graph_width": Width of the summary graph
		fhicl::Atom<size_t> graph_width{fhicl::Name{"graph_width"}, fhicl::Comment{"Width of the summary graph"}, 40};
		fhicl::TableFragment<artdaq::artdaqapp::Config> artdaqAppConfig;  ///< Configuration for artdaq Application (BoardReader, etc)
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
		 * \brief RoutingReceiver Constructor
		 * \param pset ParameterSet used to configure RoutingReceiver (see RoutingReceiver::Config)
		 */
	explicit RoutingReceiver(fhicl::ParameterSet const& pset)
	    : should_stop_(false)
	    , table_socket_(-1)
	    , routing_table_last_(0)
	{
		TLOG(TLVL_DEBUG) << "Received pset: " << pset.to_string();

		// Validate parameters

		auto rmConfig = pset.get<fhicl::ParameterSet>("routing_table_config", fhicl::ParameterSet());
		use_routing_master_ = rmConfig.get<bool>("use_routing_master", false);
		table_port_ = rmConfig.get<int>("table_update_port", 35556);
		table_address_ = rmConfig.get<std::string>("table_update_address", "227.128.12.28");

		host_map_ = MakeHostMap(pset);

		if (use_routing_master_) startTableReceiverThread_();
	}

	/**
		 * \brief RoutingReceiver Destructor
		 */
	~RoutingReceiver()
	{
		TLOG(TLVL_DEBUG) << "Shutting down RoutingReceiver BEGIN";
		should_stop_ = true;
		if (routing_thread_.joinable()) routing_thread_.join();
		TLOG(TLVL_DEBUG) << "Shutting down RoutingReceiver END.";
	}

	/**
		 * \brief Get the current routing table
		 * \return A snapshot of the current routing table
		 */
	std::map<Fragment::sequence_id_t, int> GetRoutingTable()
	{
		std::unique_lock<std::mutex> lk(routing_mutex_);
		std::map<Fragment::sequence_id_t, int> routing_table_copy(routing_table_);
		return routing_table_copy;
	}

	/**
	 * \brief Get the current routing table, additionally clearing all entries
	 * \return A snapshot of the current routing table
		 */
	std::map<Fragment::sequence_id_t, int> GetAndClearRoutingTable()
	{
		std::unique_lock<std::mutex> lk(routing_mutex_);
		std::map<Fragment::sequence_id_t, int> routing_table_copy(routing_table_);
		routing_table_.clear();
		return routing_table_copy;
	}

	/**
		 * \brief Get the host map
		 * \return The host map, relating ranks to hostnames
		 */
	hostMap_t GetHostMap() { return host_map_; }

private:
	void setupTableListener_()
	{
		int sts;
		table_socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
		if (table_socket_ < 0)
		{
			TLOG(TLVL_ERROR) << "Error creating socket for receiving table updates!";
			exit(1);
		}

		struct sockaddr_in si_me_request;

		int yes = 1;
		if (setsockopt(table_socket_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		{
			TLOG(TLVL_ERROR) << " Unable to enable port reuse on request socket";
			exit(1);
		}
		memset(&si_me_request, 0, sizeof(si_me_request));
		si_me_request.sin_family = AF_INET;
		si_me_request.sin_port = htons(table_port_);
		//si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
		struct in_addr in_addr_s;
		sts = inet_aton(table_address_.c_str(), &in_addr_s);
		if (sts == 0)
		{
			TLOG(TLVL_ERROR) << "inet_aton says table_address " << table_address_ << " is invalid";
		}
		si_me_request.sin_addr.s_addr = in_addr_s.s_addr;
		if (bind(table_socket_, (struct sockaddr*)&si_me_request, sizeof(si_me_request)) == -1)
		{
			TLOG(TLVL_ERROR) << "Cannot bind request socket to port " << table_port_;
			exit(1);
		}

		struct ip_mreq mreq;
		sts = ResolveHost(table_address_.c_str(), mreq.imr_multiaddr);
		if (sts == -1)
		{
			TLOG(TLVL_ERROR) << "Unable to resolve multicast address for table updates";
			exit(1);
		}
		mreq.imr_interface.s_addr = htonl(INADDR_ANY);
		if (setsockopt(table_socket_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
		{
			TLOG(TLVL_ERROR) << "Unable to join multicast group";
			exit(1);
		}
	}
	void startTableReceiverThread_()
	{
		if (routing_thread_.joinable()) routing_thread_.join();
		TLOG(TLVL_INFO) << "Starting Routing Thread";
		try
		{
			routing_thread_ = boost::thread(&RoutingReceiver::receiveTableUpdatesLoop_, this);
		}
		catch (const boost::exception& e)
		{
			TLOG(TLVL_ERROR) << "Caught boost::exception starting Routing Table Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
			std::cerr << "Caught boost::exception starting Routing Table Receive thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
			exit(5);
		}
	}
	void receiveTableUpdatesLoop_()
	{
		while (true)
		{
			if (should_stop_)
			{
				TLOG(TLVL_DEBUG) << __func__ << ": should_stop is " << std::boolalpha << should_stop_ << ", stopping";
				return;
			}

			TLOG(TLVL_TRACE) << __func__ << ": Polling table socket for new routes";
			if (table_socket_ == -1)
			{
				TLOG(TLVL_DEBUG) << __func__ << ": Opening table listener socket";
				setupTableListener_();
			}
			if (table_socket_ == -1)
			{
				TLOG(TLVL_DEBUG) << __func__ << ": The listen socket was not opened successfully.";
				return;
			}

			struct pollfd fd;
			fd.fd = table_socket_;
			fd.events = POLLIN | POLLPRI;

			auto res = poll(&fd, 1, 1000);
			if (res > 0)
			{
				auto first = artdaq::Fragment::InvalidSequenceID;
				auto last = artdaq::Fragment::InvalidSequenceID;
				std::vector<uint8_t> buf(MAX_ROUTING_TABLE_SIZE);
				artdaq::detail::RoutingPacketHeader hdr;

				TLOG(TLVL_DEBUG) << __func__ << ": Going to receive RoutingPacketHeader";
				struct sockaddr_in from;
				socklen_t len = sizeof(from);
				auto stss = recvfrom(table_socket_, &buf[0], MAX_ROUTING_TABLE_SIZE, 0, (struct sockaddr*)&from, &len);
				TLOG(TLVL_DEBUG) << __func__ << ": Received " << stss << " bytes from " << inet_ntoa(from.sin_addr) << ":" << from.sin_port;

				if (stss > static_cast<ssize_t>(sizeof(hdr)))
				{
					memcpy(&hdr, &buf[0], sizeof(artdaq::detail::RoutingPacketHeader));
				}
				else
				{
					TLOG(TLVL_TRACE) << __func__ << ": Incorrect size received. Discarding.";
					continue;
				}

				TRACE(TLVL_DEBUG, "receiveTableUpdatesLoop_: Checking for valid header with nEntries=%lu headerData:0x%016lx%016lx", hdr.nEntries, ((unsigned long*)&hdr)[0], ((unsigned long*)&hdr)[1]);
				if (hdr.header != ROUTING_MAGIC)
				{
					TLOG(TLVL_TRACE) << __func__ << ": non-RoutingPacket received. No ROUTING_MAGIC. size(bytes)=" << stss;
				}
				else
				{
					artdaq::detail::RoutingPacket buffer(hdr.nEntries);
					assert(static_cast<size_t>(stss) == sizeof(artdaq::detail::RoutingPacketHeader) + sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);
					memcpy(&buffer[0], &buf[sizeof(artdaq::detail::RoutingPacketHeader)], sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);
					TRACE(6, "receiveTableUpdatesLoop_: Received a packet of %ld bytes. 1st 16 bytes: 0x%016lx%016lx", stss, ((unsigned long*)&buffer[0])[0], ((unsigned long*)&buffer[0])[1]);

					first = buffer[0].sequence_id;
					last = buffer[buffer.size() - 1].sequence_id;

					if (first + hdr.nEntries - 1 != last)
					{
						TLOG(TLVL_ERROR) << __func__ << ": Skipping this RoutingPacket because the first (" << first << ") and last (" << last << ") entries are inconsistent (sz=" << hdr.nEntries << ")!";
						continue;
					}
					auto thisSeqID = first;

					{
						std::unique_lock<std::mutex> lck(routing_mutex_);
						if (routing_table_.count(last) == 0)
						{
							for (auto entry : buffer)
							{
								if (thisSeqID != entry.sequence_id)
								{
									TLOG(TLVL_ERROR) << __func__ << ": Aborting processing of this RoutingPacket because I encountered an inconsistent entry (seqid=" << entry.sequence_id << ", expected=" << thisSeqID << ")!";
									last = thisSeqID - 1;
									break;
								}
								thisSeqID++;
								if (routing_table_.count(entry.sequence_id))
								{
									if (routing_table_[entry.sequence_id] != entry.destination_rank)
									{
										TLOG(TLVL_ERROR) << __func__ << ": Detected routing table corruption! Recevied update specifying that sequence ID " << entry.sequence_id
										                 << " should go to rank " << entry.destination_rank << ", but I had already been told to send it to " << routing_table_[entry.sequence_id] << "!"
										                 << " I will use the original value!";
									}
									continue;
								}
								if (entry.sequence_id < routing_table_last_) continue;
								routing_table_[entry.sequence_id] = entry.destination_rank;
								TLOG(TLVL_DEBUG) << __func__ << ": (my_rank=" << my_rank << ") received update: SeqID " << entry.sequence_id
								                 << " -> Rank " << entry.destination_rank;
							}
						}

						TLOG(TLVL_DEBUG) << __func__ << ": There are now " << routing_table_.size() << " entries in the Routing Table";
						if (routing_table_.size() > 0) TLOG(TLVL_DEBUG) << __func__ << ": Last routing table entry is seqID=" << routing_table_.rbegin()->first;

						auto counter = 0;
						for (auto& entry : routing_table_)
						{
							TLOG(45) << "Routing Table Entry" << counter << ": " << entry.first << " -> " << entry.second;
							counter++;
						}
					}

					if (last > routing_table_last_) routing_table_last_ = last;
				}
			}
		}
	}

private:
	bool use_routing_master_;
	std::atomic<bool> should_stop_;
	int table_port_;
	std::string table_address_;
	int table_socket_;
	std::map<Fragment::sequence_id_t, int> routing_table_;
	Fragment::sequence_id_t routing_table_last_;
	mutable std::mutex routing_mutex_;
	boost::thread routing_thread_;
	hostMap_t host_map_;
};
}  // namespace artdaq

static bool sighandler_init = false;
static bool should_stop = false;
static void signal_handler(int signum)
{
	// Messagefacility may already be gone at this point, TRACE ONLY!
	TRACE_STREAMER(TLVL_ERROR, &("routingReceiver")[0], 0, 0, 0) << "A signal of type " << signum << " was caught by routingReceiver. Stopping receive loop!";

	should_stop = true;

	sigset_t set;
	pthread_sigmask(SIG_UNBLOCK, NULL, &set);
	pthread_sigmask(SIG_UNBLOCK, &set, NULL);
}

int main(int argc, char* argv[])
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
			sigaction(signal, NULL, &old_action);

			//If the old handler wasn't SIG_IGN (it's a handler that just
			// "ignore" the signal)
			if (old_action.sa_handler != SIG_IGN)
			{
				struct sigaction action;
				action.sa_handler = signal_handler;
				sigemptyset(&action.sa_mask);
				for (auto sigblk : signals)
				{
					sigaddset(&action.sa_mask, sigblk);
				}
				action.sa_flags = 0;

				//Replace the signal handler of SIGINT with the one described by new_action
				sigaction(signal, &action, NULL);
			}
		}
	}

	fhicl::ParameterSet init_ps = LoadParameterSet<artdaq::RoutingReceiver::Config>(argc, argv, "routingReceiver", "This application receives Routing Tables, and calculates statistics about the usage of the receivers");
	fhicl::ParameterSet config_ps = init_ps.get<fhicl::ParameterSet>("daq", init_ps);
	fhicl::ParameterSet metric_ps = config_ps.get<fhicl::ParameterSet>("metrics", config_ps);
	fhicl::ParameterSet fr_ps = config_ps.get<fhicl::ParameterSet>("fragment_receiver", config_ps);

	artdaq::RoutingReceiver rr(fr_ps);

	auto host_map = rr.GetHostMap();

	size_t collection_time_ms = init_ps.get<size_t>("collection_time_ms", 1000);
	size_t max_graph_width = init_ps.get<size_t>("max_graph_width", 100);
	bool print_verbose = init_ps.get<bool>("print_verbose_info", true);
	bool verbose_clear_screen = init_ps.get<bool>("clear_screen", true);

	auto blue = "\033[34m";
	auto cyan = "\033[36m";
	auto green = "\033[32m";
	auto yellow = "\033[93m";
	auto red = "\033[31m";

	metricMan->initialize(metric_ps, "RoutingReceiver");
	metricMan->do_start();
	if (print_verbose && verbose_clear_screen) std::cout << "\033[2J";

	std::map<int, int> receiver_table = std::map<int, int>();

	while (!should_stop)
	{
		auto start_time = std::chrono::steady_clock::now();

		auto this_table = rr.GetAndClearRoutingTable();

		if (this_table.size() > 0)
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

			if (print_verbose && verbose_clear_screen) std::cout << "\033[;H\033[J";

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
			if (print_verbose) std::cout << verbose_report.str() << std::endl;
		}
		std::this_thread::sleep_until(start_time + std::chrono::milliseconds(collection_time_ms));
	}

	metricMan->do_stop();
	artdaq::Globals::CleanUpGlobals();
}