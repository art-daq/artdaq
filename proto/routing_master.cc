#include "MPIProg.hh"
#include "artdaq/Application/Routing/RoutingPacket.hh"
#include "artdaq/DAQdata/TCPConnect.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include "artdaq/Application/RoutingMasterCore.hh"
#include "artdaq/Application/RoutingMasterApp.hh"
#include <netdb.h>
namespace bpo = boost::program_options;

#include <algorithm>
#include <cmath>
#include <cstdio>

extern "C"
{
#include <unistd.h>
}

#include <iostream>
#include <memory>
#include <utility>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

extern "C"
{
#include <sys/time.h>
#include <sys/resource.h>
}

/**
 * \brief Create a "lock file", removing it upon class destruction
 */
class LockFile
{
public:
	/**
	 * \brief Create a lock file with the given path
	 * \param path Path to lock file
	 */
	explicit LockFile(std::string path) : fileName_(path)
	{
		std::ofstream fstream(fileName_);
		fstream << "Locked" << std::endl;
	}

	/**
	 * \brief LockFile Destructor, removes the lock file
	 */
	~LockFile()
	{
		if(IsLocked(fileName_))	remove(fileName_.c_str());
	}
	/**
	 * \brief Check if the given lock file exists
	 * \param path Path to lock file
	 * \return Whether the lock file exists, as determined by boost::filesystem
	 */
	static bool IsLocked(std::string path)
	{
		return boost::filesystem::exists(path);
	}

private:
	std::string fileName_;
};

/**
 * \brief The RoutingMasterTest class runs the routing_master test
 */
class RoutingMasterTest : public MPIProg
{
public:
	/**
	 * \brief RoutingMasterTest Constructor
	 * \param argc Number of arguments
	 * \param argv Array of argument strings
	 * 
	 * The configuration file should contains the following Parameters:
	 * "daq" (REQUIRED): DAQ configuration
	 *   "policy" (REQUIRED): The RoutingMasterPolicy configuration
	 *     "receiver_ranks" (REQUIRED): Ranks of the Table Receivers
	 * "token_count" (Default: 1000): Number of tokens to generate
	 * "token_interval_us" (Default: 5000): Delay between tokens
	 * 
	 * The configuration file is also passed to RoutingMasterCore, see that class for required configuration parameters
	 */
	RoutingMasterTest(int argc, char* argv[]);

	/**
	 * \brief Start the test, using the role assigned
	 */
	void go();

	/**
	 * \brief Generate tokens and send them to the Routing Master
	 */
	void generate_tokens();

	/**
	 * \brief Load a RoutingMasterCore instance, receive tokens from the token generators, and send table updates to the table receivers
	 */
	void routing_master();

	/**
	 * \brief Receive Routing Tables from the Routing Master and send acknowledgement packets back
	 */
	void table_receiver();

	/**
	 * \brief Parse the command line arguments and load a configuration FHiCL file
	 * \param argc Number of arguments
	 * \param argv Array of argument strings
	 * \return ParameterSet used to configure the test
	 */
	fhicl::ParameterSet getPset(int argc, char* argv[]) const;

private:
	enum class TestRole_t : int
	{
		TOKEN_GEN = 0,
		ROUTING_MASTER = 1,
		TABLE_RECEIVER = 2
	};

	void printHost(const std::string& functionName) const;

	fhicl::ParameterSet const pset_;
	fhicl::ParameterSet const daq_pset_;
	MPI_Comm local_group_comm_;
	TestRole_t role_;

	std::string routing_master_address_;
	std::string multicast_address_;
	int token_port_;
	int table_port_;
	int ack_port_;
	std::vector<int> eb_ranks_;
	int token_count_;
	size_t token_interval_us_;
};

RoutingMasterTest::RoutingMasterTest(int argc, char* argv[]) :
	MPIProg(argc, argv)
	, pset_(getPset(argc, argv))
	, daq_pset_(pset_.get<fhicl::ParameterSet>("daq"))
	, local_group_comm_()
	, routing_master_address_(daq_pset_.get<std::string>("routing_master_hostname", "localhost"))
	, multicast_address_(daq_pset_.get<std::string>("table_update_address", "227.128.12.28"))
	, token_port_(daq_pset_.get<int>("routing_token_port", 35555))
	, table_port_(daq_pset_.get<int>("table_update_port", 35556))
	, ack_port_(daq_pset_.get<int>("table_acknowledge_port", 35557))
	, token_count_(pset_.get<int>("token_count", 1000))
	, token_interval_us_(pset_.get<size_t>("token_interval_us", 5000))
{
	assert(!(my_rank < 0));
	switch (my_rank)
	{
	case 0:
		role_ = TestRole_t::TOKEN_GEN;
		break;
	case 1:
		role_ = TestRole_t::ROUTING_MASTER;
		break;
	default:
		role_ = TestRole_t::TABLE_RECEIVER;
		break;
	}
	auto policy_pset = daq_pset_.get<fhicl::ParameterSet>("policy");
	eb_ranks_ = policy_pset.get<std::vector<int>>("receiver_ranks");

}

fhicl::ParameterSet RoutingMasterTest::getPset(int argc, char* argv[]) const
{
	std::ostringstream descstr;
	descstr << "-- <-c <config-file>>";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("config,c", bpo::value<std::string>(), "Configuration file.");
	bpo::variables_map vm;
	try
	{
		bpo::store(bpo::command_line_parser(argc, argv).
				   options(desc).allow_unregistered().run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const& e)
	{
		std::cerr << "Exception from command line processing in Config::getArtPset: " << e.what() << "\n";
		throw "cmdline parsing error.";
	}
	if (!vm.count("config"))
	{
		std::cerr << "Expected \"-- -c <config-file>\" fhicl file specification.\n";
		throw "cmdline parsing error.";
	}
	fhicl::ParameterSet pset;
	cet::filepath_lookup lookup_policy("FHICL_FILE_PATH");
	fhicl::make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);

	return pset;
}

void RoutingMasterTest::go()
{
	if (LockFile::IsLocked("/tmp/routing_master_t.lock")) return;
	MPI_Barrier(MPI_COMM_WORLD);
	std::unique_ptr<LockFile> lock;
	if (my_rank == 0) {
		lock = std::make_unique<LockFile>("/tmp/routing_master_t.lock");
	}
	//std::cout << "daq_pset_: " << daq_pset_.to_string() << std::endl << "conf_.makeParameterSet(): " << conf_.makeParameterSet().to_string() << std::endl;
	MPI_Comm_split(MPI_COMM_WORLD, static_cast<int>(role_), 0, &local_group_comm_);
	switch (role_)
	{
	case TestRole_t::TABLE_RECEIVER:
		table_receiver();
		break;
	case TestRole_t::ROUTING_MASTER:
		routing_master();
		break;
	case TestRole_t::TOKEN_GEN:
		generate_tokens();
		break;
	default:
		throw "No such node type";
	}
	TLOG_DEBUG("routing_master") << "Rank " << my_rank << " complete." << TLOG_ENDL;
}

void RoutingMasterTest::generate_tokens()
{
	TLOG_DEBUG("generate_tokens") << "Init" << TLOG_ENDL;
	printHost("generate_tokens");
	sleep(1);

	int token_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (!token_socket)
	{
		TLOG_ERROR("generate_tokens") << "I failed to create the socket for sending Routing Tokens!" << TLOG_ENDL;
		exit(1);
	}
	struct sockaddr_in token_addr;
	auto sts = ResolveHost(routing_master_address_.c_str(), token_port_, token_addr);
	if(sts == -1)
	{
		TLOG_ERROR("generate_tokens") << "Could not resolve host name" << TLOG_ENDL;
	}

	connect(token_socket, (struct sockaddr*)&token_addr, sizeof(token_addr));

	int sent_tokens = 0;
	std::map<int, int> token_counter;
	for(auto rank : eb_ranks_)
	{
		token_counter[rank] = 0;
	}
	while (sent_tokens < token_count_) {
		int this_rank = eb_ranks_[rand() % eb_ranks_.size()];
		token_counter[this_rank]++;
		artdaq::detail::RoutingToken token;
		token.header = TOKEN_MAGIC;
		token.rank = this_rank;
		token.new_slots_free = 1;

		TLOG_DEBUG("generate_tokens") << "Sending RoutingToken " << std::to_string(++sent_tokens) << " for rank " << this_rank << " to " << routing_master_address_ << TLOG_ENDL;
		send(token_socket, &token, sizeof(artdaq::detail::RoutingToken), 0);
		usleep(token_interval_us_);
	}
	auto max_rank = 0;
	for(auto rank : token_counter)
	{
		if (rank.second > max_rank) max_rank = rank.second;
	}
	for(auto rank : token_counter)
	{
		artdaq::detail::RoutingToken token;
		token.header = TOKEN_MAGIC;
		token.rank = rank.first;
		token.new_slots_free = max_rank - rank.second;

		TLOG_DEBUG("generate_tokens") << "Sending RoutingToken " << std::to_string(++sent_tokens) << " for rank " << rank.first << " to " << routing_master_address_ << TLOG_ENDL;
		send(token_socket, &token, sizeof(artdaq::detail::RoutingToken), 0);
		usleep(token_interval_us_);
		
	}

	MPI_Comm_free(&local_group_comm_);
	TLOG_INFO("generate_tokens") << "Waiting at MPI_Barrier" << TLOG_ENDL;
	MPI_Barrier(MPI_COMM_WORLD);
	TLOG_INFO("generate_tokens") << "Done with MPI_Barrier" << TLOG_ENDL;
}

void RoutingMasterTest::table_receiver()
{
	TLOG_DEBUG("table_receiver") << "Init" << TLOG_ENDL;
	printHost("table_receiver");


	auto table_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (!table_socket)
	{
		TLOG_ERROR("table_receiver") << "Error creating socket for receiving data requests!" << TLOG_ENDL;
		exit(1);
	}

	struct sockaddr_in si_me_request;

	int yes = 1;
	if (setsockopt(table_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
	{
		TLOG_ERROR("table_receiver") << " Unable to enable port reuse on request socket" << TLOG_ENDL;
		exit(1);
	}
	memset(&si_me_request, 0, sizeof(si_me_request));
	si_me_request.sin_family = AF_INET;
	si_me_request.sin_port = htons(table_port_);
	si_me_request.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(table_socket, (struct sockaddr *)&si_me_request, sizeof(si_me_request)) == -1)
	{
		TLOG_ERROR("table_receiver") << "Cannot bind request socket to port " << table_port_ << TLOG_ENDL;
		exit(1);
	}

	struct ip_mreq mreq;
	long int sts = ResolveHost(multicast_address_.c_str(), mreq.imr_multiaddr);
	if(sts == -1)
	{
		TLOG_ERROR("table_Receiver") << "Unable to resolve multicast hostname" << TLOG_ENDL;
		exit(1);
	}
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	if (setsockopt(table_socket, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
	{
		TLOG_ERROR("table_receiver") << "Unable to join multicast group" << TLOG_ENDL;
		exit(1);
	}

	struct epoll_event ev;
	int table_epoll_fd = epoll_create1(0);
	ev.events = EPOLLIN | EPOLLPRI;
	ev.data.fd = table_socket;
	if (epoll_ctl(table_epoll_fd, EPOLL_CTL_ADD, table_socket, &ev) == -1)
	{
		TLOG_ERROR("table_receiver") << "Could not register listen socket to epoll fd" << TLOG_ENDL;
		exit(3);
	}

	auto ack_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	struct sockaddr_in ack_addr;  
	sts = ResolveHost(routing_master_address_.c_str(), ack_port_, ack_addr);
	if(sts == -1)
	{
		TLOG_ERROR("table_Receiver") << "Unable to resolve routing master hostname" << TLOG_ENDL;
		exit(1);
	}

	if (table_socket == -1 || table_epoll_fd == -1 || ack_socket == -1)
	{
		TLOG_DEBUG("table_receiver") << "One of the listen sockets was not opened successfully." << TLOG_ENDL;
		exit(4);
	}
	artdaq::Fragment::sequence_id_t max_sequence_id = token_count_;
	artdaq::Fragment::sequence_id_t current_sequence_id = 0;
	std::map<artdaq::Fragment::sequence_id_t, int> routing_table;
	TLOG_INFO("table_receiver") << "Expecting " << std::to_string(max_sequence_id) << " as the last Sequence ID in this run" << TLOG_ENDL;
	while (current_sequence_id < max_sequence_id)
	{
		std::vector<epoll_event> table_events_(4);
		TLOG_DEBUG("table_receiver") << "Waiting for event on table socket" << TLOG_ENDL;
		auto nfds = epoll_wait(table_epoll_fd, &table_events_[0], table_events_.size(), -1);
		if (nfds == -1) {
			perror("epoll_wait");
			exit(EXIT_FAILURE);
		}

		TLOG_DEBUG("table_receiver") << "Received " << nfds << " table update(s)" << TLOG_ENDL;
		for (auto n = 0; n < nfds; ++n) {
			auto first = artdaq::Fragment::InvalidSequenceID;
			auto last = artdaq::Fragment::InvalidSequenceID;
			artdaq::detail::RoutingPacketHeader hdr;
			recv(table_events_[n].data.fd, &hdr, sizeof(artdaq::detail::RoutingPacketHeader), 0);

			TLOG_DEBUG("table_receiver") << "Checking for valid header" << TLOG_ENDL;
			if (hdr.header == ROUTING_MAGIC) {
				artdaq::detail::RoutingPacket buffer(hdr.nEntries);
				TLOG_DEBUG("table_receiver") << "Receiving data buffer" << TLOG_ENDL;
				sts = recv(table_events_[n].data.fd, &buffer[0], sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries, 0);
				assert(static_cast<size_t>(sts) == sizeof(artdaq::detail::RoutingPacketEntry) * hdr.nEntries);

				first = buffer[0].sequence_id;
				last = buffer[buffer.size() - 1].sequence_id;

				for (auto entry : buffer)
				{
					if (routing_table.count(entry.sequence_id))
					{
						assert(routing_table[entry.sequence_id] == entry.destination_rank);
						continue;
					}
					routing_table[entry.sequence_id] = entry.destination_rank;
					TLOG_DEBUG("table_receiver") << "table_receiver " << std::to_string(my_rank) << ": received update: SeqID " << std::to_string(entry.sequence_id) << " -> Rank " << std::to_string(entry.destination_rank) << TLOG_ENDL;
				}

				artdaq::detail::RoutingAckPacket ack;
				ack.rank = my_rank;
				ack.first_sequence_id = first;
				ack.last_sequence_id = last;

				TLOG_DEBUG("table_receiver") << "Sending RoutingAckPacket with first= " << std::to_string(first) << " and last= " << std::to_string(last) << " to " << routing_master_address_ << ", port " << ack_port_ << TLOG_ENDL
				sendto(ack_socket, &ack, sizeof(artdaq::detail::RoutingAckPacket), 0, (struct sockaddr *)&ack_addr, sizeof(ack_addr));
				current_sequence_id = last;
			}
		}
	}

	MPI_Comm_free(&local_group_comm_);
	TLOG_INFO("table_receiver") << "Waiting at MPI_Barrier" << TLOG_ENDL;
	MPI_Barrier(MPI_COMM_WORLD);
	TLOG_INFO("table_receiver") << "Done with MPI_Barrier" << TLOG_ENDL;
}

void RoutingMasterTest::routing_master()
{
	TLOG_DEBUG("routing_master") << "Init" << TLOG_ENDL;
	printHost("routing_master");

	auto app = std::make_unique<artdaq::RoutingMasterApp>(local_group_comm_, "RoutingMaster");

	app->initialize(pset_, 0, 0);
	app->do_start(art::RunID(1), 0, 0);
	TLOG_INFO("routing_master") << "Waiting at MPI_Barrier" << TLOG_ENDL;
	MPI_Barrier(MPI_COMM_WORLD);
	TLOG_INFO("routing_master") << "Done with MPI_Barrier, calling RoutingMasterCore::stop" << TLOG_ENDL;
	app->do_stop(0, 0);
	TLOG_INFO("routing_master") << "Done with RoutingMasterCore::stop, calling shutdown" << TLOG_ENDL;
	app->do_shutdown(0);
	TLOG_INFO("routing_master") << "Done with RoutingMasterCore::shutdown" << TLOG_ENDL;
	MPI_Comm_free(&local_group_comm_);
}

void RoutingMasterTest::printHost(const std::string& functionName) const
{
	char* doPrint = getenv("PRINT_HOST");
	if (doPrint == 0) { return; }
	const int ARRSIZE = 80;
	char hostname[ARRSIZE];
	std::string hostString;
	if (!gethostname(hostname, ARRSIZE))
	{
		hostString = hostname;
	}
	else
	{
		hostString = "unknown";
	}
	TLOG_DEBUG("routing_master") << "Running " << functionName
		<< " on host " << hostString
		<< " with rank " << my_rank << "."
		<< TLOG_ENDL;
}

void printUsage()
{
	int myid = 0;
	struct rusage usage;
	getrusage(RUSAGE_SELF, &usage);
	std::cout << myid << ":"
		<< " user=" << artdaq::Globals::timevalAsDouble(usage.ru_utime)
		<< " sys=" << artdaq::Globals::timevalAsDouble(usage.ru_stime)
		<< std::endl;
}

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("routing_master", false);
	int rc = 1;
	try
	{
		RoutingMasterTest p(argc, argv);
		std::cerr << "Started process " << my_rank << " of " << p.procs_ << ".\n";
		p.go();
		rc = 0;
	}
	catch (std::string& x)
	{
		std::cerr << "Exception (type string) caught in routing_master: "
			<< x
			<< '\n';
		return 1;
	}
	catch (char const* m)
	{
		std::cerr << "Exception (type char const*) caught in routing_master: ";
		if (m)
		{
			std::cerr << m;
		}
		else
		{
			std::cerr << "[the value was a null pointer, so no message is available]";
		}
		std::cerr << '\n';
	}
	return rc;
}
