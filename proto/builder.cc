#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Generators/makeFragmentGenerator.hh"
#include "MPIProg.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"

#include <boost/program_options.hpp>
#include "fhiclcpp/make_ParameterSet.h"
namespace bpo = boost::program_options;

#include <algorithm>
#include <cmath>
#include <cstdlib>

extern "C"
{
#include <unistd.h>
}

#include <iostream>
#include <memory>
#include <utility>

extern "C"
{
#include <sys/time.h>
#include <sys/resource.h>
}

/**
 * \brief The Builder class runs the builder test
 */
class Builder : public MPIProg
{
public:
	/**
	 * \brief Builder Constructor
	 * \param argc Argument Count
	 * \param argv Argument Array
	 * \param pset fhicl::ParameterSet used to configure builder
	 */
	Builder(int argc, char* argv[], fhicl::ParameterSet pset);

	/**
	 * \brief Start the Builder application, using the type configuration to select which method to run
	 */
	void go();

	/**
	 * \brief Receive data from source via DataReceiverManager, send it to the EventStore (and art, if configured)
	 */
	void sink();

	/**
	 * \brief Generate data, and send it using DataSenderManager
	 */
	void detector();

private:
	enum class Role : int
	{
		DETECTOR,
		SINK
	};

	void printHost(const std::string& functionName) const;

	fhicl::ParameterSet daq_pset_;
	bool const want_sink_;
	bool const want_periodic_sync_;
	MPI_Comm local_group_comm_;
	Role builder_role_;
};

Builder::Builder(int argc, char* argv[], fhicl::ParameterSet pset) :
	MPIProg(argc, argv)
	, daq_pset_(pset)
	, want_sink_(daq_pset_.get<bool>("want_sink", true))
	, want_periodic_sync_(daq_pset_.get<bool>("want_periodic_sync", false))
	, local_group_comm_()
{
	std::vector<std::string> detectors;
	daq_pset_.get_if_present("detectors", detectors);
	if (static_cast<size_t>(my_rank) >= detectors.size())
	{
		builder_role_ = Role::SINK;
	}
	else
	{
		builder_role_ = Role::DETECTOR;
	}
	std::string type(pset.get<std::string>("transfer_plugin_type", "Shmem"));

	int senders = pset.get<int>("num_senders");
	int receivers = pset.get<int>("num_receivers");
	int buffer_count = pset.get<int>("buffer_count", 10);
	int max_payload_size = pset.get<size_t>("fragment_size", 0x100000);

	std::string hostmap = "";
	if (pset.has_key("hostmap"))
	{
		hostmap = " host_map: @local::hostmap";
	}

	std::stringstream ss;
	ss << pset.to_string();
	ss << " sources: {";
	for (int ii = 0; ii < senders; ++ii)
	{
		ss << "s" << ii << ": { transferPluginType: " << type << " source_rank: " << ii << " max_fragment_size_words: " << max_payload_size << " buffer_count: " << buffer_count << hostmap << "}";
	}
	ss << "} destinations: {";
	for (int jj = senders; jj < senders + receivers; ++jj)
	{
		ss << "d" << jj << ": { transferPluginType: " << type << " destination_rank: " << jj << " max_fragment_size_words: " << max_payload_size << " buffer_count: " << buffer_count << hostmap << "}";
	}
	ss << "}";

	daq_pset_ = fhicl::ParameterSet();
	make_ParameterSet(ss.str(), daq_pset_);


}

void Builder::go()
{
	//volatile bool loopForever = true;
	//while(loopForever)
	//{
	//	usleep(1000000);
	//}


	MPI_Barrier(MPI_COMM_WORLD);
	//std::cout << "daq_pset_: " << daq_pset_.to_string() << std::endl << "conf_.makeParameterSet(): " << conf_.makeParameterSet().to_string() << std::endl;
	MPI_Comm_split(MPI_COMM_WORLD, static_cast<int>(builder_role_), 0, &local_group_comm_);
	switch (builder_role_)
	{
	case Role::SINK:
		if (want_sink_)
		{
			sink();
		}
		else
		{
			std::string
				msg("WARNING: a sink was instantiated despite want_sink being false:\n"
					"set nsinks to 0 in invocation of daqrate?\n");
			std::cerr << msg;
			MPI_Barrier(MPI_COMM_WORLD);
		}
		break;
	case Role::DETECTOR:
		detector();
		break;
	default:
		throw "No such node type";
	}
}

void Builder::detector()
{
	printHost("detector");
	int detector_rank;
	// Should be zero-based, detectors only.
	MPI_Comm_rank(local_group_comm_, &detector_rank);
	assert(!(detector_rank < 0));
	std::ostringstream det_ps_name_loc;
	std::vector<std::string> detectors;
	bool detectors_present = daq_pset_.get_if_present("detectors", detectors);
	size_t detectors_size = detectors.size();
	if (!(detectors_present && detectors_size))
	{
		throw cet::exception("Configuration")
			<< "Unable to find required sequence of detector "
			<< "parameter set names, \"detectors\".";
	}
	fhicl::ParameterSet det_ps =
		daq_pset_.get<fhicl::ParameterSet>(((detectors_size > static_cast<size_t>(detector_rank)) ? detectors[detector_rank] : detectors[0]));
	std::unique_ptr<artdaq::FragmentGenerator> const
		gen(artdaq::makeFragmentGenerator
		(det_ps.get<std::string>("generator"),
		 det_ps));
	{ // Block to handle lifetime of h, below.
		artdaq::DataSenderManager h(daq_pset_);
		MPI_Barrier(local_group_comm_);
		// not using the run time method
		// TimedLoop tl(conf_.run_time_);
		size_t fragments_per_source = -1;
		daq_pset_.get_if_present("fragments_per_source", fragments_per_source);
		artdaq::FragmentPtrs frags;
		size_t fragments_sent = 0;
		while (fragments_sent < fragments_per_source && gen->getNext(frags))
		{
			if (!fragments_sent)
			{
				// Get the detectors lined up first time before we start the
				// firehoses.
				MPI_Barrier(local_group_comm_);
			}
			for (auto& fragPtr : frags)
			{
				std::cout << "Program::detector: Sending fragment " << fragments_sent + 1 << " of " << fragments_per_source << std::endl;
				TLOG_DEBUG("builder") << "Program::detector: Sending fragment " << fragments_sent + 1 << " of " << fragments_per_source << TLOG_ENDL;
				h.sendFragment(std::move(*fragPtr));
				if (++fragments_sent == fragments_per_source) { break; }
				if (want_periodic_sync_ && (fragments_sent % 100) == 0)
				{
					// Don't get too far out of sync.
					MPI_Barrier(local_group_comm_);
				}
			}
			frags.clear();
		}
		TLOG_DEBUG("builder") << "detector waiting " << my_rank << TLOG_ENDL;
	}
	TLOG_DEBUG("builder") << "detector done " << my_rank << TLOG_ENDL;
	MPI_Comm_free(&local_group_comm_);
	MPI_Barrier(MPI_COMM_WORLD);
}

void Builder::sink()
{
	printHost("sink");
	{
		usleep(1000 * my_rank);
		// This scope exists to control the lifetime of 'events'
		auto events = std::make_shared<artdaq::SharedMemoryEventManager>(daq_pset_, daq_pset_);
		events->startRun(daq_pset_.get<int>("run_number", 100));
		{ // Block to handle scope of h, below.
			artdaq::DataReceiverManager h(daq_pset_, events);
			h.start_threads();
			while (h.running_sources().size() > 0)
			{
				usleep(10000);
			}
		}

		TLOG_DEBUG("builder") << "All detectors are done, Sending endOfData Fragment" << TLOG_ENDL;
		// Make the reader application finish, and capture its return
		// status.
		bool endSucceeded = false;
		endSucceeded = events->endOfData();
		if (endSucceeded)
		{
			TLOG_DEBUG("builder") << "Sink: reader is done" << TLOG_ENDL;
		}
		else
		{
			TLOG_DEBUG("builder") << "Sink: reader failed to complete because the "
				<< "endOfData marker could not be pushed onto the queue."
				<< TLOG_ENDL;
		}
	} // end of lifetime of 'events'
	TLOG_DEBUG("builder") << "Sink done " << my_rank << TLOG_ENDL;
	MPI_Barrier(MPI_COMM_WORLD);
}

void Builder::printHost(const std::string& functionName) const
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
	TLOG_DEBUG("builder") << "Running " << functionName
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
	artdaq::configureMessageFacility("builder");

	std::ostringstream descstr;
	descstr << argv[0]
		<< " <-c <config-file>> <other-options> [<source-file>]+";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("config,c", bpo::value<std::string>(), "Configuration file.")
		("help,h", "produce help message");
	bpo::variables_map vm;
	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
		bpo::notify(vm);
	}
	catch (bpo::error const & e) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": " << e.what() << "\n";
		return -1;
	}
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 1;
	}
	if (!vm.count("config")) {
		std::cerr << "Exception from command line processing in " << argv[0]
			<< ": no configuration file given.\n"
			<< "For usage and an options list, please do '"
			<< argv[0] << " --help"
			<< "'.\n";
		return 2;
	}
	fhicl::ParameterSet pset;
	if (getenv("FHICL_FILE_PATH") == nullptr) {
		std::cerr
			<< "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
		setenv("FHICL_FILE_PATH", ".", 0);
	}
	cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
	fhicl::make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);

	int rc = 1;
	try
	{
		Builder p(argc, argv, pset);
		std::cerr << "Started process " << my_rank << " of " << p.procs_ << ".\n";
		p.go();
		rc = 0;
	}
	catch (std::string& x)
	{
		std::cerr << "Exception (type string) caught in driver: "
			<< x
			<< '\n';
		return 1;
	}
	catch (char const* m)
	{
		std::cerr << "Exception (type char const*) caught in driver: ";
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
