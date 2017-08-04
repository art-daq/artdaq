#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Generators/makeFragmentGenerator.hh"
#include "Config.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "MPIProg.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "fhiclcpp/ParameterSet.h"

#include <boost/program_options.hpp>
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
	 */
	Builder(int argc, char* argv[]);

	/**
	 * \brief Start the Builder application, using the type configuration to select which method to run
	 */
	void go();

	/**
	 * \brief Receive data from detector via DataReceiverManager, and send to a sink using DataSenderManager
	 */
	void source();

	/**
	 * \brief Receive data from source via DataReceiverManager, send it to the EventStore (and art, if configured)
	 */
	void sink();

	/**
	 * \brief Generate data, and send it using DataSenderManager
	 */
	void detector();

private:
	enum Color_t : int
	{
		DETECTOR,
		SOURCE,
		SINK
	};

	void printHost(const std::string& functionName) const;

	artdaq::Config conf_;
	fhicl::ParameterSet const daq_pset_;
	bool const want_sink_;
	bool const want_periodic_sync_;
	MPI_Comm local_group_comm_;
};

Builder::Builder(int argc, char* argv[]) :
										 MPIProg(argc, argv)
										 , conf_(my_rank, procs_, 10, 10240, argc, argv)
										 , daq_pset_(conf_.getArtPset())
										 , want_sink_(daq_pset_.get<bool>("want_sink", true))
										 , want_periodic_sync_(daq_pset_.get<bool>("want_periodic_sync", false))
										 , local_group_comm_()
{
	conf_.writeInfo();
}

void Builder::go()
{
	MPI_Barrier(MPI_COMM_WORLD);
	//std::cout << "daq_pset_: " << daq_pset_.to_string() << std::endl << "conf_.makeParameterSet(): " << conf_.makeParameterSet().to_string() << std::endl;
	MPI_Comm_split(MPI_COMM_WORLD, conf_.type_, 0, &local_group_comm_);
	switch (conf_.type_)
	{
	case artdaq::Config::TaskSink:
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
	case artdaq::Config::TaskSource:
		source();
		break;
	case artdaq::Config::TaskDetector:
		detector();
		break;
	default:
		throw "No such node type";
	}
}

void Builder::source()
{
	printHost("source");
	// needs to get data from the detectors and send it to the sinks
	artdaq::FragmentPtr frag;
	{ // Block to handle lifetime of to_r and from_d, below.
		artdaq::DataReceiverManager from_d(conf_.makeParameterSet());
		from_d.start_threads();
		std::unique_ptr<artdaq::DataSenderManager> to_r(want_sink_ ? new artdaq::DataSenderManager(conf_.makeParameterSet()) : nullptr);
		int senderCount = from_d.enabled_sources().size();
		while (senderCount > 0)
		{
			int ignoredSender;
			frag = from_d.recvFragment(ignoredSender);
			if (!frag || ignoredSender == artdaq::TransferInterface::RECV_TIMEOUT) continue;
			std::cout << "Program::source: Received fragment " << frag->sequenceID() << " from sender " << ignoredSender << std::endl;
			if (want_sink_ && frag->type() != artdaq::Fragment::EndOfDataFragmentType)
			{
				to_r->sendFragment(std::move(*frag));
			}
			else if (frag->type() == artdaq::Fragment::EndOfDataFragmentType)
			{
				senderCount--;
			}
		}
	}
	TLOG_DEBUG("builder") << "source done " << conf_.rank_ << TLOG_ENDL;
	MPI_Barrier(MPI_COMM_WORLD);
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
	size_t detectors_size = 0;
	if (!(daq_pset_.get_if_present("detectors", detectors) &&
		  (detectors_size = detectors.size())))
	{
		throw cet::exception("Configuration")
			  << "Unable to find required sequence of detector "
			  << "parameter set names, \"detectors\".";
	}
	fhicl::ParameterSet det_ps =
		daq_pset_.get<fhicl::ParameterSet>
		((detectors_size > static_cast<size_t>(detector_rank)) ?
			 detectors[detector_rank] :
			 detectors[0]);
	std::unique_ptr<artdaq::FragmentGenerator> const
		gen(artdaq::makeFragmentGenerator
		(det_ps.get<std::string>("generator"),
		 det_ps));
	{ // Block to handle lifetime of h, below.
		artdaq::DataSenderManager h(conf_.makeParameterSet());
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
		TLOG_DEBUG("builder") << "detector waiting " << conf_.rank_ << TLOG_ENDL;
	}
	TLOG_DEBUG("builder") << "detector done " << conf_.rank_ << TLOG_ENDL;
	MPI_Comm_free(&local_group_comm_);
	MPI_Barrier(MPI_COMM_WORLD);
}

void Builder::sink()
{
	printHost("sink");
	{
		// This scope exists to control the lifetime of 'events'
		int sink_rank;
		bool useArt = daq_pset_.get<bool>("useArt", false);
		const char* dummyArgs[1]{"SimpleQueueReader"};
		MPI_Comm_rank(local_group_comm_, &sink_rank);
		artdaq::EventStore::ART_CMDLINE_FCN* reader =
			useArt ?
				&artapp :
				&artdaq::simpleQueueReaderApp;
		artdaq::EventStore events(daq_pset_, conf_.detectors_,
								  conf_.run_,
								  useArt ? conf_.art_argc_ : 1,
								  useArt ? conf_.art_argv_ : const_cast<char**>(dummyArgs),
								  reader);
		{ // Block to handle scope of h, below.
			artdaq::DataReceiverManager h(conf_.makeParameterSet());
			h.start_threads();
			int senderCount = h.enabled_sources().size();
			while (senderCount > 0)
			{
				artdaq::FragmentPtr pfragment(new artdaq::Fragment);
				int ignoredSource;
				pfragment = h.recvFragment(ignoredSource);
				if (!pfragment || ignoredSource == artdaq::TransferInterface::RECV_TIMEOUT) continue;
				std::cout << "Program::sink: Received fragment " << pfragment->sequenceID() << " from sender " << ignoredSource << std::endl;
				if (pfragment->type() != artdaq::Fragment::EndOfDataFragmentType)
				{
					events.insert(std::move(pfragment));
				}
				else
				{
					senderCount--;
				}
			}
		}
		// Make the reader application finish, and capture its return
		// status.
		int readerReturnValue;
		bool endSucceeded = false;
		int attemptsToEnd = 1;
		endSucceeded = events.endOfData(readerReturnValue);
		while (!endSucceeded && attemptsToEnd < 3)
		{
			++attemptsToEnd;
			endSucceeded = events.endOfData(readerReturnValue);
		}
		if (endSucceeded)
		{
			TLOG_DEBUG("builder") << "Sink: reader is done, its exit status was: "
				 << readerReturnValue << TLOG_ENDL;
		}
		else
		{
			TLOG_DEBUG("builder") << "Sink: reader failed to complete because the "
				 << "endOfData marker could not be pushed onto the queue."
				 << TLOG_ENDL;
		}
	} // end of lifetime of 'events'
	TLOG_DEBUG("builder") << "Sink done " << conf_.rank_ << TLOG_ENDL;
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
	int rc = 1;
	try
	{
		Builder p(argc, argv);
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
