#include "art/Framework/Art/artapp.h"
#include "artdaq/DAQdata/Debug.hh"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq-core/Generators/makeFragmentGenerator.hh"
#include "Config.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "MPIProg.hh"
#include "artdaq/DAQrate/Perf.hh"
#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq/DAQrate/SHandles.hh"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "artdaq/DAQrate/Utils.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "cetlib/container_algorithms.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include "boost/program_options.hpp"
namespace bpo = boost::program_options;

#include <algorithm>
#include <cmath>
#include <cstdlib>
extern "C" {
#include <unistd.h>
}
#include <iostream>
#include <memory>
#include <utility>

extern "C" {
#include <sys/time.h>
#include <sys/resource.h>
}

// Class Program is our application object.
class Program : public MPIProg {
public:
  Program(int argc, char * argv[]);
  void go();
  void source();
  void sink();
  void detector();

private:
enum Color_t : int { DETECTOR, SOURCE, SINK };

  fhicl::ParameterSet getPset(char const * progname);
  void printHost(const std::string & functionName) const;

  Config conf_;
  fhicl::ParameterSet const daq_pset_;
  bool const want_sink_;
  bool const want_periodic_sync_;
  size_t const max_payload_size_words_;
  size_t const source_buffers_;
  size_t const sink_buffers_;
  MPI_Comm local_group_comm_;
};

Program::Program(int argc, char * argv[]):
  MPIProg(argc, argv),
  conf_(rank_, procs_, argc, argv),
  daq_pset_(getPset(argv[0])),
  want_sink_(daq_pset_.get<bool>("want_sink", true)),
  want_periodic_sync_(daq_pset_.get<bool>("want_periodic_sync", false)),
  max_payload_size_words_(daq_pset_.get<size_t>("max_payload_size_words", 512 * 1024)),
  source_buffers_(daq_pset_.get<size_t>("source_buffers", 10)),
  sink_buffers_(daq_pset_.get<size_t>("sink_buffers", 10)),
  local_group_comm_()
{
  conf_.writeInfo();
  configureDebugStream(conf_.rank_, conf_.run_);
  PerfConfigure(conf_.rank_,
                conf_.run_,
                conf_.type_);
}

void Program::go()
{
  MPI_Barrier(MPI_COMM_WORLD);
  PerfSetStartTime();
  PerfWriteJobStart();
  MPI_Comm_split(MPI_COMM_WORLD, conf_.type_, 0, &local_group_comm_);
  switch (conf_.type_) {
  case Config::TaskSink:
    if (want_sink_) {
      sink();
    }
    else {
      std::string
        msg("WARNING: a sink was instantiated despite want_sink being false:\n"
            "set nsinks to 0 in invocation of daqrate?\n");
      std::cerr << msg;
      MPI_Barrier(MPI_COMM_WORLD);
    }
    break;
  case Config::TaskSource:
    source(); break;
  case Config::TaskDetector:
    detector(); break;
  default:
    throw "No such node type";
  }
  PerfWriteJobEnd();
}

void Program::source()
{
  printHost("source");
  // needs to get data from the detectors and send it to the sinks
  artdaq::Fragment frag;
  { // Block to handle lifetime of to_r and from_d, below.
    artdaq::RHandles from_d(source_buffers_,
                            max_payload_size_words_,
                            1, // Direct.
                            conf_.getSrcFriend());
    std::unique_ptr<artdaq::SHandles>
      to_r(want_sink_?
           new artdaq::SHandles(sink_buffers_,
                                max_payload_size_words_,
                                conf_.sinks_,
                                conf_.sink_start_,
                                false) :
           nullptr
          );
    while (from_d.sourcesActive() > 0) {
      from_d.recvFragment(frag);
      if (want_sink_ && frag.type() != artdaq::Fragment::EndOfDataFragmentType) {
        to_r->sendFragment(std::move(frag));
      }
      if (from_d.sourcesActive() > 0 &&
          from_d.sourcesActive() == from_d.sourcesPending()) {
        Debug << "Waiting for in-flight fragments from "
              << from_d.sourcesPending()
              << " sources." << flusher;
      }
    }
  }
  Debug << "source done " << conf_.rank_ << flusher;
  MPI_Barrier(MPI_COMM_WORLD);
}

void Program::detector()
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
        (detectors_size = detectors.size()))) {
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
    artdaq::SHandles h(source_buffers_,
                       max_payload_size_words_,
                       1, // Direct.
                       conf_.getDestFriend(),
                       false);
    MPI_Barrier(local_group_comm_);
    // not using the run time method
    // TimedLoop tl(conf_.run_time_);
    size_t fragments_per_source = -1;
    daq_pset_.get_if_present("fragments_per_source", fragments_per_source);
    artdaq::FragmentPtrs frags;
    size_t fragments_sent = 0;
    while (fragments_sent < fragments_per_source && gen->getNext(frags)) {
      if (!fragments_sent) {
        // Get the detectors lined up first time before we start the
        // firehoses.
        MPI_Barrier(local_group_comm_);
      }
      for (auto & fragPtr : frags) {
        h.sendFragment(std::move(*fragPtr));
        if (++fragments_sent == fragments_per_source) { break; }
        if (want_periodic_sync_ && (fragments_sent % 100) == 0) {
          // Don't get too far out of sync.
          MPI_Barrier(local_group_comm_);
        }
      }
      frags.clear();
    }
    Debug << "detector waiting " << conf_.rank_ << flusher;
  }
  Debug << "detector done " << conf_.rank_ << flusher;
  MPI_Comm_free(&local_group_comm_);
  MPI_Barrier(MPI_COMM_WORLD);
}

void Program::sink()
{
  printHost("sink");
  {
    // This scope exists to control the lifetime of 'events'
    int sink_rank;
    bool useArt = daq_pset_.get<bool>("useArt", false);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
    char * dummyArgs[1] { "SimpleQueueReader" };
#pragma GCC diagnostic pop
    MPI_Comm_rank(local_group_comm_, &sink_rank);
    artdaq::EventStore::ART_CMDLINE_FCN * reader =
      useArt ?
      &artapp :
      &artdaq::simpleQueueReaderApp;
    artdaq::EventStore events(daq_pset_, conf_.detectors_,
            conf_.run_,
                              sink_rank,
                              useArt ? conf_.art_argc_ : 1,
                              useArt ? conf_.art_argv_ : dummyArgs,
                              reader);
    { // Block to handle scope of h, below.
      artdaq::RHandles h(sink_buffers_ *
                         std::ceil(1.0 * conf_.sources_ / conf_.sinks_),
                         max_payload_size_words_,
                         conf_.sources_,
                         conf_.source_start_);
      while (h.sourcesActive() > 0) {
        artdaq::FragmentPtr pfragment(new artdaq::Fragment);
        h.recvFragment(*pfragment);
        if (pfragment->type() != artdaq::Fragment::EndOfDataFragmentType) {
          events.insert(std::move(pfragment));
        }
        if (h.sourcesActive() == h.sourcesPending()) {
          Debug << "Waiting for in-flight fragments from "
                << h.sourcesPending()
                << " sources." << flusher;
        }
      }
    }
    // Make the reader application finish, and capture its return
    // status.
    int readerReturnValue;
    bool endSucceeded = false;
    int attemptsToEnd = 1;
    endSucceeded = events.endOfData(readerReturnValue);
    while (! endSucceeded && attemptsToEnd < 3) {
      ++attemptsToEnd;
      endSucceeded = events.endOfData(readerReturnValue);
    }
    if (endSucceeded) {
      Debug << "Sink: reader is done, its exit status was: "
            << readerReturnValue << flusher;
    }
    else {
      Debug << "Sink: reader failed to complete because the "
            << "endOfData marker could not be pushed onto the queue."
            << flusher;
    }
  } // end of lifetime of 'events'
  Debug << "Sink done " << conf_.rank_ << flusher;
  MPI_Barrier(MPI_COMM_WORLD);
}

fhicl::ParameterSet
Program::getPset(char const * progname)
{
  std::ostringstream descstr;
  descstr << progname
          << " <-c <config-file>>";
  bpo::options_description desc(descstr.str());
  desc.add_options()
    ("config,c", bpo::value<std::string>(), "Configuration file.");
  bpo::variables_map vm;
  try {
    bpo::store(bpo::command_line_parser(conf_.art_argc_, conf_.art_argv_).
               options(desc).allow_unregistered().run(), vm);
    bpo::notify(vm);
  }
  catch (bpo::error const & e) {
    std::cerr << "Exception from command line processing in " << progname
              << ": " << e.what() << "\n";
    throw "cmdline parsing error.";
  }
  if (!vm.count("config")) {
    std::cerr << "Expected \"-- -c <config-file>\" fhicl file specification.\n";
    throw "cmdline parsing error.";
  }
  fhicl::ParameterSet pset;
  cet::filepath_lookup lookup_policy("FHICL_FILE_PATH");
  fhicl::make_ParameterSet(vm["config"].as<std::string>(),
                           lookup_policy, pset);
  return pset.get<fhicl::ParameterSet>("daq");
}

void Program::printHost(const std::string & functionName) const
{
  char * doPrint = getenv("PRINT_HOST");
  if (doPrint == 0) {return;}
  const int ARRSIZE = 80;
  char hostname[ARRSIZE];
  std::string hostString;
  if (! gethostname(hostname, ARRSIZE)) {
    hostString = hostname;
  }
  else {
    hostString = "unknown";
  }
  Debug << "Running " << functionName
        << " on host " << hostString
        << " with rank " << rank_ << "."
        << flusher;
}

void printUsage()
{
  int myid = 0;
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  std::cout << myid << ":"
            << " user=" << asDouble(usage.ru_utime)
            << " sys=" << asDouble(usage.ru_stime)
            << std::endl;
}

int main(int argc, char * argv[])
{
  int rc = 1;
  try {
    Program p(argc, argv);
    std::cerr << "Started process " << p.rank_ << " of " << p.procs_ << ".\n";
    p.go();
    rc = 0;
  }
  catch (std::string & x) {
    std::cerr << "Exception (type string) caught in driver: "
              << x
              << '\n';
    return 1;
  }
  catch (char const * m) {
    std::cerr << "Exception (type char const*) caught in driver: ";
    if (m)
    { std::cerr << m; }
    else
    { std::cerr << "[the value was a null pointer, so no message is available]"; }
    std::cerr << '\n';
  }
  return rc;
}
