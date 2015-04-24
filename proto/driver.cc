//
// Driver is a program for testing the behavior of the generic
// RawInput source. Run 'driver --help' to get a description of the
// expected command-line parameters.
//
//
// The current version generates simple data fragments, for testing
// that data are transmitted without corruption from the
// artdaq::EventStore through to the artdaq::RawInput source.
//

#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq/DAQdata/GenericFragmentSimulator.hh"
#include "artdaq-core/Generators/makeFragmentGenerator.hh"
#include "artdaq/DAQrate/EventStore.hh"
#include "MPIProg.hh"
#include "artdaq-core/Core/SimpleQueueReader.hh"
#include "cetlib/container_algorithms.h"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include "boost/program_options.hpp"

#include <signal.h>
#include <iostream>
#include <memory>
#include <utility>

using namespace std;
using namespace fhicl;
namespace  bpo = boost::program_options;

volatile int events_to_generate;
void sig_handler(int) {events_to_generate = -1;}

int main(int argc, char * argv[]) try
{
  MPIProg mpiSentry(argc, argv);
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
              << argv[0] <<  " --help"
              << "'.\n";
    return 2;
  }
  ParameterSet pset;
  if (getenv("FHICL_FILE_PATH") == nullptr) {
    std::cerr
      << "INFO: environment variable FHICL_FILE_PATH was not set. Using \".\"\n";
    setenv("FHICL_FILE_PATH", ".", 0);
  }
  cet::filepath_lookup_after1 lookup_policy("FHICL_FILE_PATH");
  make_ParameterSet(vm["config"].as<std::string>(), lookup_policy, pset);
  ParameterSet fragment_receiver_pset = pset.get<ParameterSet>("fragment_receiver");
  std::unique_ptr<artdaq::FragmentGenerator> const
    gen(artdaq::makeFragmentGenerator(fragment_receiver_pset.get<std::string>("generator"),
                                      fragment_receiver_pset));
  artdaq::FragmentPtrs frags;
  //////////////////////////////////////////////////////////////////////
  // Note: we are constrained to doing all this here rather than
  // encapsulated neatly in a function due to the lieftime issues
  // associated with async threads and std::string::c_str().
  ParameterSet event_builder_pset = pset.get<ParameterSet>("event_builder");
  bool const want_artapp(event_builder_pset.get<bool>("use_art", false));
  std::ostringstream os;
  if (!want_artapp) {
    os << event_builder_pset.get<int>("events_expected_in_SimpleQueueReader");
  }
  std::string const oss(os.str());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  char * args[2] {"SimpleQueueReader", const_cast<char *>(oss.c_str())};
#pragma GCC diagnostic pop
  int es_argc(want_artapp?argc:2);
  char **es_argv (want_artapp?argv:args);
  artdaq::EventStore::ART_CMDLINE_FCN *
    es_fcn(want_artapp?&artapp:&artdaq::simpleQueueReaderApp);
  artdaq::EventStore store(event_builder_pset, event_builder_pset.get<size_t>("expected_fragments_per_event"),
			   pset.get<artdaq::EventStore::run_id_t>("run_number"),
                           1,
                           es_argc,
                           es_argv,
                           es_fcn);
  //////////////////////////////////////////////////////////////////////

  int events_to_generate = pset.get<int>("events_to_generate", 0);
  int event_count = 0;
  artdaq::Fragment::sequence_id_t previous_sequence_id = -1;

  // Read or generate fragments as rapidly as possible, and feed them
  // into the EventStore. The throughput resulting from this design
  // choice is likely to have the fragment reading (or generation)
  // speed as the limiting factor
  while (gen->getNext(frags)) {
    for (auto & val : frags) {
      if (val->sequenceID() != previous_sequence_id) {
        ++event_count;
        previous_sequence_id = val->sequenceID();
      }
      if (events_to_generate != 0 && event_count > events_to_generate) {break;}
      store.insert(std::move(val));
    }
    frags.clear();

    if (events_to_generate != 0 && event_count >= events_to_generate) {break;}
  }

  int readerReturnValue;
  bool endSucceeded = false;
  int attemptsToEnd = 1;
  endSucceeded = store.endOfData(readerReturnValue);
  while (! endSucceeded && attemptsToEnd < 3) {
    ++attemptsToEnd;
    endSucceeded = store.endOfData(readerReturnValue);
  }
  if (! endSucceeded) {
    std::cerr << "Failed to shut down the reader and the event store "
              << "because the endOfData marker could not be pushed "
              << "onto the queue." << std::endl;
  }

  return readerReturnValue;
}

catch (std::string & x)
{
  cerr << "Exception (type string) caught in driver: " << x << '\n';
  return 1;
}

catch (char const * m)
{
  cerr << "Exception (type char const*) caught in driver: ";
  if (m)
  { cerr << m; }
  else
  { cerr << "[the value was a null pointer, so no message is available]"; }
  cerr << '\n';
}
