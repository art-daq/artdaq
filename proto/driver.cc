//
// artdaqDriver is a program for testing the behavior of the generic
// RawInput source. Run 'artdaqDriver --help' to get a description of the
// expected command-line parameters.
//
//
// The current version generates simple data fragments, for testing
// that data are transmitted without corruption from the
// artdaq::EventStore through to the artdaq::RawInput source.
//

#include "art/Framework/Art/artapp.h"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/DAQdata/GenericFragmentSimulator.hh"

#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Generators/makeFragmentGenerator.hh"
#include "artdaq/Application/makeCommandableFragmentGenerator.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "cetlib/filepath_maker.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"
#include <boost/program_options.hpp>

#include <signal.h>
#include <iostream>
#include <memory>
#include <utility>
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"
#include "LoadParameterSet.hh"

namespace  bpo = boost::program_options;

volatile int events_to_generate;
void sig_handler(int) { events_to_generate = -1; }

template<typename B, typename D>
std::unique_ptr<D>
dynamic_unique_ptr_cast(std::unique_ptr<B>& p);

int main(int argc, char * argv[]) try
{
	auto pset = LoadParameterSet(argc, argv);

	int run = pset.get<int>("run_number", 1);
	uint64_t timeout = pset.get<uint64_t>("transition_timeout", 30);
	uint64_t timestamp = 0;

	artdaq::configureMessageFacility("artdaqDriver");

	fhicl::ParameterSet fragment_receiver_pset = pset.get<fhicl::ParameterSet>("fragment_receiver");

	std::unique_ptr<artdaq::FragmentGenerator>
		gen(artdaq::makeFragmentGenerator(fragment_receiver_pset.get<std::string>("generator"),
										  fragment_receiver_pset));

	std::unique_ptr<artdaq::CommandableFragmentGenerator> commandable_gen =
		dynamic_unique_ptr_cast<artdaq::FragmentGenerator, artdaq::CommandableFragmentGenerator>(gen);

	artdaq::MetricManager metricMan_;
	metricMan = &metricMan_;
	my_rank = 0;
	// pull out the Metric part of the ParameterSet
	fhicl::ParameterSet metric_pset;
	try {
		metric_pset = pset.get<fhicl::ParameterSet>("metrics");
	}
	catch (...) {} // OK if there's no metrics table defined in the FHiCL 

	if (metric_pset.is_empty()) {
		TLOG_INFO("artdaqDriver") << "No metric plugins appear to be defined" << TLOG_ENDL;
	}
	try {
		metricMan_.initialize(metric_pset, "artdaqDriver");
		metricMan_.do_start();
	}
	catch (...) {
	}
	artdaq::FragmentPtrs frags;
	//////////////////////////////////////////////////////////////////////
	// Note: we are constrained to doing all this here rather than
	// encapsulated neatly in a function due to the lieftime issues
	// associated with async threads and std::string::c_str().
	fhicl::ParameterSet event_builder_pset = pset.get<fhicl::ParameterSet>("event_builder");

	artdaq::SharedMemoryEventManager store(event_builder_pset, pset);
	//////////////////////////////////////////////////////////////////////

	int events_to_generate = pset.get<int>("events_to_generate", 0);
	int event_count = 0;
	artdaq::Fragment::sequence_id_t previous_sequence_id = -1;

	if (commandable_gen) {
		commandable_gen->StartCmd(run, timeout, timestamp);
	}

	TLOG_ARB(50, "artdaqDriver") << "driver main before store.startRun" << TLOG_ENDL;
	store.startRun(run);

	// Read or generate fragments as rapidly as possible, and feed them
	// into the EventStore. The throughput resulting from this design
	// choice is likely to have the fragment reading (or generation)
	// speed as the limiting factor
	while ((commandable_gen && commandable_gen->getNext(frags)) ||
		(gen && gen->getNext(frags))) {
		TLOG_ARB(50, "artdaqDriver") << "driver main: getNext returned frags.size()=" << std::to_string(frags.size()) << " current event_count=" << event_count << TLOG_ENDL;
		for (auto & val : frags) {
			if (val->sequenceID() != previous_sequence_id) {
				++event_count;
				previous_sequence_id = val->sequenceID();
			}
			if (events_to_generate != 0 && event_count > events_to_generate) {
				if (commandable_gen) {
					commandable_gen->StopCmd(timeout, timestamp);
				}
				break;
			}
			artdaq::FragmentPtr tempFrag;
			auto sts = store.AddFragment(std::move(val), 1000000, tempFrag);
			if (!sts)
			{
				TLOG_ERROR("artdaqDriver") << "Fragment was not added after 1s. Check art thread status!" << TLOG_ENDL;
				exit(1);
			}
		}
		frags.clear();

		if (events_to_generate != 0 && event_count >= events_to_generate) {
			if (commandable_gen) {
				commandable_gen->StopCmd(timeout, timestamp);
			}
			break;
		}
	}


	std::vector<int> readerReturnValues;
	bool endSucceeded = false;
	int attemptsToEnd = 1;
	endSucceeded = store.endOfData(readerReturnValues);
	while (!endSucceeded && attemptsToEnd < 3) {
		++attemptsToEnd;
		endSucceeded = store.endOfData(readerReturnValues);
	}
	if (!endSucceeded) {
		std::cerr << "Failed to shut down the reader and the event store "
			<< "because the endOfData marker could not be pushed "
			<< "onto the queue." << std::endl;
	}

	metricMan_.do_stop();
	return 0;
}
catch (std::string & x)
{
	std::cerr << "Exception (type string) caught in artdaqDriver: " << x << '\n';
	return 1;
}
catch (char const * m)
{
	std::cerr << "Exception (type char const*) caught in artdaqDriver: ";
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
catch (...) {
	artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no,
							 "Exception caught in artdaqDriver");
}


template<typename B, typename D>
std::unique_ptr<D>
dynamic_unique_ptr_cast(std::unique_ptr<B>& p)
{
	D* result = dynamic_cast<D*>(p.get());

	if (result) {
		p.release();
		return std::unique_ptr<D>(result);
	}
	return nullptr;
}
