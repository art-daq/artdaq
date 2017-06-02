#include "Config.hh"
#include "artdaq/DAQrate/infoFilename.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>

#include <sys/types.h>
#include <regex.h>

#include <boost/program_options.hpp>
namespace bpo = boost::program_options;

const char* artdaq::Config::usage = "DetectorsPerNode SinksPerNode Run";



// remember rank starts at zero
//run_time_(getArgRuntime(argc,argv)),

artdaq::Config::Config(int rank, int total_procs, int buffer_count, size_t max_payload_size, int argc, char* argv[]):
																											rank_(rank)
																											, total_procs_(total_procs)
																											, detectors_(getArgDetectors(argc, argv))
																											, sources_(detectors_)
																											, sinks_(getArgSinks(argc, argv))
																											, detector_start_(0)
																											, source_start_(detectors_)
																											, sink_start_(detectors_ + sources_)
																											, event_queue_size_(getArgQueueSize(argc, argv))
																											, run_(getArgRun(argc, argv))
																											, buffer_count_(buffer_count)
																											, max_payload_size_(max_payload_size)
																											, type_((rank_ < detectors_) ? TaskDetector : ((rank_ < (detectors_ + sources_)) ? TaskSource : TaskSink))
																											, offset_(rank_ - ((type_ == TaskDetector) ? detector_start_ : (type_ == TaskSource) ? source_start_ : sink_start_))
																											, node_name_(getProcessorName())
																											, art_argc_(getArtArgc(argc, argv))
																											, art_argv_(getArtArgv(argc - art_argc_, argv))
																											, use_artapp_(getenv("ARTDAQ_DAQRATE_USE_ART") != 0)
{
	int total_workers = (detectors_ + sinks_ + sources_);
	if (total_procs_ != total_workers)
	{
		std::cerr << "total_procs " << total_procs_ << " != "
			<< "total_workers " << total_workers << "\n";
		throw "total_procs != total_workers";
	}
}

void artdaq::Config::writeInfo() const
{
	std::string fname = artdaq::infoFilename("config_", rank_, run_);
	std::ofstream ostr(fname.c_str());
	printHeader(ostr);
	ostr << *this << "\n";
}

int artdaq::Config::destCount() const
{
	if (type_ == TaskSink) { throw "No destCount for a sink"; }
	return type_ == TaskDetector ? sources_ : sinks_;
}

int artdaq::Config::destStart() const
{
	if (type_ == TaskSink) { throw "No destStart for a sink"; }
	return type_ == TaskDetector ? source_start_ : sink_start_;
}

int artdaq::Config::srcCount() const
{
	if (type_ == TaskDetector) { throw "No srcCount for a detector"; }
	return type_ == TaskSink ? sources_ : detectors_;
}

int artdaq::Config::srcStart() const
{
	if (type_ == TaskDetector) { throw "No srcStart for a detector"; }
	return type_ == TaskSink ? source_start_ : detector_start_;
}

std::string artdaq::Config::typeName() const
{
	static const char* names[] = {"Sink", "Source", "Detector"};
	return names[type_];
}

int artdaq::Config::getDestFriend() const
{
	return offset_ + destStart();
}

int artdaq::Config::getSrcFriend() const
{
	return offset_ + srcStart();
}

int artdaq::Config::getArtArgc(int argc, char* argv[]) const
{
	// Find the '--' in argv
	int pos = 0;
	for (; pos < argc; ++pos)
	{
		if (strcmp(argv[pos], "--") == 0) { break; }
	}
	return argc - pos;
}

char** artdaq::Config::getArtArgv(int pos, char** argv) const
{
	return argv + pos;
}

void artdaq::Config::printHeader(std::ostream& ost) const
{
	ost << "Rank TotalNodes "
		<< "DetectorsPerNode SourcesPerNode SinksPerNode "
		<< "BuilderNodes DetectorNodes Sources Sinks Detectors "
		<< "DetectorStart SourceStart SinkStart "
		<< "EventQueueSize "
		<< "Run "
		<< "Type Offset "
		<< "Nodename "
		<< "StartTime\n";
}

void artdaq::Config::print(std::ostream& ost) const
{
	ost << rank_ << " "
		<< sources_ << " "
		<< sinks_ << " "
		<< detectors_ << " "
		<< detector_start_ << " "
		<< source_start_ << " "
		<< sink_start_ << " "
		<< event_queue_size_ << " "
		<< run_ << " "
		<< typeName() << " "
		<< offset_ << " "
		<< node_name_;
}

fhicl::ParameterSet artdaq::Config::makeParameterSet() const
{
	std::stringstream ss;
	if (type_ != TaskDetector)
	{
		ss << "sources: {";
		int count = type_ == TaskSource ? detectors_ : sources_;
		int start = type_ == TaskSource ? detector_start_ : source_start_;
		for (int ii = 0; ii < count; ++ii)
		{
			ss << "s" << ii + start << ": { transferPluginType: MPI source_rank: " << ii + start << " max_fragment_size_words: " << max_payload_size_ << " buffer_count: " << buffer_count_ << "}";
		}

		ss << "}";
	}
	if (type_ != TaskSink)
	{
		ss << " destinations: {";
		int count = type_ == TaskDetector ? sources_ : sinks_;
		int start = type_ == TaskDetector ? source_start_ : sink_start_;
		for (int ii = 0; ii < count; ++ii)
		{
			ss << "d" << ii + start << ": { transferPluginType: MPI destination_rank: " << ii + start << " max_fragment_size_words: " << max_payload_size_ << " buffer_count: " << buffer_count_ << "}";
		}

		ss << "}";
	}

	fhicl::ParameterSet ps;
	fhicl::make_ParameterSet(ss.str(), ps);
	return ps;
}

fhicl::ParameterSet artdaq::Config::getArtPset()
{
	std::ostringstream descstr;
	descstr << "-- <-c <config-file>>";
	bpo::options_description desc(descstr.str());
	desc.add_options()
		("config,c", bpo::value<std::string>(), "Configuration file.");
	bpo::variables_map vm;
	try
	{
		bpo::store(bpo::command_line_parser(art_argc_, art_argv_).
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
	auto ps = pset.get<fhicl::ParameterSet>("daq");
	buffer_count_ = ps.get<int>("buffer_count", buffer_count_);
	max_payload_size_ = ps.get<size_t>("max_fragment_size_words", max_payload_size_);

	return ps;
}
