
#include "Config.hh"
#include "artdaq/DAQrate/Perf.hh"
#include "artdaq/DAQrate/infoFilename.hh"
#include "artdaq/DAQdata/Fragment.hh"

#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>

#include "artdaq/DAQrate/quiet_mpi.hh"

#include <sys/types.h>
#include <regex.h>

using namespace std;

static const char * usage = "DetectorsPerNode SinksPerNode Run";

static void throwUsage(char * argv0, const string & msg)
{
  cerr << argv0 << " " << usage << "\n";
  throw msg;
}

static double getArgDetectors(int argc, char * argv[])
{
  if (argc < 2) { throwUsage(argv[0], "no detectors_per_node argument"); }
  return atof(argv[1]);
}

static double getArgSinks(int argc, char * argv[])
{
  if (argc < 3) { throwUsage(argv[0], "no sinks_per_node argument"); }
  return atof(argv[2]);
}

static int getArgQueueSize(int argc, char * argv[])
{
  if (argc < 4) { throwUsage(argv[0], "no event_queue_size argument"); }
  return atoi(argv[3]);
}

static int getArgRun(int argc, char * argv[])
{
  if (argc < 5) { throwUsage(argv[0], "no run argument"); }
  return atoi(argv[4]);
}

static std::string getProcessorName()
{
  char buf[100];
  int sz = sizeof(buf);
  MPI_Get_processor_name(buf, &sz);
  return std::string(buf);
}


// remember rank starts at zero
//run_time_(getArgRuntime(argc,argv)),

Config::Config(int rank, int total_procs, int argc, char * argv[]):
  rank_(rank),
  total_procs_(total_procs),

  detectors_(getArgDetectors(argc, argv)),
  sources_(detectors_),
  sinks_(getArgSinks(argc, argv)),

  detector_start_(0),
  source_start_(detectors_),
  sink_start_(detectors_ + sources_),

  event_queue_size_(getArgQueueSize(argc, argv)),
  run_(getArgRun(argc, argv)),

  type_((rank_ < detectors_) ? TaskDetector : ((rank_ < (detectors_ + sources_)) ? TaskSource : TaskSink)),
  offset_(rank_ - ((type_ == TaskDetector) ? detector_start_ : (type_ == TaskSource) ? source_start_ : sink_start_)),
  node_name_(getProcessorName()),
  art_argc_(getArtArgc(argc, argv)),
  art_argv_(getArtArgv(argc - art_argc_, argv)),
  use_artapp_(getenv("ARTDAQ_DAQRATE_USE_ART") != 0)
{
  int total_workers = (detectors_ + sinks_ + sources_);
  if (total_procs_ != total_workers) {
    cerr << "total_procs " << total_procs_ << " != "
         << "total_workers " << total_workers << "\n";
    throw "total_procs != total_workers";
  }
}

void Config::writeInfo() const
{
  string fname = artdaq::infoFilename("config_", rank_, run_);
  ofstream ostr(fname.c_str());
  printHeader(ostr);
  ostr << *this << "\n";
}

int Config::destCount() const
{
  if (type_ == TaskSink) { throw "No destCount for a sink"; }
  return type_ == TaskDetector ? sources_ : sinks_;
}

int Config::destStart() const
{
  if (type_ == TaskSink) { throw "No destStart for a sink"; }
  return type_ == TaskDetector ? source_start_ : sink_start_;
}

int Config::srcCount() const
{
  if (type_ == TaskDetector) { throw "No srcCount for a detector"; }
  return type_ == TaskSink ? sources_ : detectors_;
}

int Config::srcStart() const
{
  if (type_ == TaskDetector) { throw "No srcStart for a detector"; }
  return type_ == TaskSink ? source_start_ : detector_start_;
}

std::string Config::typeName() const
{
  static const char * names[] = { "Sink", "Source", "Detector" };
  return names[type_];
}

int Config::getDestFriend() const
{
  return offset_ + destStart();
}

int Config::getSrcFriend() const
{
  return offset_ + srcStart();
}

int Config::getArtArgc(int argc, char * argv[]) const
{
  // Find the '--' in argv
  int pos = 0;
  for (; pos < argc; ++pos) {
    if (strcmp(argv[pos], "--") == 0) { break; }
  }
  return argc - pos;
}

char ** Config::getArtArgv(int pos, char ** argv) const
{
  return argv + pos;
}

void Config::printHeader(std::ostream & ost) const
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

void Config::print(std::ostream & ost) const
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
      << node_name_ << " "
      << PerfGetStartTime();
}
