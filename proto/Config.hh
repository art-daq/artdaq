#ifndef proto_Config_hh
#define proto_Config_hh

#include <iostream>
#include <string>
#include <fhiclcpp/fwd.h>

// sources are first, sinks are second
// the offset_ is the index of the first sink
// the offset_ = sources_

// Define the environment variable ARTDAQ_DAQRATE_USE_ART to any value
// to set use_artapp_ to true.

class Config {
public:
enum TaskType : int { TaskSink = 0, TaskSource = 1, TaskDetector = 2 };

  Config(int rank, int nprocs,int buffer_count, size_t max_payload_size, int argc, char * argv[]);

  int destCount() const;
  int destStart() const;
  int srcCount() const;
  int srcStart() const;
  int getDestFriend() const;
  int getSrcFriend() const;
  int getArtArgc(int argc, char * argv[]) const;
  char ** getArtArgv(int argc, char * argv[]) const;
  std::string typeName() const;
  void writeInfo() const;

  // input parameters
  int rank_;
  int total_procs_;

  int detectors_;
  int sources_;
  int sinks_;
  int detector_start_;
  int source_start_;
  int sink_start_;

  int event_queue_size_;
  int run_;

  int buffer_count_;
  size_t max_payload_size_;

  // calculated parameters
  TaskType type_;
  int offset_;
  std::string node_name_;

  int  art_argc_;
  char ** art_argv_;
  bool use_artapp_;

  void print(std::ostream & ost) const;
  void printHeader(std::ostream & ost) const;

  fhicl::ParameterSet makeParameterSet() const;
};

inline std::ostream & operator<<(std::ostream & ost, Config const & c)
{
  c.print(ost);
  return ost;
}

#endif /* proto_Config_hh */
