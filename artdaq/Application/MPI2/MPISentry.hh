#ifndef artdaq_Application_MPI2_MPISentry_hh
#define artdaq_Application_MPI2_MPISentry_hh

#include "artdaq/Application/TaskType.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"

namespace artdaq
{
	class MPISentry;
}

class artdaq::MPISentry
{
public:
	MPISentry(int* argc_ptr, char*** argv_ptr);

	MPISentry(int* argc_ptr,
	          char*** argv_ptr,
	          int threading_level);

	MPISentry(int* argc_ptr,
	          char*** argv_ptr,
	          int threading_level, artdaq::TaskType type,
	          MPI_Comm& local_group_comm);

	~MPISentry();

	int threading_level() const;

	int rank() const;

	int procs() const;

private:
	void initialize_();

	int threading_level_;
	int rank_;
	int procs_;
};


#endif /* artdaq_Application_MPI2_MPISentry_hh */
