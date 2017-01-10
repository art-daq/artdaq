#ifndef proto_MPIProg_hh
#define proto_MPIProg_hh

#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQdata/Globals.hh"

struct MPIProg {
  MPIProg(int argc, char ** argv)
    {
      MPI_Init(&argc, &argv);
      MPI_Comm_size(MPI_COMM_WORLD, &procs_);
      MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    };
  ~MPIProg() { MPI_Finalize(); };

  int procs_;
};


#endif /* proto_MPIProg_hh */
