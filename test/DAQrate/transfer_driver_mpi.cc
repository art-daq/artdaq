
#include "test/DAQrate/TransferTest.hh"
#include "trace.h"
#include "artdaq/DAQdata/Globals.hh"
#include <fhiclcpp/make_ParameterSet.h>
#include <mpi.h>
#include <cstdlib>

int main(int argc, char * argv[])
{
  TRACE_CNTL("reset");
  TRACE( 10, "s_r_handles main enter" );
  char envvar[] = "MV2_ENABLE_AFFINITY=0";
  assert(putenv(envvar) == 0);
  auto const requested_threading = MPI_THREAD_SERIALIZED;
  int  provided_threading = -1;
  auto rc = MPI_Init_thread(&argc, &argv, requested_threading, &provided_threading);
  assert(rc == 0);
  assert(requested_threading == provided_threading);
  rc = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  assert(rc == 0);

  if (my_rank == 0) {
    std::cout << "argc:" << argc << std::endl;
    for (int i = 0; i < argc; ++i) {
      std::cout << "argv[" << i << "]: " << argv[i] << std::endl;
    }
  }

  if (argc != 2) {
    std::cerr << argv[0] << " requires 1 argument, " << argc - 1 << " provided\n";
    return 1;
  }
  
  cet::filepath_maker maker;
  fhicl::ParameterSet ps;

  auto fhicl = std::string(argv[1]);
  make_ParameterSet(fhicl, maker, ps);

  artdaq::TransferTest theTest(ps);
  theTest.runTest();

  rc = MPI_Finalize();
  assert(rc == 0);
  TRACE( 11, "s_r_handles main return" );
  return 0;
}
