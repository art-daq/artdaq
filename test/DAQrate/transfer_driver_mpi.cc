#define MPI_MODE 1

#include "test/DAQrate/TransferTest.hh"
#include "trace.h"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/configureMessageFacility.hh"
#include <fhiclcpp/make_ParameterSet.h>
#include <mpi.h>
#include <cstdlib>

#include <sys/types.h>
#include <unistd.h>

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("transfer_driver_mpi");
	TRACE(TLVL_TRACE, "s_r_handles main enter" );
	char envvar[] = "MV2_ENABLE_AFFINITY=0";
	assert(putenv(envvar) == 0);
	auto const requested_threading = MPI_THREAD_SERIALIZED;
	int provided_threading = -1;
	auto rc = MPI_Init_thread(&argc, &argv, requested_threading, &provided_threading);
	assert(rc == 0);
	assert(requested_threading == provided_threading);
	rc = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	assert(rc == 0);

	if (my_rank == 0)
	{
		std::cout << "argc:" << argc << std::endl;
		for (int i = 0; i < argc; ++i)
		{
			std::cout << "argv[" << i << "]: " << argv[i] << std::endl;
		}
	}

	if (argc != 2)
	{
		std::cerr << argv[0] << " requires 1 argument, " << argc - 1 << " provided\n";
		return 1;
	}

	cet::filepath_lookup lookup_policy("FHICL_FILE_PATH");
	fhicl::ParameterSet ps;

	auto fhicl = std::string(argv[1]);
	make_ParameterSet(fhicl, lookup_policy, ps);

	artdaq::TransferTest theTest(ps);

	//std::cout << "Entering infinite loop to connect debugger. PID=" << std::to_string(getpid()) << std::endl;
	// volatile bool loopForever = true;
	// while (loopForever) {
	// 	  usleep(1000);
	// }

	theTest.runTest();

	rc = MPI_Finalize();
	assert(rc == 0);
	TRACE(TLVL_TRACE, "s_r_handles main return" );
	return 0;
}
