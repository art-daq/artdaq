#define MPI_MODE 1

#include "artdaq/DAQdata/Globals.hh"
#include "proto/TransferTest.hh"
#include "fhiclcpp/make_ParameterSet.h"
#include <mpi.h>
#include <cstdlib>

#include <sys/types.h>
#include <unistd.h>

int main(int argc, char* argv[])
{
	artdaq::configureMessageFacility("transfer_driver_mpi");
	TLOG_ARB(TLVL_TRACE, "transfer_driver_mpi") << "BEGIN" << TLOG_ENDL;
	char envvar[] = "MV2_ENABLE_AFFINITY=0";
	if (putenv(envvar) != 0)
	{
		std::cerr << "Unable to set MV2_ENABLE_AFFINITY environment variable!";
		return 1;
	}
	auto const requested_threading = MPI_THREAD_SERIALIZED;
	int provided_threading = -1;
	auto rc = MPI_Init_thread(&argc, &argv, requested_threading, &provided_threading);
	assert(rc == 0);
	assert(requested_threading == provided_threading);
	TLOG_ARB(TLVL_TRACE, "transfer_driver_mpi") << "MPI_Init_thread rc=" << std::to_string(rc) << TLOG_ENDL;
	rc = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	assert(rc == 0);
	TLOG_ARB(TLVL_TRACE, "transfer_driver_mpi")<< "MPI_Comm_rank rc=" << std::to_string(rc) << TLOG_ENDL;


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
	//volatile bool loopForever = true;
	//while (loopForever) {
	//	usleep(1000);
	//}

	theTest.runTest();

	rc = MPI_Finalize();
	assert(rc == 0);
	TLOG_ARB(TLVL_TRACE, "transfer_driver_mpi") << "END" << TLOG_ENDL;
	return 0;
}
