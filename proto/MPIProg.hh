#ifndef proto_MPIProg_hh
#define proto_MPIProg_hh

#include "artdaq/DAQrate/quiet_mpi.hh"
#include "artdaq/DAQdata/Globals.hh"

/**
 * \brief A wrapper for a MPI program. Similar to MPISentry
 */
struct MPIProg
{
	/**
	 * \brief MPIProg Constructor
	 * \param argc Argument count
	 * \param argv Array of arguments as strings
	 */
	MPIProg(int argc, char** argv)
	{
		MPI_Init(&argc, &argv);
		MPI_Comm_size(MPI_COMM_WORLD, &procs_);
		MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	};
	/**
	 * \brief MPIProg destructor
	 */
	~MPIProg() { MPI_Finalize(); };

	int procs_; ///< Number of processes in MPI cluster
};


#endif /* proto_MPIProg_hh */
