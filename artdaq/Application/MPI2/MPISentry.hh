#ifndef artdaq_Application_MPI2_MPISentry_hh
#define artdaq_Application_MPI2_MPISentry_hh

#include "artdaq/Application/TaskType.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"

namespace artdaq
{
	class MPISentry;
}

/**
 * \brief The MPISentry class initializes and finalizes the MPI context that the artdaq applciations run in
 */
class artdaq::MPISentry
{
public:
	/**
	 * \brief MPISentry Constructor
	 * \param argc_ptr Pointer to the main argc
	 * \param argv_ptr Pointer to the main argv
	 */
	MPISentry(int* argc_ptr, char*** argv_ptr);

	/**
	 * \brief MPISentry Constructor
	 * \param argc_ptr Pointer to the main argc
	 * \param argv_ptr Pointer to the main argv
	 * \param threading_level Requested MPI threading level
	 */
	MPISentry(int* argc_ptr,
	          char*** argv_ptr,
	          int threading_level);

	/**
	 * \brief MPISentry Constructor
	 * \param argc_ptr Pointer to the main argc
	 * \param argv_ptr Pointer to the main argv
	 * \param threading_level Requested MPI threading level
	 * \param type The application type of this application.
	 * \param local_group_comm The local communicatior for this application
	 */
	MPISentry(int* argc_ptr,
	          char*** argv_ptr,
	          int threading_level, artdaq::TaskType type,
	          MPI_Comm& local_group_comm);

	/**
	 * \brief MPISentry Destructor. Calls MPI_Finalize
	 */
	~MPISentry();

	/**
	 * \brief Get the actual threading level
	 * \return The actual threading level provided by MPI
	 */
	int threading_level() const;

	/**
	 * \brief Get the MPI rank of the application
	 * \return The MPI rank of the application
	 */
	int rank() const;

	/**
	 * \brief The number of processes in the MPI context
	 * \return The number of processes in the MPI context
	 */
	int procs() const;

private:
	void initialize_();

	int threading_level_;
	int rank_;
	int procs_;
};


#endif /* artdaq_Application_MPI2_MPISentry_hh */
