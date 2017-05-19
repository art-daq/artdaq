#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/Application/MPI2/MPISentry.hh"
#include "artdaq/DAQrate/quiet_mpi.hh"
#include "cetlib/exception.h"

#include <sstream>

artdaq::MPISentry::
MPISentry(int* argc_ptr, char*** argv_ptr)
	:
	threading_level_(0)
	, rank_(-1)
	, procs_(0)
{
	MPI_Init(argc_ptr, argv_ptr);
	initialize_();
}

artdaq::MPISentry::
MPISentry(int* argc_ptr,
		  char*** argv_ptr,
		  int threading_level)
	:
	threading_level_(0)
	, rank_(-1)
	, procs_(0)
{
	MPI_Init_thread(argc_ptr, argv_ptr, threading_level, &threading_level_);
	initialize_();

	std::ostringstream threadresult;
	threadresult << "MPI initialized with requested thread support level of "
		<< threading_level << ", actual support level = "
		<< threading_level_ << ".";

	TLOG_DEBUG("MPISentry") << threadresult.str() << TLOG_ENDL;

	if (threading_level != threading_level_) throw cet::exception("MPISentry") << threadresult.str();

	TLOG_DEBUG("MPISentry")
		<< "size = "
		<< procs_
		<< ", rank = "
		<< rank_ << TLOG_ENDL;
}

artdaq::MPISentry::
MPISentry(int* argc_ptr,
		  char*** argv_ptr,
		  int threading_level, artdaq::TaskType type, MPI_Comm& local_group_comm)
	:
	threading_level_(0)
	, rank_(-1)
	, procs_(0)
{
	MPI_Init_thread(argc_ptr, argv_ptr, threading_level, &threading_level_);
	initialize_();

	std::ostringstream threadresult;
	threadresult << "MPI initialized with requested thread support level of "
		<< threading_level << ", actual support level = "
		<< threading_level_ << ".";

	TLOG_DEBUG("MPISentry") << threadresult.str() << TLOG_ENDL;

	if (threading_level != threading_level_) throw cet::exception("MPISentry") << threadresult.str();

	TLOG_DEBUG("MPISentry")
		<< "size = "
		<< procs_
		<< ", rank = "
		<< rank_ << TLOG_ENDL;

	std::ostringstream groupcommresult;

	int status = MPI_Comm_split(MPI_COMM_WORLD, type, 0, &local_group_comm);

	if (status == MPI_SUCCESS)
	{
		int temp_rank;
		MPI_Comm_rank(local_group_comm, &temp_rank);

		groupcommresult << "Successfully created local communicator for type "
			<< type << ", identifier = 0x"
			<< std::hex << local_group_comm << std::dec
			<< ", rank = " << temp_rank << ".";

		TLOG_DEBUG("MPISentry") << groupcommresult.str() << TLOG_ENDL;
	}
	else
	{
		groupcommresult << "Failed to create the local MPI communicator group for "
			<< "task type #" << type << ", status code = " << status << ".";
		throw cet::exception("MPISentry") << groupcommresult.str();
	}
}

artdaq::MPISentry::
~MPISentry()
{
	MPI_Finalize();
}

int
artdaq::MPISentry::
threading_level() const
{
	return threading_level_;
}

int
artdaq::MPISentry::
rank() const
{
	return rank_;
}

int
artdaq::MPISentry::
procs() const
{
	return procs_;
}

void
artdaq::MPISentry::
initialize_()
{
	MPI_Comm_size(MPI_COMM_WORLD, &procs_);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
}
