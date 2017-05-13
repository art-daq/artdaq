#ifndef artdaq_DAQrate_quiet_mpi_hh
#define artdaq_DAQrate_quiet_mpi_hh

/**
 * \file quiet_mpi.hh
 * Use this header, rather than mpi.h directly, to include the GCC
 * pragma magic to silence warnings about unused variables in mpi.h
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "mpi.h"
#pragma GCC diagnostic pop

#endif /* artdaq_DAQrate_quiet_mpi_hh */
