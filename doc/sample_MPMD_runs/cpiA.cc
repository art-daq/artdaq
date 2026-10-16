/*
   (C) 2001 by Argonne National Laboratory.
       See COPYRIGHT in top-level directory.
*/
#include <mpi.h>
#include <cmath>
#include <cstdio>
#include <iostream>

using namespace std;

extern char** environ;

double f(double);

double f(double a)
{
	return (4.0 / (1.0 + a * a));
}

int main(int argc, char* argv[])
{
	// for(int i=0;environ[i]!=0;++i) cout << environ[i] << "\n";

	cout << "BooA" << endl;

	int done = 0, n, myid, numprocs, i;  // NOLINT
	double PI25DT = 3.141592653589793238462643;
	double mypi, pi, h, sum, x;         // NOLINT
	double startwtime = 0.0, endwtime;  // NOLINT
	int namelen = 0;
	char processor_name[MPI_MAX_PROCESSOR_NAME];  // NOLINT

	for (int i = 0; i < argc; ++i)
		cout << "argv[" << i << "] = " << argv[i] << endl;  // NOLINT

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &myid);
	MPI_Get_processor_name(processor_name, &namelen);

	fprintf(stderr, "Process %d on %s\n",
	        myid, processor_name);

	n = 0;
	while (!done)
	{
		if (myid == 0)
		{
			if (n == 0)
				n = 100;
			else
				n = 0;

			startwtime = MPI_Wtime();
		}
		MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);
		if (n == 0)
			done = 1;
		else
		{
			h = 1.0 / static_cast<double>(n);
			sum = 0.0;
			for (i = myid + 1; i <= n; i += numprocs)
			{
				x = h * (static_cast<double>(i) - 0.5);
				sum += f(x);
			}
			mypi = h * sum;

			MPI_Reduce(&mypi, &pi, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

			if (myid == 0)
			{
				printf("pi is approximately %.16f, Error is %.16f\n",
				       pi, fabs(pi - PI25DT));
				endwtime = MPI_Wtime();
				printf("wall clock time = %f\n",
				       endwtime - startwtime);
			}
		}
	}
	MPI_Finalize();
	return 0;
}
