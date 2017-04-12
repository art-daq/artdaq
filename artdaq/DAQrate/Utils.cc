#include "artdaq/DAQrate/Utils.hh"

#include <fstream>
#include <sstream>

using namespace std;

std::ostream& getStream(int rank)
{
	ostringstream ost;
	ost << "save" << rank << ".dat";
	static ofstream of(ost.str().c_str());
	return of;
}

void writeData(int rank, const char* d, int size)
{
	ostream& ost = getStream(rank);
	ost.write(d, size);
}
