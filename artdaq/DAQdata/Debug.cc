#include "artdaq/DAQdata/Debug.hh"
#include <iomanip>
#include <string>
#include <sstream>
#include <fstream>

using namespace std;

Junker junker;

namespace
{
	ostream* debug_str = 0;

	struct cleaner
	{
		~cleaner() { if (debug_str) { delete debug_str; } }
	};

	cleaner do_cleanup;
}

void PconfigureDebugStream(int rank, int run)
{
	if (debug_str == 0)
	{
		ostringstream os;
		os << "debug_" << setfill('0') << setw(4) << run << "_" << setfill('0') << setw(4) << rank << ".txt";
		debug_str = new ofstream(os.str().c_str());
	}
}

std::ostream& PgetDebugStream()
{
	return debug_str ? *debug_str : std::cerr;
}
