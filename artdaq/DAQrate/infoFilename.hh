#ifndef artdaq_DAQrate_infoFilename_hh
#define artdaq_DAQrate_infoFilename_hh

#include <string>

namespace artdaq
{
	std::string infoFilename(std::string const& prefix, int rank, int run);
}
#endif /* artdaq_DAQrate_infoFilename_hh */
