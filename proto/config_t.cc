#include "Config.hh"
#include "MPIProg.hh"
#include <cstring>
#include <cassert>

using namespace std;

int main()
{
	char const* argv[] = {"execname", "5", "5", "100", "6000140",
		"2", "1", "--", "a", "bc", "de f"
	};
	int argc = sizeof(argv) / sizeof(char *);
	MPIProg mpiSentry(argc, const_cast<char **>(argv));
	int rank = 1;
	int nprocs = 15;
	Config cfg(rank, nprocs, 10, 0x100000, argc, const_cast<char **>(argv));
	assert(cfg.art_argc_ == 4);
	assert(strcmp(cfg.art_argv_[0], "--") == 0);
	assert(strcmp(cfg.art_argv_[1], "a") == 0);
	assert(strcmp(cfg.art_argv_[2], "bc") == 0);
	assert(strcmp(cfg.art_argv_[3], "de f") == 0);
	assert(!cfg.use_artapp_);
}
