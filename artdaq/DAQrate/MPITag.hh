#ifndef artdaq_DAQrate_MPITag_hh
#define artdaq_DAQrate_MPITag_hh

#include <cstdint>

namespace artdaq
{
	// This is to ensure that access to the tags is scoped only per
	// C++2011 and we don't pollute artdaq namespace unnecessarily. Don't
	// want enum class because we need to be able to convert to integral
	// types easily for use with MPI.
	namespace detail
	{
		enum MPITag : uint8_t
		{
			FINAL = 1,
			INCOMPLETE = 2
		};
	}

	typedef detail::MPITag MPITag;
}

#endif /* artdaq_DAQrate_MPITag_hh */
