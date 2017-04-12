#ifndef artdaq_DAQdata_Debug_hh
#define artdaq_DAQdata_Debug_hh

#include <iostream>

class Junker
{
public:
	template <class T>
	Junker& operator<<(T const&) { return *this; }

	template <class T>
	Junker& operator<<(T&) { return *this; }
};

extern Junker junker;

std::ostream& PgetDebugStream();

void PconfigureDebugStream(int rank, int run);

#ifdef DEBUGME
#define Debug getDebugStream()
inline std::ostream & getDebugStream() { return PgetDebugStream(); }
inline void configureDebugStream(int rank, int run) { PconfigureDebugStream(rank, run); }
#define flusher std::endl
#else
#define Debug junker
inline void configureDebugStream(int, int) { }
#define flusher (char)0
#endif

#endif /* artdaq_DAQdata_Debug_hh */
