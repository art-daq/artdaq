#ifndef artdaq_DAQrate_Utils_hh
#define artdaq_DAQrate_Utils_hh

#include <stdlib.h>
#include <sys/time.h>
#include <ostream>

struct LongMaker
{
	long operator()() const { return make(); }
	static long make() { return (long) lrand48(); }
};

inline double asDouble(struct timeval& t)
{
	return (double)t.tv_sec + (double)t.tv_usec / 1e6;
}

// TimedLoop should be moved to cetlib.
struct TimedLoop
{
	TimedLoop(unsigned int seconds): _seconds(seconds)
	                               , _count()
	                               , _start()
	{
		struct timeval t;
		gettimeofday(&t, 0);
		_start = t.tv_sec;
	}

	bool isDone()
	{
		bool rc = false;
		++_count;
		if (_count % 20 == 0)
		{
			struct timeval t;
			gettimeofday(&t, 0);
			if ((t.tv_sec - _start) > _seconds) { rc = true; }
		}
		return rc;
	}

	unsigned int _seconds, _count;
	unsigned long _start;
};

// efficientAddOrUpdate should be moved to cetlib The following is
// from Scott Meyer's _Effective STL_.

template <class MapType, class KeyArgType, class ValueArgType>
typename MapType::iterator
efficientAddOrUpdate(MapType& m,
                     KeyArgType const& k,
                     ValueArgType const& v)
{
	// Find where 'k' either is, or should be.
	typename MapType::iterator lb = m.lower_bound(k);
	if (lb != m.end() && !(m.key_comp()(k, lb->first)))
	{
		// Update the existing entry in the map
		lb->second = v;
	}
	else
	{
		// Add a new entry, using the hint
		typedef typename MapType::value_type MVT;
		lb = m.insert(lb, MVT(k, v));
	}
	return lb;
}


std::ostream& getStream(int rank);

void writeData(int rank, const char* d, int size);

#endif /* artdaq_DAQrate_Utils_hh */
