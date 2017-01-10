#ifndef TIMEOUT_HH
#define TIMEOUT_HH
/*  This file (Timeout.h) was created by Ron Rechenmacher <ron@fnal.gov> on
    Sep 28, 2009. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile: Timeout.h,v $
    rev="$Revision: 1.9 $$Date: 2016/10/12 07:11:55 $";
    */
#include <time.h>               // struct timespec
#include <list>
#include <functional>			// std::function
#include <string>
#include <mutex>
#include <vector>
#include <map>
#include <unordered_map>

class Timeout
{
public:
    struct timeoutspec
    {
#if 0
		timeoutspec();
		timeoutspec( const timeoutspec & other );
		Timeout::timeoutspec & operator=( const Timeout::timeoutspec & other );
#endif
		std::string desc;
		void *		tag;     // could be file descriptor (fd)
		std::function<void()> function;
		uint64_t	tmo_tod_us;
		uint64_t	period_us;   /* 0 if not periodic */
		int		missed_periods;
		int     check;
    };

    Timeout( int max_tmos=100 );

	// maybe need to return a timeout id??   
    void add_periodic(  const char* desc, void* tag, std::function<void()> &function
	             , uint64_t period_us
	             , uint64_t start_us=0 );

	// maybe need to return a timeout id??   
    void add_periodic(  const char* desc, void* tag, std::function<void()> &function
	             , int	rel_ms );

	// maybe need to return a timeout id??   
    void add_periodic(  const char* desc
	             , uint64_t period_us
	             , uint64_t start_us=0 );

	// maybe need to return a timeout id??   
    void add_relative( const char* desc, void* tag, std::function<void()> &function
	             , int               rel_ms );

	// maybe need to return a timeout id??   
    void add_relative( std::string desc, int rel_ms );

    void copy_in_timeout( const char *desc, uint64_t period_us, uint64_t start_us=0 );

    // maybe need to be able to cancel by id,and/or desc and/or Client/funptr
    bool cancel_timeout(  void* tag, std::string desc );

	// return -1 if no timeout. ts is an output
    int get_next_expired_timeout(  std::string &desc, void** tag, std::function<void()> &function
	                         , uint64_t *tmo_tod_us );

    void get_next_timeout_delay( int64_t *delay_us );
    int  get_next_timeout_msdly();


    // return a reference to a timespec with the current time (similar to
    // gettimeofday(*timeval)
    uint64_t gettimeofday_us();

	bool is_consistent();
    void list_active_time();

private:

	std::mutex                             lock_mutex_;
	std::vector<timeoutspec>               tmospecs_;
    std::list<size_t>                      free_;   // list of tmospecs indexes
    std::multimap<uint64_t,size_t>         active_time_;
    std::unordered_multimap<std::string,size_t> active_desc_;

    void timeoutlist_init();
    int  tmo_is_before_ts(  timeoutspec &tmo
	                      , const timespec    &ts );
    int get_clear_next_expired_timeout(  timeoutspec &tmo
	                                    , uint64_t tod_now_us );
    void copy_in_timeout( timeoutspec &tmo );
};

#endif // TIMEOUT_HH
