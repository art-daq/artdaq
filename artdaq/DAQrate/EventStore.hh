#ifndef artdaq_DAQrate_EventStore_hh
#define artdaq_DAQrate_EventStore_hh

#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-core/Core/GlobalQueue.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include <fhiclcpp/ParameterSet.h>

#include <map>
#include <memory>
#include <chrono>
//#include <thread>
#include <future>
#include <stdint.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

namespace artdaq
{
	// An EventStore is given Fragments, which it collects until it
	// finds it has a complete RawEvent. When a complete RawEvent is
	// assembled, the EventStore puts it onto the global RawEvent queue.
	// There should be only one EventStore per process; an MPI program
	// can thus have multiple EventStores. By construction, each
	// EventStore will only deal with events (and fragments) from a
	// single run.
	//
	// The EventStore is also responsible for starting the thread that
	// will be popping events off the global queue. This is so that the
	// EventStore is guaranteed to live long enough to allow the global
	// queue to be drained. The current implementation uses only a free
	// function as the 'thread function' for this thread.
	//
	// A future enhancement of EventStore may make it be able to move
	// from handling run X to handling run Y; such an enhancement will
	// have to include how to deal with any incomplete events in storage
	// at the time of the introduction of the new run.

	class EventStore
	{
	public:
		typedef int (ART_CMDLINE_FCN)(int, char**);

		typedef int (ART_CFGSTRING_FCN)(const std::string&);

		typedef RawEvent::run_id_t run_id_t;
		typedef RawEvent::subrun_id_t subrun_id_t;
		typedef Fragment::sequence_id_t sequence_id_t;
		typedef std::map<sequence_id_t, RawEvent_ptr> EventMap;

		static const std::string EVENT_RATE_STAT_KEY;
		static const std::string INCOMPLETE_EVENT_STAT_KEY;

		enum class EventStoreInsertResult : int
		{
			REJECT_QUEUEFULL,
			SUCCESS,
			SUCCESS_STOREFULL,
			REJECT_STOREFULL
		};

		EventStore() = delete;

		EventStore(EventStore const&) = delete;

		EventStore& operator=(EventStore const&) = delete;

		// Create an EventStore that uses 'reader' as the function to be
		// executed by the thread this EventStore will spawn.
		EventStore(fhicl::ParameterSet pset,
		           size_t num_fragments_per_event, run_id_t run,
		           int argc, char* argv[],
		           ART_CMDLINE_FCN* reader);

		EventStore(fhicl::ParameterSet pset,
		           size_t num_fragments_per_event, run_id_t run,
		           const std::string& configString,
		           ART_CFGSTRING_FCN* reader);

		~EventStore();

		// Give ownership of the Fragment to the EventStore. The pointer
		// we are given must NOT be null, and the Fragment to which it
		// points must NOT be empty; the Fragment must at least contain
		// the necessary header information.
		// -> this first instance of the insert method discards completed
		//    events if they can't be pushed onto the event queue within
		//    the specified timeout. This is a rather severe response, so
		//    this method should only be used in special cases.
		void insert(FragmentPtr pfrag,
		            bool printWarningWhenFragmentIsDropped = true);

		// -> this second instance of the insert method returns false,
		//    and returns the fragment to the caller, if there is no
		//    room to push events onto the event queue, within the
		//    timeout that was specified at construction time.
		EventStoreInsertResult insert(FragmentPtr pfrag, FragmentPtr& rejectedFragment);

		// Put the end-of-data marker onto the RawEvent queue (if possible),
		// wait for the reader function to exit, and fill in the reader return
		// value.  This scenario returns true.  If the end-of-data marker
		// can not be pushed onto the RawEvent queue, false is returned.
		bool endOfData(int& readerReturnValue);

		// Set the parameter that will be used to determine which sequence IDs get
		// grouped together into events.  This defaults to 1 which is the case where
		// fragments with the same sequence ID will get grouped together.  The other
		// use case is for the aggregator which will group together fragments with
		// different sequence IDs.
		void setSeqIDModulus(unsigned int seqIDModulus);

		// Push any incomplete events onto the queue.  Returns true if
		// all stale events were flushed, false if one or more events
		// could not be flushed because the queue was full.
		bool flushData();

		void startRun(run_id_t runID);

		void startSubrun();

		subrun_id_t subrunID() const { return subrun_id_; }

		// These methods return true if the relevant markers were pushed
		// onto the RawEvent queue, false if not.
		bool endRun();

		bool endSubrun();

		void sendMetrics();

		size_t incompleteEventCount() const { return events_.size(); }

	private:
		// id_ is the unique identifier of this object; MPI programs will
		// use the MPI rank to fill in this value.
		size_t const num_fragments_per_event_;
		size_t const max_queue_size_;
		size_t const max_incomplete_count_;
		run_id_t run_id_;
		subrun_id_t subrun_id_;
		EventMap events_;
		RawEventQueue& queue_;
		std::chrono::steady_clock::time_point reader_thread_launch_time_;
		std::future<int> reader_thread_;

		bool send_requests_;
		std::mutex request_mutex_;
		std::map<Fragment::sequence_id_t, Fragment::timestamp_t> active_requests_;
		int request_port_;
		size_t request_delay_;
		int request_socket_;
		struct sockaddr_in request_addr_;
		std::string multicast_out_addr_;

		unsigned int seqIDModulus_;
		sequence_id_t lastFlushedSeqID_;
		sequence_id_t highestSeqIDSeen_;

		artdaq::detail::seconds const enq_timeout_;
		size_t enq_check_count_;
		bool const printSummaryStats_;

		int incomplete_event_report_interval_ms_;
		std::chrono::steady_clock::time_point last_incomplete_event_report_time_;

	private:
		void initStatistics_();

		void reportStatistics_();

		void setup_requests_(std::string trigger_addr);

		void send_request_();

		void do_send_request_();
	};
}
#endif /* artdaq_DAQrate_EventStore_hh */
