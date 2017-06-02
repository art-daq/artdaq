#ifndef artdaq_DAQrate_EventStore_hh
#define artdaq_DAQrate_EventStore_hh

#include "artdaq/DAQdata/Globals.hh" // Before trace.h gets included in ConcurrentQueue (from GlobalQueue)
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq-core/Core/GlobalQueue.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"
#include "fhiclcpp/ParameterSet.h"

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

	/**
	 * \brief The EventStore class collects Fragment objects, until it receives a complete
	 * event, at which point the event is handed over to the art thread
	 * \todo Make the art thread a separate process
	 * 
	 * An EventStore is given Fragments, which it collects until it
	 * finds it has a complete RawEvent. When a complete RawEvent is
	 * assembled, the EventStore puts it onto the global RawEvent queue.
	 * There should be only one EventStore per process; an MPI program
	 * can thus have multiple EventStores. By construction, each
	 * EventStore will only deal with events (and fragments) from a
	 * single run.
	 *
	 * The EventStore is also responsible for starting the thread that
	 * will be popping events off the global queue. This is so that the
	 * EventStore is guaranteed to live long enough to allow the global
	 * queue to be drained. The current implementation uses only a free
	 * function as the 'thread function' for this thread.
	 *
	 * A future enhancement of EventStore may make it be able to move
	 * from handling run X to handling run Y; such an enhancement will
	 * have to include how to deal with any incomplete events in storage
	 * at the time of the introduction of the new run.
	 */
	class EventStore
	{
	public:
		/**
		 * \brief An art function that accepts standard C main arguments
		 * \return Return code from art
		 */
		typedef int (ART_CMDLINE_FCN)(int, char**);

		/**
		 * \brief An art function that accepts a fhicl::ParameterSet as a string
		 * \return Return code from art
		 */
		typedef int (ART_CFGSTRING_FCN)(const std::string&);

		typedef RawEvent::run_id_t run_id_t; ///< Copy RawEvent::run_id_t into local scope
		typedef RawEvent::subrun_id_t subrun_id_t; ///< Copy RawEvent::subrun_id_t into local scope
		typedef Fragment::sequence_id_t sequence_id_t; ///< Copy Fragment::sequence_id_t into local scope
		typedef std::map<sequence_id_t, RawEvent_ptr> EventMap; ///< An EventMap is a map of RawEvent_ptr objects, keyed by sequence ID

		static const std::string EVENT_RATE_STAT_KEY; ///< Key for the Event Rate MonitoredQuantity
		static const std::string INCOMPLETE_EVENT_STAT_KEY; ///< Key for the Incomplete Events MonitoredQuantity

		/**
		 * \brief This enumeration contains possible status codes of insertion attempts
		 */
		enum class EventStoreInsertResult : int
		{
			REJECT_QUEUEFULL, ///< The Fragment was rejected, because the RawEventQueue is full
			SUCCESS, ///< The Fragment was successfully inserted
			SUCCESS_STOREFULL, ///< The EventStore is full, but the Fragment was accepted as it is for an already-open event
			REJECT_STOREFULL ///< The EventStore is full, and the Fragment was rejected
		};

		/**
		 * \brief Default Constructor is deleted
		 */
		EventStore() = delete;

		/**
		 * \brief Copy Constructor is deleted
		 */
		EventStore(EventStore const&) = delete;

		/**
		 * \brief Copy Assignment operator is deleted
		 * \return EventStore copy
		 */
		EventStore& operator=(EventStore const&) = delete;

		/**
		 * \brief EventStore Constructor
		 * \param pset ParameterSet used to configured EventStore
		 * \param num_fragments_per_event Number of fragments per event
		 * \param run Run Number
		 * \param event_queue_depth Default for the event_queue_depth, if not specified in pset
		 * \param max_incomplete_events Default for the max_incomplete_events, if not specified in pset
		 * 
		 * This constructor is not meant to be called directly; it handles all of the EventStore setup
		 * for the other constructors which merely start the art thread.
		 * 
		 * \verbatim
		 * EventStore accepts the following Parameters:
		 * "event_queue_depth" (Default: event_queue_depth): The size of the ConcurrentQueue to allocate
		 * "max_incomplete_events" (Default: max_incomplete_events): The maximum size of the EventStore
		 * "send_requests" (Default: false): Whether to send DataRequests when new sequence IDs are seen
		 * "request_port" (Default: 3001): Port to send DataRequests on
		 * "request_delay_ms" (Default: 10): How long to wait before sending new DataRequests
		 * "output_address" (Default: "localhost"): Use this hostname for multicast output (to assign to the proper NIC)
		 * "event_queue_wait_time" (Default: 5.0): 
		 * "event_queue_check_count" (Default: 5000):
		 * "print_event_store_stats" (Default: false):
		 * "incomplete_event_report_interval_ms" (Default: -1):
		 * "art_thread_wait_ms" (Default: 4000): Amount of time to wait for the art thread to start dequeuing events
		 * "request_address" (Default: "227.128.12.26"): Multicast address to send DataRequests to
		 * "routing_token_config" (Default: Empty table): FHiCL table containing RoutingToken configuration
		 *   "use_routing_master" (Default: false): Whether to send tokens to a RoutingMaster
		 *   "routing_token_port" (Default: 35555): Port to send tokens on
		 *   "routing_master_hostname" (Default: "localhost"): Hostname or IP of RoutingMaster
		 * \endverbatim
		 */
		EventStore(const fhicl::ParameterSet& pset,
				   size_t num_fragments_per_event, run_id_t run,
				   size_t event_queue_depth, size_t max_incomplete_events);

		/**
		 * \brief EventStore Constructor
		 * \param pset ParameterSet used to configured EventStore
		 * \param num_fragments_per_event Number of fragments per event
		 * \param run Run Number
		 * \param argc Number of arguments
		 * \param argv Array of arguments, as strings
		 * \param reader art function to start
		 * 
		 * This constructor deletgates to the first constructor, with event_queue_depth and max_incomplete_events set to 50.
		 */
		EventStore(const fhicl::ParameterSet& pset,
				   size_t num_fragments_per_event, run_id_t run,
				   int argc, char* argv[],
				   ART_CMDLINE_FCN* reader);

		/**
		 * \brief EventStore Constructor
		 * \param pset ParameterSet used to configured EventStore
		 * \param num_fragments_per_event Number of fragments per event
		 * \param run Run Number
		 * \param configString fhicl::ParameterSet string for configuring art thread
		 * \param reader art function to start
		 * 
		 * This constructor deletgates to the first constructor, with event_queue_depth and max_incomplete_events set to 20.
		 */
		EventStore(const fhicl::ParameterSet& pset,
				   size_t num_fragments_per_event, run_id_t run,
				   const std::string& configString,
				   ART_CFGSTRING_FCN* reader);

		/**
		 * \brief EventStore Destructor
		 */
		virtual ~EventStore();

		/**
		 * \brief Give ownership of the Fragment to the EventStore.
		 * \param pfrag Fragment to insert
		 * \param printWarningWhenFragmentIsDropped Print a warning if the Fragment is dopped (data loss) Default = true
		 * 
		 * Give ownership of the Fragment to the EventStore. The pointer
		 * we are given must NOT be null, and the Fragment to which it
		 * points must NOT be empty; the Fragment must at least contain
		 * the necessary header information.
		 * -> this instance of the insert method discards completed
		 *    events if they can't be pushed onto the event queue within
		 *    the specified timeout. This is a rather severe response, so
		 *    this method should only be used in special cases.
		 */
		void insert(FragmentPtr pfrag,
					bool printWarningWhenFragmentIsDropped = true);

		/**
		 * \brief  Give ownership of the Fragment to the EventStore.
		 * \param pfrag Fragment to insert
		 * \param[out] rejectedFragment If the Fragment could not be inserted, it will be returned here
		 * \return EventStoreInsertResult detailing the status of the EventStore insertion operation
		 * 
		 * Give ownership of the Fragment to the EventStore. The pointer
		 * we are given must NOT be null, and the Fragment to which it
		 * points must NOT be empty; the Fragment must at least contain
		 * the necessary header information.
		 * -> this instance of the insert method returns an EventStoreInsertResult,
		 *    and returns the fragment to the caller, if there is no
		 *    room to push events onto the event queue, within the
		 *    timeout that was specified at construction time.
		 */
		EventStoreInsertResult insert(FragmentPtr pfrag, FragmentPtr& rejectedFragment);

		/**
		 * \brief Indicate that the end of input has been reached to the art thread.
		 * \param[out] readerReturnValue Exit status code of the art thread
		 * \return True if the end proceeded correctly
		 * 
		 * Put the end-of-data marker onto the RawEvent queue (if possible),
		 * wait for the reader function to exit, and fill in the reader return
		 * value.  This scenario returns true.  If the end-of-data marker
		 * can not be pushed onto the RawEvent queue, false is returned.
		 */
		bool endOfData(int& readerReturnValue);

		/**
		 * \brief Set the parameter that will be used to determine which sequence IDs get
		 * grouped together into events.
		 * \param seqIDModulus Sequence ID grouping parameter
		 * 
		 * Set the parameter that will be used to determine which sequence IDs get
		 * grouped together into events.  This defaults to 1 which is the case where
		 * fragments with the same sequence ID will get grouped together.  The other
		 * use case is for the aggregator which will group together fragments with
		 * different sequence IDs.
		 */
		void setSeqIDModulus(unsigned int seqIDModulus);

		/**
		 * \brief Push any incomplete events onto the queue.
		 * \return Returns true if all stale events were flushed, false if one or more events
		 * could not be flushed because the queue was full.
		 */
		bool flushData();

		/**
		 * \brief Start a Run
		 * \param runID Run number of the new run
		 */
		void startRun(run_id_t runID);

		/**
		 * \brief Start a new Subrun, incrementing the subrun number
		 */
		void startSubrun();

		/**
		 * \brief Get the current subrun number
		 * \return The current subrun number
		 */
		subrun_id_t subrunID() const { return subrun_id_; }

		/**
		 * \brief Send an EndOfRunFragment to the art thread
		 * \return True if enqueue successful
		 */
		bool endRun();

		/**
		* \brief Send an EndOfSubRunFragment to the art thread
		* \return True if enqueue successful
		*/
		bool endSubrun();

		/**
		 * \brief Send metrics to the MetricManager, if one has been instantiated in the application
		 */
		void sendMetrics();

		/**
		 * \brief Get the number of events currently being built in the EventStore
		 * \return The number of events currently being built in the EventStore
		 */
		size_t incompleteEventCount() const { return events_.size(); }

		/**
		 * \brief Set the mode for RequestMessages. Used to indicate when EventStore should enter "EndOfRun" mode
		 * \param mode Mode to set
		 */
		void setRequestMode(detail::RequestMessageMode mode) { request_mode_ = mode; }

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
		detail::RequestMessageMode request_mode_;

		unsigned int seqIDModulus_;
		sequence_id_t lastFlushedSeqID_;
		sequence_id_t highestSeqIDSeen_;

		artdaq::detail::seconds const enq_timeout_;
		size_t enq_check_count_;
		bool const printSummaryStats_;

		int incomplete_event_report_interval_ms_;
		std::chrono::steady_clock::time_point last_incomplete_event_report_time_;

		bool send_routing_tokens_;
		int token_port_;
		int token_socket_;
		struct sockaddr_in token_addr_;
		std::string token_address_;

				int art_thread_wait_ms_;

	private:
		void initStatistics_();

		void reportStatistics_();

		void setup_requests_(std::string trigger_addr);

		void send_request_();

		void do_send_request_();

		void setup_tokens_();

		void send_routing_token_(int nSlots);
	};
}
#endif /* artdaq_DAQrate_EventStore_hh */
