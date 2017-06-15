#ifndef ARTDAQ_CORE_CORE_SHARED_MEMORY_EVENT_MANAGER_HH
#define ARTDAQ_CORE_CORE_SHARED_MEMORY_EVENT_MANAGER_HH

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq/DAQrate/RequestSender.hh"
#include <set>
#include <deque>
#include "fhiclcpp/fwd.h"

namespace artdaq {
	/**
	 * \brief The SharedMemoryEventManager is a SharedMemoryManger which tracks events as they are built
	 */
	class SharedMemoryEventManager : public SharedMemoryManager
	{
	public:
		typedef RawEvent::run_id_t run_id_t; ///< Copy RawEvent::run_id_t into local scope
		typedef RawEvent::subrun_id_t subrun_id_t; ///< Copy RawEvent::subrun_id_t into local scope
		typedef Fragment::sequence_id_t sequence_id_t; ///< Copy Fragment::sequence_id_t into local scope
		typedef std::map<sequence_id_t, RawEvent_ptr> EventMap; ///< An EventMap is a map of RawEvent_ptr objects, keyed by sequence ID

		static const std::string EVENT_RATE_STAT_KEY; ///< Key for the Event Rate MonitoredQuantity
		static const std::string INCOMPLETE_EVENT_STAT_KEY; ///< Key for the Incomplete Events MonitoredQuantity
		
		/**
		 * \brief SharedMemoryEventManager Constructor
		 * \param pset ParameterSet used to configure SharedMemoryEventManager
		 * \param num_fragments_per_event Number of Fragments to expect per event
		 * \param run Run number
		 * \param event_queue_depth Number of events in the Shared Memory (incomplete + pending art)
		 * \param art_fhicl FHiCL string used to configure art
		 * 
		 * \verbatim
		 * SharedMemoryEventManager accepts the following Parameters:
		 * 
		 * "shm_key" (Default: 0xBEE7): Key used to connect to shared memory
		 * "event_queue_depth" (Default: event_queue_depth, above): Number of events in the Shared Memory (incomplete + pending art)
		 * "max_event_size_bytes" REQUIRED: Maximum event size (all Fragments), in bytes
		 * "stale_buffer_touch_count" (Default: 0x10000): Maximum number of times a buffer may be queried before being marked as abandoned. 
		 * Owner resets this counter every time it touches the buffer.
		 * "art_analyzer_count" (Default: 1): Number of art procceses to start
		 * "fragment_count" (Default: num_fragments_per_event, above): Number of Fragments to expect per event
		 * "update_run_ids_on_new_fragment" (Default: true): Whether the run and subrun ID of an event should be updated whenever a Fragment is added.
		 * "incomplete_event_report_interval_ms" (Default: -1): Interval at which an incomplete event report should be written
		 * \endverbatim
		 */
		SharedMemoryEventManager(fhicl::ParameterSet pset, size_t num_fragments_per_event, run_id_t run,
								 size_t event_queue_depth, std::string art_fhicl);
		/**
		 * \brief SharedMemoryEventManager Destructor
		 */
		virtual ~SharedMemoryEventManager();

		/**
		 * \brief Add a Fragment to the SharedMemoryEventManager
		 * \param frag Header of the Fragment (seq ID and size info)
		 * \param dataPtr Pointer to the fragment's data (i.e. Fragment::headerAddress())
		 */
		void AddFragment(detail::RawFragmentHeader frag, void* dataPtr);
		/**
		 * \brief Check if there is space for a Fragment with the given sequence ID
		 * \param seqID Sequence ID to check
		 * \return Whether there is space (either in a new buffer or an incomplete event) for a Fragment with the given sequence ID.
		 */
		bool CheckSpace(Fragment::sequence_id_t seqID);
		/**
		 * \brief Get the number of incomplete events in the SharedMemoryEventManager
		 * \return The number of incomplete events in the SharedMemoryEventManager
		 */
		size_t GetOpenEventCount();
		/**
		 * \brief Get the count of Fragments of a given type in the buffer
		 * \param buffer Buffer ID of buffer
		 * \param type Type of fragments to count. Use InvalidFragmentType to count all fragments (default)
		 * \return Number of Fragments in buffer of given type
		 */
		size_t GetFragmentCount(int buffer, Fragment::type_t type = Fragment::InvalidFragmentType);

		/**
		 * \brief Run an art instance, recording the return codes and restarting it until the end flag is raised
		 */
		void RunArt();
		/**
		 * \brief Start all the art processes
		 */
		void StartArt();
		/**
		 * \brief Restart all art processes, using the given fhicl code to configure the new art processes
		 * \param n_art_processes Number of art processes to start, -1 (default) leaves the number unchanged
		 * \param art_fhicl Fhicl string used to configure art
		 */
		void ReconfigureArt(std::string art_fhicl, int n_art_processes = -1);

		/**
		* \brief Indicate that the end of input has been reached to the art processes.
		* \param[out] readerReturnValues Exit status codes of the art processes
		* \return True if the end proceeded correctly
		*
		* Put the end-of-data marker onto the RawEvent queue (if possible),
		* wait for the reader function to exit, and fill in the reader return
		* value.  This scenario returns true.  If the end-of-data marker
		* can not be pushed onto the RawEvent queue, false is returned.
		*/
		bool endOfData(std::vector<int>& readerReturnValues);

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
		
	private:
		size_t num_art_processes_;
		size_t const num_fragments_per_event_;
		size_t const queue_size_;
		run_id_t run_id_;
		subrun_id_t subrun_id_;
		bool update_run_ids_;

		unsigned int seqIDModulus_;
		sequence_id_t lastFlushedSeqID_;
		sequence_id_t highestSeqIDSeen_;

		int incomplete_event_report_interval_ms_;
		std::chrono::steady_clock::time_point last_incomplete_event_report_time_;

		std::string config_file_name_;
		std::vector<std::thread> art_processes_;
		std::vector<int> art_process_return_codes_;
		std::atomic<bool> restart_art_;

		RequestSender requests_;

		void broadcastFragment_(FragmentPtr frag);

		int getBufferForSequenceID_(Fragment::sequence_id_t seqID);

		void initStatistics_();

		void reportStatistics_();
	};
}

#endif //ARTDAQ_CORE_CORE_SHARED_MEMORY_EVENT_MANAGER_HH