#ifndef ARTDAQ_CORE_CORE_SHARED_MEMORY_EVENT_MANAGER_HH
#define ARTDAQ_CORE_CORE_SHARED_MEMORY_EVENT_MANAGER_HH

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq/DAQrate/RequestSender.hh"
#include <set>
#include <deque>
#include "fhiclcpp/fwd.h"
#include "artdaq/Application/StatisticsHelper.hh"

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
				
		/**
		 * \brief SharedMemoryEventManager Constructor
		 * \param pset ParameterSet used to configure SharedMemoryEventManager
		 * \param art_pset ParameterSet used to configure art
		 * 
		 * \verbatim
		 * SharedMemoryEventManager accepts the following Parameters:
		 * 
		 * "buffer_count" REQUIRED: Number of events in the Shared Memory (incomplete + pending art)
		 * "max_event_size_bytes" REQUIRED: Maximum event size (all Fragments), in bytes
		 *  OR "max_fragment_size_bytes" REQURIED: Maximum Fragment size, in bytes
		 * "stale_buffer_touch_count" (Default: 0x10000): Maximum number of times a buffer may be queried before being marked as abandoned. 
		 * Owner resets this counter every time it touches the buffer.
		 * "art_analyzer_count" (Default: 1): Number of art procceses to start
		 * "expected_fragments_per_event" (REQUIRED): Number of Fragments to expect per event
		 * "update_run_ids_on_new_fragment" (Default: true): Whether the run and subrun ID of an event should be updated whenever a Fragment is added.
		 * "incomplete_event_report_interval_ms" (Default: -1): Interval at which an incomplete event report should be written
		 * \endverbatim
		 */
		SharedMemoryEventManager(fhicl::ParameterSet pset, fhicl::ParameterSet art_pset);
		/**
		 * \brief SharedMemoryEventManager Destructor
		 */
		virtual ~SharedMemoryEventManager();

	private:
		/**
		 * \brief Add a Fragment to the SharedMemoryEventManager
		 * \param frag Header of the Fragment (seq ID and size info)
		 * \param dataPtr Pointer to the fragment's data (i.e. Fragment::headerAddress())
		 * \param skipCheck (Default: false): Whether to skip checking for event completion (i.e. broadcastFragment_)
		 * \return Whether the Fragment was successfully added
		 */
		bool AddFragment(detail::RawFragmentHeader frag, void* dataPtr, bool skipCheck = false);

	public:
		/**
		* \brief Copy a Fragment into the SharedMemoryEventManager
		* \param frag FragmentPtr object
		* \param timeout_usec Timeout for adding Fragment to the Shared Memory
		* \param [out] outfrag Rejected Fragment if timeout occurs
		* \return Whether the Fragment was successfully added
		*/
		bool AddFragment(FragmentPtr frag, int64_t timeout_usec, FragmentPtr& outfrag);

		/**
		 * \brief Get a pointer to a reserved memory area for the given Fragment header
		 * \param frag Fragment header (contains sequence ID and size information)
		 * \return Pointer to memory location for Fragment body (Header is copied into buffer here)
		 */
		RawDataType* WriteFragmentHeader(detail::RawFragmentHeader frag);

		/**
		 * \brief Used to indicate that the given Fragment is now completely in the buffer. Will check for buffer completeness, and unset the pending flag.
		 * \param frag Fragment that is now completely in the buffer.
		 */
		void DoneWritingFragment(detail::RawFragmentHeader frag);

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
		 * \param art_pset ParameterSet used to configure art
		 * \param newRun New Run number for reconfigured art
		 * \param n_art_processes Number of art processes to start, -1 (default) leaves the number unchanged
		 */
		void ReconfigureArt(fhicl::ParameterSet art_pset, run_id_t newRun = 0, int n_art_processes = -1);

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
		* \brief Start a Run
		* \param runID Run number of the new run
		*/
		void startRun(run_id_t runID);

		/**
		* \brief Start a new Subrun, incrementing the subrun number
		*/
		void startSubrun();

		/**
		 * \brief Get the current Run number
		 * \return The current Run number
		 */
		run_id_t runID() const { return run_id_; }

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
		 * \brief Set the RequestMessageMode for all outgoing data requests
		 * \param mode Mode to set
		 */
		void setRequestMode(detail::RequestMessageMode mode) { requests_.SetRequestMode(mode); }
		
		/**
		 * \brief Set the overwrite flag (non-reliable data transfer) for the Shared Memory
		 * \param overwrite Whether to allow the writer to overwrite data that has not yet been read
		 */
		void setOverwrite(bool overwrite) { overwrite_mode_ = overwrite; }

		/**
		 * \brief Write out information about the Shared Memory to a string
		 * \return String containing information about the current Shared Memory buffers
		 */
		std::string toString() override;

	private:
		size_t num_art_processes_;
		size_t const num_fragments_per_event_;
		size_t const queue_size_;
		run_id_t run_id_;
		subrun_id_t subrun_id_;
		bool update_run_ids_;
		bool overwrite_mode_;

		std::unordered_map<int,std::atomic<int>> buffer_writes_pending_;
		std::unordered_map<int, std::mutex> buffer_write_mutexes_;
		std::mutex seq_id_buffer_mutex_;
		
		int incomplete_event_report_interval_ms_;
		std::chrono::steady_clock::time_point last_incomplete_event_report_time_;
		int broadcast_timeout_ms_;

		std::string config_file_name_;
		std::vector<std::thread> art_processes_;
		std::vector<int> art_process_return_codes_;
		std::atomic<bool> restart_art_;

		RequestSender requests_;
		
		FragmentPtr init_fragment_;

		void broadcastFragment_(FragmentPtr frag);

		detail::RawEventHeader* getEventHeader_(int buffer);

		int getBufferForSequenceID_(Fragment::sequence_id_t seqID, Fragment::timestamp_t timestamp = Fragment::InvalidTimestamp);

		void configureArt_(fhicl::ParameterSet art_pset);
	};
}

#endif //ARTDAQ_CORE_CORE_SHARED_MEMORY_EVENT_MANAGER_HH