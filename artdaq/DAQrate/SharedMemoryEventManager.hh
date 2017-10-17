#ifndef ARTDAQ_DAQRATE_SHAREDMEMORYEVENTMANAGER_HH
#define ARTDAQ_DAQRATE_SHAREDMEMORYEVENTMANAGER_HH

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Data/RawEvent.hh"
#include "artdaq/DAQrate/RequestSender.hh"
#include <set>
#include <deque>
#include <fstream>
#include <iomanip>
#include "fhiclcpp/fwd.h"
#include "artdaq/Application/StatisticsHelper.hh"

namespace artdaq {

	/**
	 * \brief art_config_file wraps a temporary file used to configure art
	 */
	class art_config_file
	{
	public:
		/**
		 * \brief art_config_file Constructor
		 * \param ps ParameterSet to write to temporary file
		 */
		art_config_file(fhicl::ParameterSet ps/*, uint32_t shm_key, uint32_t broadcast_key*/) : file_name_(std::tmpnam(nullptr))
		{
			std::ofstream of(file_name_, std::ofstream::trunc);
			of << ps.to_string();

			//if (ps.has_key("services.NetMonTransportServiceInterface"))
			//{
			//	of << " services.NetMonTransportServiceInterface.shared_memory_key: 0x" << std::hex << shm_key;
			//	of << " services.NetMonTransportServiceInterface.broadcast_shared_memory_key: 0x" << std::hex << broadcast_key;
			//	of << " services.NetMonTransportServiceInterface.rank: " << std::dec << my_rank;
			//}
			if (!ps.has_key("services.message"))
			{
				of << " services.message: { " << generateMessageFacilityConfiguration("art") << "} ";
			}
			//of << " source.shared_memory_key: 0x" << std::hex << shm_key;
			//of << " source.broadcast_shared_memory_key: 0x" << std::hex << broadcast_key;
			//of << " source.rank: " << std::dec << my_rank;
			of.close();
		}
		~art_config_file() { remove(file_name_.c_str()); }
		/**
		 * \brief Get the path of the temporary file
		 * \return The path of the temporary file
		 */
		std::string getFileName() const { return file_name_; }
	private:
		std::string file_name_;
	};

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
		 * \return Whether the Fragment was successfully added
		 */
		bool AddFragment(detail::RawFragmentHeader frag, void* dataPtr);

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
		 * \param dropIfNoBuffersAvailable Whether to drop the fragment (instead of returning nullptr) when no buffers are available (Default: false)
		 * \return Pointer to memory location for Fragment body (Header is copied into buffer here)
		 */
		RawDataType* WriteFragmentHeader(detail::RawFragmentHeader frag, bool dropIfNoBuffersAvailable = false);

		/**
		 * \brief Used to indicate that the given Fragment is now completely in the buffer. Will check for buffer completeness, and unset the pending flag.
		 * \param frag Fragment that is now completely in the buffer.
		 */
		void DoneWritingFragment(detail::RawFragmentHeader frag);

		/**
		* \brief Returns the number of buffers which have no data
		* \return The number of buffers which have no data
		*/
		size_t GetInactiveEventCount() { return inactive_buffers_.size(); }

		/**
		* \brief Returns the number of buffers which contain data but are not yet complete
		* \return The number of buffers which contain data but are not yet complete
		*/
		size_t GetIncompleteEventCount() { return active_buffers_.size(); }

		/**
		* \brief Returns the number of events which are complete but waiting on lower sequenced events to finish
		* \return The number of events which are complete but waiting on lower sequenced events to finish
		*/
		size_t GetPendingEventCount() { return pending_buffers_.size(); }

		/**
		* \brief Returns the number of buffers currently owned by this manager
		* \return The number of buffers currently owned by this manager
		*/
		size_t GetLockedBufferCount() { return GetBuffersOwnedByManager().size(); }

		/**
		 * \brief Returns the number of events sent to art this subrun
		 * \return The number of events sent to art this subrun
		 */
		size_t GetArtEventCount() { return subrun_event_count_; }

		/**
		 * \brief Get the count of Fragments of a given type in an event
		 * \param seqID Sequence ID of Fragments
		 * \param type Type of fragments to count. Use InvalidFragmentType to count all fragments (default)
		 * \return Number of Fragments in event of given type
		 */
		size_t GetFragmentCount(Fragment::sequence_id_t seqID, Fragment::type_t type = Fragment::InvalidFragmentType);

		/**
		 * \brief Run an art instance, recording the return codes and restarting it until the end flag is raised
		 */
		void RunArt(std::shared_ptr<art_config_file> config_file, pid_t& pid_out);
		/**
		 * \brief Start all the art processes
		 */
		void StartArt();

		/**
		 * \brief Start one art process
		 * \param pset ParameterSet to send to this art process
		 * \return pid_t of the started process
		 */
		pid_t StartArtProcess(fhicl::ParameterSet pset);

		/**
		 * \brief Shutdown a set of art processes
		 * \param pids PIDs of the art processes
		 */
		void ShutdownArtProcesses(std::set<pid_t> pids);

		/**
		 * \brief Restart all art processes, using the given fhicl code to configure the new art processes
		 * \param art_pset ParameterSet used to configure art
		 * \param newRun New Run number for reconfigured art
		 * \param n_art_processes Number of art processes to start, -1 (default) leaves the number unchanged
		 */
		void ReconfigureArt(fhicl::ParameterSet art_pset, run_id_t newRun = 0, int n_art_processes = -1);

		/**
		* \brief Indicate that the end of input has been reached to the art processes.
		* \return True if the end proceeded correctly
		*
		* Put the end-of-data marker onto the RawEvent queue (if possible),
		* wait for the reader function to exit, and fill in the reader return
		* value.  This scenario returns true.  If the end-of-data marker
		* can not be pushed onto the RawEvent queue, false is returned.
		*/
		bool endOfData();

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
		 * \brief Set the stored Init fragment, if one has not yet been set already.
		 */
		void SetInitFragment(FragmentPtr frag);

		/**
		 * \brief Gets the shared memory key of the broadcast SharedMemoryManager
		 * \return The shared memory key of the broadcast SharedMemoryManager
		 */
		uint32_t GetBroadcastKey() { return broadcasts_.GetKey(); }

		/**
		 * \brief Gets the address of the "dropped data" fragment. Used for testing.
		 * \return Pointer to the data payload of the "dropped data" fragment
		 */
		RawDataType* GetDroppedDataAddress() { return dropped_data_->dataBegin(); }

	private:
		size_t num_art_processes_;
		size_t const num_fragments_per_event_;
		size_t const queue_size_;
		run_id_t run_id_;
		subrun_id_t subrun_id_;
		Fragment::sequence_id_t sequence_id_;

		std::set<int> inactive_buffers_;
		std::set<int> active_buffers_;
		std::set<int> pending_buffers_;

		bool update_run_ids_;
		bool overwrite_mode_;
		bool every_seqid_expected_;

		std::unordered_map<int, std::atomic<int>> buffer_writes_pending_;
		std::unordered_map<int, std::mutex> buffer_mutexes_;
		std::mutex sequence_id_mutex_;

		int incomplete_event_report_interval_ms_;
		std::chrono::steady_clock::time_point last_incomplete_event_report_time_;
		int broadcast_timeout_ms_;
		int broadcast_count_;
		int subrun_event_count_;

		std::set<pid_t> art_processes_;
		std::atomic<bool> restart_art_;
		fhicl::ParameterSet current_art_pset_;
		std::shared_ptr<art_config_file> current_art_config_file_;

		RequestSender requests_;

		FragmentPtr init_fragment_;
		FragmentPtr dropped_data_; ///< Used for when data comes in badly out-of-sequence

		bool broadcastFragment_(FragmentPtr frag, FragmentPtr& outFrag);

		detail::RawEventHeader* getEventHeader_(int buffer);

		int getBufferForSequenceID_(Fragment::sequence_id_t seqID, bool create_new, Fragment::timestamp_t timestamp = Fragment::InvalidTimestamp);
		bool hasFragments_(int buffer);
		void complete_buffer_(int buffer);
		bool bufferComparator(int bufA, int bufB);
		void check_pending_buffers_();

		void send_init_frag_();
		SharedMemoryManager broadcasts_;
	};
}

#endif //ARTDAQ_DAQRATE_SHAREDMEMORYEVENTMANAGER_HH