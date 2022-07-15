#ifndef artdaq_Application_FragmentBuffer_hh
#define artdaq_Application_FragmentBuffer_hh

#include "fhiclcpp/types/Sequence.h"  // Must pre-empt fhiclcpp/types/Atom.h

#include "TRACE/tracemf.h"  // Pre-empt TRACE/trace.h from Fragment.hh.
#include "artdaq-core/Data/Fragment.hh"

#include "artdaq/DAQrate/RequestBuffer.hh"

namespace fhicl {
class ParameterSet;
}
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Comment.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/Name.h"

// Socket Includes
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <queue>

namespace artdaq {
/**
 * \brief The RequestMode enumeration contains the possible ways which FragmentBuffer responds to data requests.
 */
enum class RequestMode
{
	Single,
	Buffer,
	Window,
	SequenceID,
	Ignored
};

/**
 * \brief FragmentBuffer is a FragmentGenerator-derived
 * abstract class that defines the interface for a FragmentGenerator
 * designed as a state machine with start, stop, etc., transition
 * commands.
 *
 * Users of classes derived from
 * FragmentBuffer will call these transitions via the
 * publically defined StartCmd(), StopCmd(), etc.; these public
 * functions contain functionality considered properly universal to
 * all FragmentBuffer-derived classes, including calls
 * to private virtual functions meant to be overridden in derived
 * classes. The same applies to this class's implementation of the
 * FragmentGenerator::getNext() pure virtual function, which is
 * declared final (i.e., non-overridable in derived classes) and which
 * itself calls a pure virtual getNext_() function to be implemented
 * in derived classes.
 *
 * State-machine related interface functions will be called only from a
 * single thread. getNext() will be called only from a single
 * thread. The thread from which state-machine interfaces functions are
 * called may be a different thread from the one that calls getNext().
 *
 * John F., 3/24/14
 *
 * After some discussion with Kurt, FragmentBuffer has
 * been updated such that it now contains a member vector
 * fragment_ids_ ; if "fragment_id" is set in the FHiCL document
 * controlling a class derived from FragmentBuffer,
 * fragment_ids_ will be booked as a length-1 vector, and the value in
 * this vector will be returned by fragment_id(). fragment_id() will
 * throw an exception if the length of the vector isn't 1. If
 * "fragment_ids" is set in the FHiCL document, then fragment_ids_ is
 * filled with the values in the list which "fragment_ids" refers to,
 * otherwise it is set to the empty vector (this is what should happen
 * if the user sets the "fragment_id" variable in the FHiCL document,
 * otherwise exceptions will end up thrown due to the logical
 * conflict). If neither "fragment_id" nor "fragment_ids" is set in
 * the FHiCL document, writers of classes derived from this one will
 * be expected to override the virtual fragmentIDs() function with
 * their own code (the CompositeDriver class is an example of this)
 */
class FragmentBuffer
{
public:
	/// <summary>
	/// Configuration of the FragmentBuffer. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "generator" (REQUIRED) Name of the FragmentBuffer plugin to load
		fhicl::Atom<std::string> generator_type{fhicl::Name{"generator"}, fhicl::Comment{"Name of the FragmentBuffer plugin to load"}};
		/// "request_window_offset" (Default: 0) : Request messages contain a timestamp. For Window request mode, start the window this far before the timestamp in the request
		fhicl::Atom<Fragment::timestamp_t> request_window_offset{fhicl::Name{"request_window_offset"}, fhicl::Comment{"Request messages contain a timestamp. For Window request mode, start the window this far before the timestamp in the request"}, 0};
		/// "request_window_width" (Default: 0) : For Window request mode, the window will be timestamp - offset to timestamp - offset + width
		fhicl::Atom<Fragment::timestamp_t> request_window_width{fhicl::Name{"request_window_width"}, fhicl::Comment{"For Window request mode, the window will be timestamp - offset to timestamp - offset + width"}, 0};
		/// "stale_fragment_timeout" (Default: 0) : Fragments stored in the fragment generator which are older than the newest stored fragment by at least stale_fragment_timeout units of request timestamp ticks will get discarded (0 to disable)
		fhicl::Atom<Fragment::timestamp_t> stale_fragment_timeout{fhicl::Name{"stale_fragment_timeout"}, fhicl::Comment{"Fragments stored in the fragment generator which are older than the newest stored fragment by at least stale_fragment_timeout units of request timestamp ticks will get discarded"}, 0};
		/// "buffer_mode_keep_latest" (Default: false): Keep the latest Fragment when running in Buffer mode, so that each response has at least one Fragment (Fragment will be discarded if new data arrives before next request)
		fhicl::Atom<bool> buffer_mode_keep_latest{fhicl::Name{"buffer_mode_keep_latest"}, fhicl::Comment{"Keep the latest Fragment when running in Buffer mode, so that each response has at least one Fragment (Fragment will be discarded if new data arrives before next request)"}, false};
		/// "expected_fragment_type" (Default: 231, EmptyFragmentType) : The type of Fragments this CFG will be generating. "Empty" will auto - detect type based on Fragments generated.
		fhicl::Atom<Fragment::type_t> expected_fragment_type{fhicl::Name{"expected_fragment_type"}, fhicl::Comment{"The type of Fragments this CFG will be generating. \"Empty\" will auto-detect type based on Fragments generated."}, Fragment::type_t(Fragment::EmptyFragmentType)};
		/// "request_windows_are_unique" (Default: true) : Whether Fragments should be removed from the buffer when matched to a request window
		fhicl::Atom<bool> request_windows_are_unique{fhicl::Name{"request_windows_are_unique"}, fhicl::Comment{"Whether Fragments should be removed from the buffer when matched to a request window"}, true};
		/// "missing_request_window_timeout_us" (Default: 5000000) : How long to track missing requests in the "out-of-order Windows" list
		fhicl::Atom<size_t> missing_request_window_timeout_us{fhicl::Name{"missing_request_window_timeout_us"}, fhicl::Comment{"How long to track missing requests in the \"out - of - order Windows\" list"}, 5000000};
		/// "window_close_timeout_us" (Default: 2000000) : How long to wait for the end of the data buffer to pass the end of a request window(measured from the time the request was received)
		fhicl::Atom<size_t> window_close_timeout_us{fhicl::Name{"window_close_timeout_us"}, fhicl::Comment{"How long to wait for the end of the data buffer to pass the end of a request window (measured from the time the request was received)"}, 2000000};
		/// "separate_data_thread" (Default: false) : Whether data collection should proceed on its own thread.Required for all data request processing
		fhicl::Atom<bool> separate_data_thread{fhicl::Name{"separate_data_thread"}, fhicl::Comment{"Whether data collection should proceed on its own thread. Required for all data request processing"}, false};
		/// "circular_buffer_mode" (Default: false) : Whether the data buffer should be treated as a circular buffer on the input side (i.e. old fragments are automatically discarded when the buffer is full to always call getNext_).
		fhicl::Atom<bool> circular_buffer_mode{fhicl::Name{"circular_buffer_mode"}, fhicl::Comment{"Whether the data buffer should be treated as a circular buffer on the input side (i.e. old fragments are automatically discarded when the buffer is full to always call getNext_)."}, false};
		/// "sleep_on_no_data_us" (Default: 0 (no sleep)) : How long to sleep after calling getNext_ if no data is returned
		fhicl::Atom<size_t> sleep_on_no_data_us{fhicl::Name{"sleep_on_no_data_us"}, fhicl::Comment{"How long to sleep after calling getNext_ if no data is returned"}, 0};
		/// "data_buffer_depth_fragments" (Default: 1000) : The max fragments which can be stored before dropping occurs
		fhicl::Atom<int> data_buffer_depth_fragments{fhicl::Name{"data_buffer_depth_fragments"}, fhicl::Comment{"The max fragments which can be stored before dropping occurs"}, 1000};
		/// "data_buffer_depth_mb" (Default: 1000) : The max cumulative size in megabytes of the fragments which can be stored before dropping occurs
		fhicl::Atom<size_t> data_buffer_depth_mb{fhicl::Name{"data_buffer_depth_mb"}, fhicl::Comment{"The max cumulative size in megabytes of the fragments which can be stored before dropping occurs"}, 1000};
		/// "separate_monitoring_thread" (Default: false) : Whether a thread that calls the checkHWStatus_ method should be created
		fhicl::Atom<bool> separate_monitoring_thread{fhicl::Name{"separate_monitoring_thread"}, fhicl::Comment{"Whether a thread that calls the checkHWStatus_ method should be created"}, false};
		/// "hardware_poll_interval_us" (Default: 0) : If a separate monitoring thread is used, how often should it call checkHWStatus_
		fhicl::Atom<int64_t> hardware_poll_interval_us{fhicl::Name{"hardware_poll_interval_us"}, fhicl::Comment{"If a separate monitoring thread is used, how often should it call checkHWStatus_"}, 0};
		/// "board_id" (REQUIRED) : The identification number for this FragmentBuffer
		fhicl::Atom<int> board_id{fhicl::Name{"board_id"}, fhicl::Comment{"The identification number for this FragmentBuffer"}};
		/// "fragment_ids" (Default: empty vector) : A list of Fragment IDs created by this FragmentBuffer
		/// Note that only one of fragment_ids and fragment_id should be specified in the configuration
		fhicl::Sequence<Fragment::fragment_id_t> fragment_ids{fhicl::Name("fragment_ids"), fhicl::Comment("A list of Fragment IDs created by this FragmentBuffer")};
		/// "fragment_id" (Default: -99) : The Fragment ID created by this FragmentBuffer
		/// Note that only one of fragment_ids and fragment_id should be specified in the configuration
		fhicl::Atom<int> fragment_id{fhicl::Name{"fragment_id"}, fhicl::Comment{"The Fragment ID created by this FragmentBuffer"}, -99};
		/// "sleep_on_stop_us" (Default: 0) : How long to sleep before returning when stop transition is called
		fhicl::Atom<int> sleep_on_stop_us{fhicl::Name{"sleep_on_stop_us"}, fhicl::Comment{"How long to sleep before returning when stop transition is called"}, 0};
		/// <summary>
		/// "request_mode" (Deafult: Ignored) : The mode by which the FragmentBuffer will process reqeusts
		/// Ignored : Request messages are ignored.This is a "push" FragmentBuffer
		/// Single : The FragmentBuffer responds to each request with the latest Fragment it has received
		/// Buffer : The FragmentBuffer responds to each request with all Fragments it has received since the last request
		/// Window : The FragmentBuffer searches its data buffer for all Fragments whose timestamp falls within the request window
		/// SequenceID:  The FragmentBuffer responds to each request with all Fragments that match the sequence ID in the request
		/// </summary>
		fhicl::Atom<std::string> request_mode{fhicl::Name{"request_mode"}, fhicl::Comment{"The mode by which the FragmentBuffer will process reqeusts"}, "ignored"};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
	 * \brief FragmentBuffer Constructor
	 * \param ps ParameterSet used to configure FragmentBuffer. See artdaq::FragmentBuffer::Config.
	 */
	explicit FragmentBuffer(const fhicl::ParameterSet& ps);

	/**
	 * \brief FragmentBuffer Destructor
	 *
	 * Joins all threads before returning
	 */
	virtual ~FragmentBuffer();

	/**
	 * @brief Add Fragments to the FragmentBuffer
	 * @param frags Fragments to add
	 */
	void AddFragmentsToBuffer(FragmentPtrs frags);

	/**
	 * @brief Inform the FragmentBuffer that it should stop
	 */
	void Stop() { should_stop_ = true; }

	/**
	 * @brief Reset the FragmentBuffer (flushes all Fragments from buffers)
	 * @param stop Whether the FragmentBuffer should be stopped during the Reset
	 */
	void Reset(bool stop);

	/// <summary>
	/// Create fragments using data buffer for request mode Ignored.
	/// Precondition: dataBufferMutex_ and request_mutex_ are locked
	/// </summary>
	/// <param name="frags">Ouput fragments</param>
	void applyRequestsIgnoredMode(artdaq::FragmentPtrs& frags);

	/// <summary>
	/// Create fragments using data buffer for request mode Single.
	/// Precondition: dataBufferMutex_ and request_mutex_ are locked
	/// </summary>
	/// <param name="frags">Ouput fragments</param>
	void applyRequestsSingleMode(artdaq::FragmentPtrs& frags);

	/// <summary>
	/// Create fragments using data buffer for request mode Buffer.
	/// Precondition: dataBufferMutex_ and request_mutex_ are locked
	/// </summary>
	/// <param name="frags">Ouput fragments</param>
	void applyRequestsBufferMode(artdaq::FragmentPtrs& frags);

	/// <summary>
	/// Create fragments using data buffer for request mode Window.
	/// Precondition: dataBufferMutex_ and request_mutex_ are locked
	/// </summary>
	/// <param name="frags">Ouput fragments</param>
	void applyRequestsWindowMode(artdaq::FragmentPtrs& frags);

	/// <summary>
	/// Create fragments using data buffer for request mode SequenceID.
	/// Precondition: dataBufferMutex_ and request_mutex_ are locked
	/// </summary>
	/// <param name="frags">Ouput fragments</param>
	void applyRequestsSequenceIDMode(artdaq::FragmentPtrs& frags);

	/// Copy data from the relevant data buffer that matches the given timestamp.
	/// </summary>
	/// <param name="frags">Output Fragments</param>
	/// <param name="id">Fragment ID of buffer to search</param>
	/// <param name="seq">Sequence ID of output Fragment</param>
	/// <param name="ts">Timestamp of output Fragment (used to determine window limits)</param>
	void applyRequestsWindowMode_CheckAndFillDataBuffer(artdaq::FragmentPtrs& frags, artdaq::Fragment::fragment_id_t id, artdaq::Fragment::sequence_id_t seq, artdaq::Fragment::timestamp_t ts);

	/**
	 * \brief See if any requests have been received, and add the corresponding data Fragment objects to the output list
	 * \param[out] frags list of FragmentPtr objects ready for transmission
	 * \return True if not stopped
	 */
	bool applyRequests(FragmentPtrs& frags);

	/**
	 * \brief Send an EmptyFragmentType Fragment
	 * \param[out] frags Output list to append EmptyFragmentType to
	 * \param sequenceId Sequence ID of Empty Fragment
	 * \param fragmentId Fragment ID of Empty Fragment
	 * \param desc Message to log with reasoning for sending Empty Fragment
	 * \return True if no exceptions
	 */
	bool sendEmptyFragment(FragmentPtrs& frags, size_t sequenceId, Fragment::fragment_id_t fragmentId, std::string desc);

	/**
	 * \brief This function is for Buffered and Single request modes, as they can only respond to one data request at a time
	 * If the request message seqID > ev_counter, simply send empties until they're equal
	 * \param[out] frags Output list to append EmptyFragmentType to
	 * \param requests List of requests to process
	 */
	void sendEmptyFragments(FragmentPtrs& frags, std::map<Fragment::sequence_id_t, Fragment::timestamp_t>& requests);

	/**
	 * \brief Check the windows_sent_ooo_ map for sequence IDs that may be removed
	 * \param seq Sequence ID of current window
	 */
	void checkSentWindows(Fragment::sequence_id_t seq);

	/**
	 * \brief Wait for the data buffer to drain (dataBufferIsTooLarge returns false), periodically reporting status.
	 * \param id Fragment ID of data buffer
	 * \return True if wait ended without something else disrupting the run
	 */
	bool waitForDataBufferReady(Fragment::fragment_id_t id);

	/**
	 * \brief Test the configured constraints on the data buffer
	 * \param id Fragment ID of data buffer
	 * \return Whether the data buffer is full
	 */
	bool dataBufferIsTooLarge(Fragment::fragment_id_t id);

	/**
	 * \brief Calculate the size of the dataBuffer and report appropriate metrics
	 * \param id Fragment ID of buffer
	 */
	void getDataBufferStats(Fragment::fragment_id_t id);

	/**
	 * \brief Calculate the size of all dataBuffers and report appropriate metrics
	 */
	void getDataBuffersStats()
	{
		for (auto& id : dataBuffers_) getDataBufferStats(id.first);
	}

	/**
	 * \brief Perform data buffer pruning operations for the given buffer. If the RequestMode is Single, removes all but the latest Fragment from the data buffer.
	 * \param id Fragment ID of buffer
	 * In Window and Buffer RequestModes, this function discards the oldest Fragment objects until the data buffer is below its size constraints,
	 * then also checks for stale Fragments, based on the timestamp of the most recent Fragment.
	 */
	void checkDataBuffer(Fragment::fragment_id_t id);

	/**
	 * \brief Perform data buffer pruning operations for all buffers.
	 */
	void checkDataBuffers()
	{
		for (auto& id : dataBuffers_) checkDataBuffer(id.first);
	}

	/**
	 * \brief Get the map of Window-mode requests fulfilled by this Fragment Geneerator for the given Fragment ID
	 * \param id Fragment ID of buffer
	 * \return Map of sequence_id and time_point for sent Window-mode requests
	 *
	 * This function is used in FragmentBuffer_t to verify correct functioning of Window mode
	 */
	std::map<Fragment::sequence_id_t, std::chrono::steady_clock::time_point> GetSentWindowList(Fragment::fragment_id_t id)
	{
		if (!dataBuffers_.count(id))
		{
			throw cet::exception("DataBufferError") << "Error in FragmentBuffer: Cannot get Sent Windows for ID " << id << " because it does not exist!";
		}

		return dataBuffers_[id]->WindowsSent;
	}

	/**
	 * \brief Get the list of Fragment IDs handled by this FragmentBuffer
	 * \return A std::vector<Fragment::fragment_id_t> containing the Fragment IDs handled by this FragmentBuffer
	 */
	std::vector<Fragment::fragment_id_t> fragmentIDs()
	{
		std::vector<Fragment::fragment_id_t> output;

		for (auto& id : dataBuffers_)
		{
			output.push_back(id.first);
		}

		return output;
	}

	/// <summary>
	/// Get the current request mode of the FragmentBuffer
	/// </summary>
	/// <returns>Current RequestMode of the CFG</returns>
	RequestMode request_mode() const { return mode_; }

	// The following functions are not yet implemented, and their
	// signatures may be subject to change.

	// John F., 12/6/13 -- do we want Reset and Shutdown commands?
	// Kurt B., 15-Feb-2014. For the moment, I suspect that we don't
	// want a Shutdown command. FragmentGenerator instances are
	// Constructed at Initialization time, and they are destructed
	// at Shutdown time. So, any shutdown operations that need to be
	// done should be put in the FragmentGenerator child class
	// destructors. If we find that want shutdown (or initialization)
	// operations that are different from destruction (construction),
	// then we'll have to add InitCmd and ShutdownCmd methods.

	//    virtual void ResetCmd() final {}
	//    virtual void ShutdownCmd() final {}

	/**
	 * @brief Set the pointer to the RequestBuffer used to retrieve requests
	 * @param buffer Pointer to the RequestBuffer
	 */
	void SetRequestBuffer(std::shared_ptr<RequestBuffer> buffer) { requestBuffer_ = buffer; }

	/**
	 * @brief Get the next sequence ID expected by this FragmentBuffer. This is used to track sent windows and missed requests
	 * @return The next sequence ID expected by this FragmentBuffer
	 */
	artdaq::Fragment::sequence_id_t GetNextSequenceID() const { return next_sequence_id_; }

protected:
	// John F., 12/6/13 -- need to figure out which of these getter
	// functions should be promoted to "public"

	// John F., 1/21/15 -- after more than a year, there hasn't been a
	// single complaint that a FragmentBuffer-derived
	// class hasn't allowed its users to access these quantities, so
	// they're probably fine as is

	/**
	 * \brief Get the Fragment ID of this Fragment generator
	 * \throws cet::exception("FragmentID") if there is more that one Fragment ID configured for this Fragment Generator
	 * \return Fragment ID for the Fragment Generator
	 */
	artdaq::Fragment::fragment_id_t fragment_id() const
	{
		if (dataBuffers_.size() > 1) throw cet::exception("FragmentID") << "fragment_id() was called, indicating that Fragment Generator was expecting one and only one Fragment ID, but " << dataBuffers_.size() << " were declared!";
		return (*dataBuffers_.begin()).first;
	}

	/**
	 * \brief Routine used by applyRequests to make sure that all outstanding requests have been fulfilled before returning
	 * \return The logical AND of should_stop, mode is not Ignored, and requests list size equal to 0
	 */
	bool check_stop();

	/**
	 * \brief Return the string representation of the current RequestMode
	 * \return The string representation of the current RequestMode
	 */
	std::string printMode_();

	/**
	 * \brief Get the total number of Fragments in all data buffers
	 * \return Number of Fragments in all data buffers
	 */
	size_t dataBufferFragmentCount_();

private:
	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended

	// Socket parameters
	Fragment::sequence_id_t next_sequence_id_;
	std::shared_ptr<RequestBuffer> requestBuffer_;

	RequestMode mode_;
	bool bufferModeKeepLatest_;
	Fragment::timestamp_t windowOffset_;
	Fragment::timestamp_t windowWidth_;
	Fragment::timestamp_t staleTimeout_;
	Fragment::type_t expectedType_;
	bool uniqueWindows_;
	bool sendMissingFragments_;
	size_t missing_request_window_timeout_us_;
	size_t window_close_timeout_us_;
	bool error_on_empty_;

	bool circularDataBufferMode_;

	std::mutex dataConditionMutex_;
	std::condition_variable dataCondition_;
	int maxDataBufferDepthFragments_;
	size_t maxDataBufferDepthBytes_;

	struct DataBuffer
	{
		std::atomic<int> DataBufferDepthFragments;
		std::atomic<size_t> DataBufferDepthBytes;
		std::map<Fragment::sequence_id_t, std::chrono::steady_clock::time_point> WindowsSent;
		bool BufferFragmentKept;
		Fragment::sequence_id_t HighestRequestSeen;
		FragmentPtrs DataBuffer;
		std::mutex DataBufferMutex;
	};

	std::mutex systemFragmentMutex_;
	FragmentPtrs systemFragments_;
	std::atomic<size_t> systemFragmentCount_;

	std::unordered_map<artdaq::Fragment::fragment_id_t, std::shared_ptr<DataBuffer>> dataBuffers_;

	std::atomic<bool> should_stop_;
};
}  // namespace artdaq

#endif /* artdaq_Application_FragmentBuffer_hh */
