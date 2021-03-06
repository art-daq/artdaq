#ifndef ARTDAQ_ARTDAQ_APPLICATION_BOARDREADERCORE_HH_
#define ARTDAQ_ARTDAQ_APPLICATION_BOARDREADERCORE_HH_

#include "artdaq/Application/Commandable.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQrate/StatisticsHelper.hh"
#include "artdaq/DAQrate/detail/RequestReceiver.hh"
#include "artdaq/Generators/CommandableFragmentGenerator.hh"

#include "canvas/Persistency/Provenance/RunID.h"
#include "fhiclcpp/ParameterSet.h"

#include <atomic>
#include <string>

namespace artdaq {
class BoardReaderCore;
}

/**
 * \brief BoardReaderCore implements the state machine for the BoardReader artdaq application.
 * It contains a CommandableFragmentGenerator, which generates Fragments which are then sent to a DataSenderManager by BoardReaderCore.
 */
class artdaq::BoardReaderCore
{
public:
	static const std::string FRAGMENTS_PROCESSED_STAT_KEY;  ///< Key for the Fragments Processed MonitoredQuantity
	static const std::string INPUT_WAIT_STAT_KEY;           ///< Key for the Input Wait MonitoredQuantity
	static const std::string BUFFER_WAIT_STAT_KEY;          ///< Key for the Fragment Buffer Wait MonitoredQuantity
	static const std::string REQUEST_WAIT_STAT_KEY;         ///< Key for the Request Buffer Wait MonitoredQuantity
	static const std::string BRSYNC_WAIT_STAT_KEY;          ///< Key for the Sync Wait MonitoredQuantity
	static const std::string OUTPUT_WAIT_STAT_KEY;          ///< Key for the Output Wait MonitoredQuantity
	static const std::string FRAGMENTS_PER_READ_STAT_KEY;   ///< Key for the Fragments Per Read MonitoredQuantity

	/**
	 * \brief BoardReaderCore Constructor
	 * \param parent_application Reference to parent Commandable object, for in_run_failure notification
	 */
	BoardReaderCore(Commandable& parent_application);

	/**
	 * \brief Copy Constructor is Deleted
	 */
	BoardReaderCore(BoardReaderCore const&) = delete;

	/**
	 * \brief BoardReaderCore Destructor
	 */
	virtual ~BoardReaderCore();

	/**
	 * \brief Copy Assignment Operator is deleted
	 * \return BoardReaderCore copy
	 */
	BoardReaderCore& operator=(BoardReaderCore const&) = delete;
	BoardReaderCore(BoardReaderCore&&) = delete;             ///< Move Constructor is deleted
	BoardReaderCore& operator=(BoardReaderCore&&) = delete;  ///< Move Assignment Operator is deleted

	/**
	 * \brief Initialize the BoardReaderCore
	 * \param pset ParameterSet used to configure the BoardReaderCore
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True if the initialize attempt succeeded
	 *
	 * \verbatim
	 * BoardReaderCore accepts the following Parameters:
	 * "daq" (REQUIRED): FHiCL table containing DAQ configuration.
	 *   "fragment_receiver" (REQUIRED): FHiCL table containing Fragment Receiver configruation.
	 *     See CommandableFragmentGenerator for configuration options.
	 *     "generator" (Default: ""): The plugin name of the generator to load
	 *     "rt_priority" (Default: 0): The unix priority to attempt to assign to the process
	 *     "verbose" (Default: true): Whether to print transition messages
	 *   "metrics": FHiCL table containing MetricManager configuration.
	 *     See MetricManager for configuration options.
	 * \endverbatim
	 *
	 */
	bool initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Start the BoardReader, and the CommandableFragmentGenerator
	 * \param id Run ID of new run
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True unless exception occurred
	 */
	bool start(art::RunID id, uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Stop the BoardReader, and the CommandableFragmentGenerator
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True unless exception occurred
	 */
	bool stop(uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Pause the BoardReader, and the CommandableFragmentGenerator
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True unless exception occurred
	 */
	bool pause(uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Resume the BoardReader, and the CommandableFragmentGenerator
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True unless exception occurred
	 */
	bool resume(uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Shutdown the BoardReader, and the CommandableFragmentGenerator
	 * \param timeout Timeout for transition
	 * \return True unless exception occurred
	 */
	bool shutdown(uint64_t timeout);

	/**
	 * \brief Soft-Initialize the BoardReader. No-Op
	 * \param pset ParameterSet used to configure the BoardReaderCore
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True unless exception occurred
	 */
	bool soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Reinitialize the BoardReader. No-Op
	 * \param pset ParameterSet used to configure the BoardReaderCore
	 * \param timeout Timeout for transition
	 * \param timestamp Timestamp of transition
	 * \return True unless exception occurred
	 */
	bool reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp);

	/**
	 * \brief Main working loop of the BoardReaderCore
	 *
	 * This loop calls the CommandableFragmentGenerator::getNext method and gives the result to the FragmentBuffer
	 */
	void receive_fragments();

	/**
	 * @brief Main working loop of the BoardReaderCore, pt. 2
	 *
	 * This loop calls the FragmentBuffer::applyRequests method and sends the result using DataSenderManager
	 */
	void send_fragments();

	/**
	 * \brief Send a report on a given run-time quantity
	 * \param which Which quantity to report
	 * \return A string containing the requested quantity.
	 *
	 * If the CommandableFragmentGenerator has been initialized, CommandableFragmentGenerator::report(std::string const& which) will be called.
	 * Otherwise, the BoardReaderCore will return the current run number and an error message.
	 */
	std::string report(std::string const& which) const;

	/**
	 * \brief Run a user-defined command on the CommandableFragmentGenerator
	 * \param command Command name to run
	 * \param arg Argument(s) for command
	 * \return Whether command completed successfully. (By convention, unsupported commands should return true)
	 */
	bool metaCommand(std::string const& command, std::string const& arg);

	/**
	 * \brief Gets a handle to the DataSenderManager
	 * \return Pointer to the DataSenderManager
	 */
	static DataSenderManager* GetDataSenderManagerPtr() { return sender_ptr_.get(); }

	/// <summary>
	/// Get the number of Fragments processed this run
	/// </summary>
	/// <returns>The number of Fragments processed this run</returns>
	size_t GetFragmentsProcessed() { return fragment_count_; }

	CommandableFragmentGenerator const* GetGeneratorPointer()
	{
		if (generator_ptr_)
			return generator_ptr_.get();
		else
			return nullptr;
	}

	/**
	 * @brief Get whether the sender thread is still running
	 * @return Whether the sender thread (applying requests to FragmentBuffer and sending to DataSenderManager) is still running
	 */
	bool GetSenderThreadActive() { return sender_thread_active_.load(); }
	/**
	 * @brief Get whether the receiver thread is still running
	 * @return Whether the receiver thread (calling CFG::getNext and putting Fragments into the FragmentBuffer) is still running
	 */
	bool GetReceiverThreadActive() { return receiver_thread_active_.load(); }
	/**
	 * @brief Set the timeout for starting the sender and receiver threads
	 * @param timeout Timeout, in seconds, for starting the sender and receiver threads
	 *
	 * This function is used to communicate the start transition timeout from BoardReaderApp to the BoardReaderCore threads
	 */
	void SetStartTransitionTimeout(double timeout) { start_transition_timeout_ = timeout; }

private:
	Commandable& parent_application_;
	std::unique_ptr<CommandableFragmentGenerator> generator_ptr_;
	std::unique_ptr<RequestReceiver> request_receiver_ptr_;
	std::shared_ptr<FragmentBuffer> fragment_buffer_ptr_;
	art::RunID run_id_;

	fhicl::ParameterSet data_pset_;
	int rt_priority_;
	bool skip_seqId_test_;

	static std::unique_ptr<artdaq::DataSenderManager> sender_ptr_;

	std::atomic<size_t> fragment_count_;
	artdaq::Fragment::sequence_id_t prev_seq_id_;
	std::atomic<bool> stop_requested_;
	std::atomic<bool> pause_requested_;

	// State orchestration
	std::atomic<bool> running_{false};
	std::atomic<bool> sender_thread_active_{false};
	std::atomic<bool> receiver_thread_active_{false};
	double start_transition_timeout_{10.0};

	// attributes and methods for statistics gathering & reporting
	artdaq::StatisticsHelper statsHelper_;

	std::string buildStatisticsString_();

	void sendMetrics_();

	bool verbose_;  ///< Whether to log transition messages
};

#endif  // ARTDAQ_ARTDAQ_APPLICATION_BOARDREADERCORE_HH_
