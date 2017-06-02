#ifndef artdaq_Application_MPI2_BoardReaderCore_hh
#define artdaq_Application_MPI2_BoardReaderCore_hh

#include <string>

#include "artdaq/Application/CommandableFragmentGenerator.hh"
#include "artdaq/Application/Commandable.hh"
#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/Application/StatisticsHelper.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"

namespace artdaq
{
	class BoardReaderCore;
}

/**
 * \brief BoardReaderCore implements the state machine for the BoardReader artdaq application.
 * It contains a CommandableFragmentGenerator, which generates Fragments which are then sent to a DataSenderManager by BoardReaderCore.
 */
class artdaq::BoardReaderCore
{
public:
	static const std::string FRAGMENTS_PROCESSED_STAT_KEY; ///< Key for the Fragments Processed MonitoredQuantity
	static const std::string INPUT_WAIT_STAT_KEY; ///< Key for the Input Wait MonitoredQuantity
	static const std::string BRSYNC_WAIT_STAT_KEY; ///< Key for the Sync Wait MonitoredQuantity
	static const std::string OUTPUT_WAIT_STAT_KEY; ///< Key for the Output Wait MonitoredQuantity
	static const std::string FRAGMENTS_PER_READ_STAT_KEY; ///< Key for the Fragments Per Read MonitoredQuantity

	/**
	 * \brief BoardReaderCore Constructor
	 * \param parent_application Reference to parent Commandable object, for in_run_failure notification
	 * \param rank Rank of the BoardReader
	 * \param name Friendly name for the BoardReader
	 */
	BoardReaderCore(Commandable& parent_application, int rank,
					std::string name);

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

	/**
	 * \brief Initialize the BoardReaderCore
	 * \param pset ParameterSet used to configure the BoardReaderCore
	 * \return True if the initialize attempt succeeded
	 * 
	 * \verbatim
	 * BoardReaderCore accepts the following Parameters:
	 * "daq" (REQUIRED): FHiCL table containing DAQ configuration.
	 *   "fragment_receiver" (REQUIRED): FHiCL table containing Fragment Receiver configruation.
	 *     See CommandableFragmentGenerator for configuration options.
	 *     "generator" (Default: ""): The plugin name of the generator to load
	 *     "rt_priority" (Default: 0): The unix priority to attempt to assign to the process
	 *   "metrics": FHiCL table containing MetricManager configuration.
	 *     See MetricManager for configuration options.
	 * \endverbatim
	 *   
	 */
	bool initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t);

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
	* \return True unless exception occurred
	*/
	bool shutdown(uint64_t);

	/**
	* \brief Soft-Initialize the BoardReader. No-Op
	 * \param pset ParameterSet used to configure the BoardReaderCore
	* \return True unless exception occurred
	*/
	bool soft_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t);

	/**
	* \brief Reinitialize the BoardReader. No-Op
	 * \param pset ParameterSet used to configure the BoardReaderCore
	* \return True unless exception occurred
	*/
	bool reinitialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t);

	/**
	 * \brief Main working loop of the BoardReaderCore
	 * \return Number of Fragments generated
	 * 
	 * This loop calls the CommandableFragmentGenerator::getNext method, then sends each Fragment using DataSenderManager.
	 */
	size_t process_fragments();

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
	 * \brief Gets a handle to the DataSenderManager
	 * \return Pointer to the DataSenderManager
	 */
	static DataSenderManager* GetDataSenderManagerPtr() { return sender_ptr_.get(); }
private:
	Commandable& parent_application_;
	//MPI_Comm local_group_comm_;
	std::unique_ptr<CommandableFragmentGenerator> generator_ptr_;
	art::RunID run_id_;
	std::string name_;

	fhicl::ParameterSet data_pset_;
	int rt_priority_;
	bool skip_seqId_test_;

	/* ELF 5/10/2017 Removing in favor of DataReceiverManager source suppression logic
	int mpi_sync_fragment_interval_;
	double mpi_sync_wait_threshold_fraction_;
	int mpi_sync_wait_threshold_count_;
	size_t mpi_sync_wait_interval_usec_;
	int mpi_sync_wait_log_level_;
	int mpi_sync_wait_log_interval_sec_;
	*/
	static std::unique_ptr<artdaq::DataSenderManager> sender_ptr_;

	size_t fragment_count_;
	artdaq::Fragment::sequence_id_t prev_seq_id_;
	std::atomic<bool> stop_requested_;
	std::atomic<bool> pause_requested_;

	// attributes and methods for statistics gathering & reporting
	artdaq::StatisticsHelper statsHelper_;

	std::string buildStatisticsString_();

	artdaq::MetricManager metricMan_;

	void sendMetrics_();
};

#endif /* artdaq_Application_MPI2_BoardReaderCore_hh */