#ifndef artdaq_Application_MPI2_DataReceiverCore_hh
#define artdaq_Application_MPI2_DataReceiverCore_hh

#include "canvas/Persistency/Provenance/RunID.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq/DAQrate/DataReceiverManager.hh"
#include "artdaq/DAQrate/SharedMemoryEventManager.hh"

#include <atomic>
#include <map>
#include <memory>
#include <string>

namespace artdaq {
class DataReceiverCore;
}

/**
 * \brief DataReceiverCore implements the state machine for the DataReceiver artdaq application.
 * DataReceiverCore receives Fragment objects from the DataReceiverManager, and sends them to the EventStore.
 */
class artdaq::DataReceiverCore
{
public:
	/**
	 * \brief DataReceiverCore Constructor.
	 */
	DataReceiverCore();

	/**
	 * \brief Copy Constructor is deleted
	 */
	DataReceiverCore(DataReceiverCore const&) = delete;

	/**
	 * Destructor.
	 */
	virtual ~DataReceiverCore();

	/**
	 * \brief Copy Assignment operator is deleted
	 * \return AggregatorCore copy
	 */
	DataReceiverCore& operator=(DataReceiverCore const&) = delete;
	DataReceiverCore(DataReceiverCore&&) = delete;             ///< Move Constructor is deleted
	DataReceiverCore& operator=(DataReceiverCore&&) = delete;  ///< Move Assignment Operator is deleted

	/**
	 * \brief Processes the initialize request.
	 * \param pset ParameterSet used to configure the DataReceiverCore
	 * \return Whether the initialize attempt succeeded
	 *
	 * \verbatim
	 * DataReceiverCore accepts the following Parameters:
	 * "daq" (REQUIRED): FHiCL table containing DAQ configuration
	 *   "event_builder" (REQUIRED): FHiCL table containing Aggregator paramters
	 *     "fragment_count" (REQUIRED): Number of Fragment objects to collect before sending them to art
	 *     "inrun_recv_timeout_usec" (Default: 100000): Amount of time to wait for new Fragment objects while running
	 *     "endrun_recv_timeout_usec" (Default: 20000000): Amount of time to wait for additional Fragment objects at EndOfRun
	 *     "pause_recv_timeout_usec" (Default: 3000000): Amount of time to wait for additional Fragment objects at PauseRun
	 *     "verbose" (Default: true): Whether to print transition messages
	 *   "metrics": FHiCL table containing configuration for MetricManager
	 * \endverbatim
	 *
	 *  Note that the "event_builder" ParameterSet is also used to configure the SharedMemoryEventManager. See that class' documentation for more information.
	 */
	virtual bool initialize(fhicl::ParameterSet const& pset) = 0;

	/**
	 * \brief Start the DataReceiverCore
	 * \param id Run ID of the current run
	 * \return True if no exception
	 */
	bool start(art::RunID id);

	/**
	 * \brief Stops the DataReceiverCore
	 * \return True if no exception
	 */
	bool stop();

	/**
	 * \brief Pauses the DataReceiverCore
	 * \return True if no exception
	 */
	bool pause();

	/**
	 * \brief Resumes the DataReceiverCore
	 * \return True if no exception
	 */
	bool resume();

	/**
	 * \brief Shuts Down the DataReceiverCore
	 * \return If the shutdown was successful
	 */
	bool shutdown();

	/**
	 * \brief Soft-Initializes the DataReceiverCore. No-Op
	 * \param pset ParameterSet for configuring DataReceiverCore
	 * \return Always returns true
	 */
	bool soft_initialize(fhicl::ParameterSet const& pset);

	/**
	 * \brief Reinitializes the DataReceiverCore.
	 * \param pset ParameterSet for configuring DataReceiverCore
	 * \return True if no exception
	 */
	bool reinitialize(fhicl::ParameterSet const& pset);

	/**
	 * \brief Rollover the subrun after the given event
	 * \param boundary Sequence ID of boundary
	 * \param subrun Subrun number of new subrun
	 * \return True event_store_ptr is valid
	 */
	bool rollover_subrun(uint64_t boundary, uint32_t subrun);

	/**
	 * \brief Send a report on a given run-time quantity
	 * \param which Which quantity to report
	 * \return A string containing the requested quantity.
	 *
	 * report accepts the following values of "which":
	 * "event_count": The number of events received, or -1 if not initialized
	 * "incomplete_event_count": The number of incomplete event bunches in the EventStore, or -1 if not initalized
	 *
	 * Anything else will return the run number and an error message.
	 */
	std::string report(std::string const& which) const;

	/**
	 * \brief Add the specified key and value to the configuration archive list.
	 * \param key String key to be used
	 * \param value String value to be stored
	 * \return This function will always return true
	 */
	bool add_config_archive_entry(std::string const& key, std::string const& value)
	{
		config_archive_entries_[key] = value;
		return true;
	}

	/**
	 * \brief Clear the configuration archive list.
	 * \return True if archive is empty after clear operation
	 */
	bool clear_config_archive()
	{
		config_archive_entries_.clear();
		return config_archive_entries_.empty();
	}

	/**
	 * \brief Override the expected Fragment IDs for a specific event
	 * \param seqID Event Sequence Number
	 * \param frags std::set of Fragment_id_t indicating Fragment IDs which should be present for event
	 */
	void OverrideFragmentIDsForEvent(Fragment::sequence_id_t seqID, std::set<Fragment::fragment_id_t> frags)
	{
		if (event_store_ptr_) event_store_ptr_->OverrideFragmentIDsForEvent(seqID, frags);
	}

	/**
	 * \brief Update the list of Fragment IDs which should be present unless overridden
	 * \param frags std::set of Fragment_id_t indicating Fragment IDs which should be present for all events
	 * \param when Sequence ID when the change should take effect. Default of 0 indicates change should take effect immediately.
	 */
	void SetDefaultFragmentIDs(std::set<Fragment::fragment_id_t> frags, Fragment::sequence_id_t when = 0)
	{
		if (event_store_ptr_) event_store_ptr_->SetDefaultFragmentIDs(frags, when);
	}

protected:
	/**
	 * \brief Initialize the DataReceiverCore (should be called from initialize() overrides
	 * \param pset ParameterSet for art configuration
	 * \param data_pset ParameterSet for DataReceiverManager and SharedMemoryEventManager configuration
	 * \param metric_pset ParameterSet for MetricManager
	 * \return Whether the initialize succeeded
	 */
	bool initializeDataReceiver(fhicl::ParameterSet const& pset, fhicl::ParameterSet const& data_pset, fhicl::ParameterSet const& metric_pset);

	std::unique_ptr<DataReceiverManager> receiver_ptr_;          ///< Pointer to the DataReceiverManager
	std::shared_ptr<SharedMemoryEventManager> event_store_ptr_;  ///< Pointer to the SharedMemoryEventManager
	std::atomic<bool> stop_requested_;                           ///< Stop has been requested?
	std::atomic<bool> pause_requested_;                          ///< Pause has been requested?
	std::atomic<bool> run_is_paused_;                            ///< Pause has been successfully completed?
	bool verbose_;                                               ///< Whether to log transition messages

	fhicl::ParameterSet art_pset_;                               ///< ParameterSet sent to art process
	std::map<std::string, std::string> config_archive_entries_;  ///< Additional strings to archive as part of the art configuration
};

#endif /* artdaq_Application_MPI2_DataReceiverCore_hh */
