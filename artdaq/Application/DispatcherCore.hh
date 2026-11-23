#ifndef artdaq_Application_MPI2_DispatcherCore_hh
#define artdaq_Application_MPI2_DispatcherCore_hh

#include "artdaq/Application/DataReceiverCore.hh"

#include "fhiclcpp/ParameterSet.h"

#include <mutex>
#include <string>
#include <unordered_map>

namespace artdaq {
class DispatcherCore;
}

/**
 * \brief DispatcherCore implements the state machine for the Dispatcher artdaq application.
 * DispatcherCore processes incoming events in one of three roles: Data Logger, Online Monitor, or Dispatcher.
 */
class artdaq::DispatcherCore : public DataReceiverCore
{
public:
	/**
	 * \brief DispatcherCore Constructor.
	 */
	DispatcherCore() = default;

	/**
	 * \brief Copy Constructor is deleted
	 */
	DispatcherCore(DispatcherCore const&) = delete;

	/**
	 * Destructor.
	 */
	~DispatcherCore()
	{
		TLOG(TLVL_DEBUG + 32) << "Destructor";
	}

	/**
	 * \brief Copy Assignment operator is deleted
	 * \return DispatcherCore copy
	 */
	DispatcherCore& operator=(DispatcherCore const&) = delete;
	DispatcherCore(DispatcherCore&&) = delete;             ///< Move Constructor is deleted
	DispatcherCore& operator=(DispatcherCore&&) = delete;  ///< Move Assignment Operator is deleted

	/**
	 * \brief Processes the initialize request.
	 * \param pset ParameterSet used to configure the DispatcherCore
	 * \return Whether the initialize attempt succeeded
	 *
	 *  Configuration Parameters unique to the Dispatcher:
	 *  "allow_label_overwrites" (default: true): Allow a new process to start with the same unique_label as an old one, stopping the appropriate art process and restarting with the new configuration.
	 *  Note that the "Dispatcher" ParameterSet is also used to configure the EventStore. See that class' documentation for more information.
	 */
	bool initialize(fhicl::ParameterSet const& pset) override;

	/**
	 * \brief Create a new TransferInterface instance using the given configuration
	 * \param pset ParameterSet used to configure the TransferInterface
	 * \return String detailing any errors encountered or "Success"
	 *
	 * See TransferInterface for details on the expected configuration
	 */
	std::string register_monitor(fhicl::ParameterSet const& pset);

	/**
	 * \brief Delete the TransferInterface having the given unique label
	 * \param label Label of the TransferInterface to delete
	 * \return String detailing any errors encountered or "Success"
	 */
	std::string unregister_monitor(std::string const& label);

private:
	fhicl::ParameterSet generate_filter_fhicl_();
	fhicl::ParameterSet merge_parameter_sets_(fhicl::ParameterSet const& skel, const std::string& label, const fhicl::ParameterSet& pset);
	void check_filters_();

	void start_art_process_(std::string const& label);
	void stop_art_process_(std::string const& label);
	void restart_art_process_(std::string const& label);

	std::mutex dispatcher_transfers_mutex_;
	std::unordered_map<std::string, fhicl::ParameterSet> registered_monitors_;
	std::unordered_map<std::string, pid_t> registered_monitor_pids_;
	fhicl::ParameterSet pset_;  // The ParameterSet initially passed to the Dispatcher (contains input info)
	bool broadcast_mode_;
	bool allow_label_overwrites_;
};

#endif

//  LocalWords:  ds
