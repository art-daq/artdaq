#ifndef artdaq_Application_MPI2_RoutingManagerApp_hh
#define artdaq_Application_MPI2_RoutingManagerApp_hh

#include "artdaq/Application/Commandable.hh"
#include "artdaq/Application/RoutingManagerCore.hh"

namespace artdaq {
class RoutingManagerApp;
}

/**
* \brief RoutingManagerApp is an artdaq::Commandable derived class which controls the RoutingManagerCore state machine
*/
class artdaq::RoutingManagerApp : public artdaq::Commandable
{
public:
	/**
	* \brief RoutingManagerApp Constructor
	*/
	RoutingManagerApp();

	/**
	* \brief Copy Constructor is deleted
	*/
	RoutingManagerApp(RoutingManagerApp const&) = delete;

	/**
	* \brief Default Destructor
	*/
	virtual ~RoutingManagerApp() = default;

	/**
	* \brief Copy Assignment Operator is deleted
	* \return RoutingManagerApp copy
	*/
	RoutingManagerApp& operator=(RoutingManagerApp const&) = delete;
	RoutingManagerApp(RoutingManagerApp&&) = delete;  ///< Move Constructor is deleted
	RoutingManagerApp& operator=(RoutingManagerApp&&) = delete;  ///< Move Assignment Operator is deleted

	// these methods provide the operations that are used by the state machine
	/**
	* \brief Initialize the RoutingManagerCore
	* \param pset ParameterSet used to configure the RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Start the RoutingManagerCore
	* \param id Run ID of new run
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_start(art::RunID id, uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Stop the RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_stop(uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Pause the RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_pause(uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Resume the RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_resume(uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Shutdown the RoutingManagerCore
	* \param timeout Timeout for transition
	* \return Whether the transition succeeded
	*/
	bool do_shutdown(uint64_t timeout) override;

	/**
	* \brief Soft-Initialize the RoutingManagerCore
	* \param pset ParameterSet used to configure the RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Reinitialize the RoutingManagerCore
	* \param pset ParameterSet used to configure the RoutingManagerCore
	* \param timeout Timeout for transition
	* \param timestamp Timestamp of transition
	* \return Whether the transition succeeded
	*/
	bool do_reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp) override;

	/**
	* \brief Action taken upon entering the "Booted" state
	*
	* This resets the RoutingManagerCore pointer
	*/
	void BootedEnter() override;

	/* Report_ptr */
	/**
	 * \brief If which is "transition_status", report the status of the last transition. Otherwise pass through to AggregatorCore
	 * \param which What to report on
	 * \return Report string. Empty for unknown "which" parameter
	 */
	std::string report(std::string const&) const override;

private:
	std::unique_ptr<artdaq::RoutingManagerCore> routing_manager_ptr_;
	boost::thread routing_manager_thread_;
};

#endif /* artdaq_Application_MPI2_RoutingManagerApp_hh */
