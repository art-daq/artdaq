#ifndef artdaq_Application_MPI2_EventBuilderApp_hh
#define artdaq_Application_MPI2_EventBuilderApp_hh

#include "artdaq/Application/Commandable.hh"
#include "artdaq/Application/EventBuilderCore.hh"

#include <memory>

namespace artdaq {
class EventBuilderApp;
}

/**
 * \brief EventBuilderApp is an artdaq::Commandable derived class which controls the EventBuilderCore
 */
class artdaq::EventBuilderApp : public artdaq::Commandable
{
public:
	/**
	* \brief EventBuilderApp Constructor
	*/
	EventBuilderApp();

	/**
	* \brief Copy Constructor is deleted
	*/
	EventBuilderApp(EventBuilderApp const&) = delete;

	/**
	* \brief Default Destructor
	*/
	virtual ~EventBuilderApp() = default;

	/**
	* \brief Copy Assignment Operator is deleted
	* \return EventBuilderApp copy
	*/
	EventBuilderApp& operator=(EventBuilderApp const&) = delete;
	EventBuilderApp(EventBuilderApp&&) = delete;             ///< Move Constructor is deleted
	EventBuilderApp& operator=(EventBuilderApp&&) = delete;  ///< Move Assignment Operator is deleted

	// these methods provide the operations that are used by the state machine
	/**
	* \brief Initialize the EventBuilderCore
	* \param pset ParameterSet used to configure the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t) override;

	/**
	* \brief Start the EventBuilderCore
	* \param id Run ID of new run
	* \return Whether the transition succeeded
	*/
	bool do_start(art::RunID id, uint64_t, uint64_t) override;

	/**
	* \brief Stop the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_stop(uint64_t, uint64_t) override;

	/**
	* \brief Pause the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_pause(uint64_t, uint64_t) override;

	/**
	* \brief Resume the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_resume(uint64_t, uint64_t) override;

	/**
	* \brief Shutdown the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_shutdown(uint64_t) override;

	/**
	* \brief Soft-Initialize the EventBuilderCore
	* \param pset ParameterSet used to configure the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_soft_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t) override;

	/**
	* \brief Reinitialize the EventBuilderCore
	* \param pset ParameterSet used to configure the EventBuilderCore
	* \return Whether the transition succeeded
	*/
	bool do_reinitialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t) override;

	/**
	* \brief Rollover the subrun after the given event
	* \param boundary Sequence ID of boundary
	* \param subrunNum Number of new subrun
	* \return True event_store_ptr is valid
	*/
	bool do_rollover_subrun(uint64_t boundary, uint32_t subrunNum) override;

	/**
	* \brief Action taken upon entering the "Booted" state
	*
	* This is a No-Op
	*/
	void BootedEnter() override;

	/* Report_ptr */
	/**
	 * \brief If which is "transition_status", report the status of the last transition. Otherwise pass through to EventBuilderCore
	 * \param which What to report on
	 * \return Report string. Empty for unknown "which" parameter
	 */
	std::string report(std::string const& which) const override;

	/**
	* \brief Add the specified configuration archive entry to the EventBuilderCore
	* \return Whether the command succeeded
	*/
	bool do_add_config_archive_entry(std::string const&, std::string const&) override;

	/**
	* \brief Clear the configuration archive list in the EventBuilderCore
	* \return Whether the command succeeded
	*/
	bool do_clear_config_archive() override;

	/**
	 * \brief Override the Fragment ID list for a given event
	 * \return Whether the command succeeded (always true)
	 */
	bool do_override_fragment_ids(uint64_t seqID, std::vector<uint32_t> frags) override;

	/**
	 * \brief Update the Fragment ID list at the given event
	 * \return Whether the command succeeded (always true)
	 */
	bool do_update_default_fragment_ids(uint64_t seqID, std::vector<uint32_t> frags) override;

private:
	std::unique_ptr<artdaq::EventBuilderCore> event_builder_ptr_;
};

#endif /* artdaq_Application_MPI2_EventBuilderApp_hh */
