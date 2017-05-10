#ifndef artdaq_Application_MPI2_AggregatorApp_hh
#define artdaq_Application_MPI2_AggregatorApp_hh

#include <future>

#include "artdaq/Application/AggregatorCore.hh"
#include "artdaq/Application/Commandable.hh"

namespace artdaq
{
	class AggregatorApp;
}

/**
 * \brief AggregatorApp contains the XMLRPC endpoints which control the AggregatorCore
 */
class artdaq::AggregatorApp : public artdaq::Commandable
{
public:
	/**
	 * \brief AggregatorApp Constructor
	 * \param rank The rank of the Aggregator
	 * \param name The nickname of the Aggregator
	 */
	AggregatorApp(int rank, std::string name);

	/**
	 * \brief Copy Constructor is Deleted
	 */
	AggregatorApp(AggregatorApp const&) = delete;

	/**
	 * \brief Default virtual destructor
	 */
	virtual ~AggregatorApp() = default;

	/**
	 * \brief Copy Assignment operator is Deleted
	 * \return AggregatorApp copy
	 */
	AggregatorApp& operator=(AggregatorApp const&) = delete;

	// these methods provide the operations that are used by the state machine
	/**
	 * \brief Initialize the AggregatorCore
	 * \param pset ParameterSet used to initialize the AggregatorCore
	 * \return Whether the initialize transition succeeded
	 */
	bool do_initialize(fhicl::ParameterSet const& pset, uint64_t, uint64_t) override;

	/**
	 * \brief Start the AggregatorCore
	 * \param id Run number of the new run
	 * \return Whether the start transition succeeded
	 */
	bool do_start(art::RunID id, uint64_t, uint64_t) override;

	/**
	 * \brief Stop the AggregatorCore
	 * \return Whether the stop transition succeeded
	 */
	bool do_stop(uint64_t, uint64_t) override;

	/**
	* \brief Pause the AggregatorCore
	* \return Whether the pause transition succeeded
	*/
	bool do_pause(uint64_t, uint64_t) override;

	/**
	* \brief Resume the AggregatorCore
	* \return Whether the resume transition succeeded
	*/
	bool do_resume(uint64_t, uint64_t) override;

	/**
	* \brief Shutdown the AggregatorCore
	* \return Whether the shutdown transition succeeded
	*/
	bool do_shutdown(uint64_t) override;

	/**
	 * \brief Soft-initialize the AggregatorCore. No-Op
	 * \return This function always returns true
	 */
	bool do_soft_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t) override;

	/**
	* \brief Reinitialize the AggregatorCore. No-Op
	* \return This function always returns true
	*/
	bool do_reinitialize(fhicl::ParameterSet const&, uint64_t, uint64_t) override;

	/**
	 * \brief If which is "transition_status", report the status of the last transition. Otherwise pass through to AggregatorCore
	 * \param which What to report on
	 * \return Report string. Empty for unknown "which" parameter
	 */
	std::string report(std::string const& which) const override;

	/**
	 * \brief Register an art Online Monitor to the AggregatorCore
	 * \param info ParameterSet containing information about the monitor
	 * \return String detailing result status
	 */
	std::string register_monitor(fhicl::ParameterSet const& info) override;

	/**
	 * \brief Remove an art Online Monitor from the AggregatorCore
	 * \param label Name of the monitor to remove
	 * \return String detailing result status
	 */
	std::string unregister_monitor(std::string const& label) override;

private:
	int rank_;
	std::string name_;
	std::unique_ptr<AggregatorCore> aggregator_ptr_;
	std::future<size_t> aggregator_future_;
};

#endif
