#ifndef artdaq_Application_CommandableFragmentGenerator_hh
#define artdaq_Application_CommandableFragmentGenerator_hh

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

#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/fwd.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQrate/FragmentBuffer.hh"

namespace artdaq {

/**
	 * \brief CommandableFragmentGenerator is a FragmentGenerator-derived
	 * abstract class that defines the interface for a FragmentGenerator
	 * designed as a state machine with start, stop, etc., transition
	 * commands.
	 *
	 * Users of classes derived from
	 * CommandableFragmentGenerator will call these transitions via the
	 * publically defined StartCmd(), StopCmd(), etc.; these public
	 * functions contain functionality considered properly universal to
	 * all CommandableFragmentGenerator-derived classes, including calls
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
	 * After some discussion with Kurt, CommandableFragmentGenerator has
	 * been updated such that it now contains a member vector
	 * fragment_ids_ ; if "fragment_id" is set in the FHiCL document
	 * controlling a class derived from CommandableFragmentGenerator,
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
class CommandableFragmentGenerator : public FragmentGenerator
{
public:
	/// <summary>
	/// Configuration of the CommandableFragmentGenerator. May be used for parameter validation
	/// </summary>
	struct Config
	{
		/// "generator" (REQUIRED) Name of the CommandableFragmentGenerator plugin to load
		fhicl::Atom<std::string> generator_type{fhicl::Name{"generator"}, fhicl::Comment{"Name of the CommandableFragmentGenerator plugin to load"}};
		/// "expected_fragment_type" (Default: 231, EmptyFragmentType) : The type of Fragments this CFG will be generating. "Empty" will auto - detect type based on Fragments generated.
		fhicl::Atom<Fragment::type_t> expected_fragment_type{fhicl::Name{"expected_fragment_type"}, fhicl::Comment{"The type of Fragments this CFG will be generating. \"Empty\" will auto-detect type based on Fragments generated."}, Fragment::type_t(Fragment::EmptyFragmentType)};
		/// "sleep_on_no_data_us" (Default: 0 (no sleep)) : How long to sleep after calling getNext_ if no data is returned
		fhicl::Atom<size_t> sleep_on_no_data_us{fhicl::Name{"sleep_on_no_data_us"}, fhicl::Comment{"How long to sleep after calling getNext_ if no data is returned"}, 0};
		/// "separate_monitoring_thread" (Default: false) : Whether a thread that calls the checkHWStatus_ method should be created
		fhicl::Atom<bool> separate_monitoring_thread{fhicl::Name{"separate_monitoring_thread"}, fhicl::Comment{"Whether a thread that calls the checkHWStatus_ method should be created"}, false};
		/// "hardware_poll_interval_us" (Default: 0) : If a separate monitoring thread is used, how often should it call checkHWStatus_
		fhicl::Atom<int64_t> hardware_poll_interval_us{fhicl::Name{"hardware_poll_interval_us"}, fhicl::Comment{"If a separate monitoring thread is used, how often should it call checkHWStatus_"}, 0};
		/// "board_id" (REQUIRED) : The identification number for this CommandableFragmentGenerator
		fhicl::Atom<int> board_id{fhicl::Name{"board_id"}, fhicl::Comment{"The identification number for this CommandableFragmentGenerator"}};
		/// "fragment_ids" (Default: empty vector) : A list of Fragment IDs created by this CommandableFragmentGenerator
		/// Note that only one of fragment_ids and fragment_id should be specified in the configuration
		fhicl::Sequence<Fragment::fragment_id_t> fragment_ids{fhicl::Name("fragment_ids"), fhicl::Comment("A list of Fragment IDs created by this CommandableFragmentGenerator")};
		/// "fragment_id" (Default: -99) : The Fragment ID created by this CommandableFragmentGenerator
		/// Note that only one of fragment_ids and fragment_id should be specified in the configuration
		fhicl::Atom<int> fragment_id{fhicl::Name{"fragment_id"}, fhicl::Comment{"The Fragment ID created by this CommandableFragmentGenerator"}, -99};
		/// "sleep_on_stop_us" (Default: 0) : How long to sleep before returning when stop transition is called
		fhicl::Atom<int> sleep_on_stop_us{fhicl::Name{"sleep_on_stop_us"}, fhicl::Comment{"How long to sleep before returning when stop transition is called"}, 0};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
		 * \brief CommandableFragmentGenerator Constructor
		 * \param ps ParameterSet used to configure CommandableFragmentGenerator. See artdaq::CommandableFragmentGenerator::Config.
		 */
	explicit CommandableFragmentGenerator(const fhicl::ParameterSet& ps);

	/**
		 * \brief CommandableFragmentGenerator Destructor
		 *
		 * Joins all threads before returning
		 */
	virtual ~CommandableFragmentGenerator();

	/**
		 * \brief Join any data-taking threads. Should be called when destructing CommandableFragmentGenerator
		 *
		 * Join any data-taking threads. Should be called when destructing CommandableFragmentGenerator
		 * Sets flags so that threads stop operations.
		 */
	void joinThreads();

	/**
		 * \brief getNext calls either applyRequests or getNext_ to get any data that is ready to be sent to the EventBuilders
		 * \param output FragmentPtrs object containing Fragments ready for transmission
		 * \return Whether getNext completed without exceptions
		 */
	bool getNext(FragmentPtrs& output) override final;

	/**
		 * \brief Function that launches the monitoring thread (getMonitoringDataLoop())
		 */
	void startMonitoringThread();

	/**
		 * \brief This function regularly calls checkHWStatus_(), and sets the isHardwareOK flag accordingly.
		 */
	void getMonitoringDataLoop();

	/**
		 * \brief Get the list of Fragment IDs handled by this CommandableFragmentGenerator
		 * \return A std::vector<Fragment::fragment_id_t> containing the Fragment IDs handled by this CommandableFragmentGenerator
		 */
	std::vector<Fragment::fragment_id_t> fragmentIDs() override
	{
		std::vector<Fragment::fragment_id_t> output;

		for (auto& id : expectedTypes_)
		{
			output.push_back(id.first);
		}

		return output;
	}

	/**
		* \brief Get the current value of the event counter
		* \return The current value of the event counter
		*/
	size_t ev_counter() const { return ev_counter_.load(); }

	//
	// State-machine related interface below.
	//

	/**
		 * \brief Start the CommandableFragmentGenerator
		 * \param run Run ID of the new run
		 * \param timeout Timeout for transition
		 * \param timestamp Timestamp of transition
		 *
		 * After a call to 'StartCmd', all Fragments returned by getNext()
		 * will be marked as part of a Run with the given run number, and
		 * with subrun number 1. Calling StartCmd also resets the event
		 * number to 1.  After a call to StartCmd(), and until a call to
		 * StopCmd, getNext() -- and hence the virtual function it calls,
		 * getNext_() -- should return true as long as datataking is meant
		 * to take place, even if a particular call returns no fragments.
		 */
	void StartCmd(int run, uint64_t timeout, uint64_t timestamp);

	/**
		 * \brief Stop the CommandableFragmentGenerator
		 * \param timeout Timeout for transition
		 * \param timestamp Timestamp of transition
		 *
		 * After a call to StopCmd(), getNext() will eventually return
		 * false. This may not happen for several calls, if the
		 * implementation has data to be 'drained' from the system.
		 */
	void StopCmd(uint64_t timeout, uint64_t timestamp);

	/**
		 * \brief Pause the CommandableFragmentGenerator
		 * \param timeout Timeout for transition
		 * \param timestamp Timestamp of transition
		 *
		 * A call to PauseCmd() is advisory. It is an indication that the
		 * BoardReader should stop the incoming flow of data, if it can do
		 * so.
		 */
	void PauseCmd(uint64_t timeout, uint64_t timestamp);

	/**
		 * \brief Resume the CommandableFragmentGenerator
		 * \param timeout Timeout for transition
		 * \param timestamp Timestamp of transition
		 *
		 * After a call to ResumeCmd(), the next Fragments returned from
		 * getNext() will be part of a new SubRun.
		 */
	void ResumeCmd(uint64_t timeout, uint64_t timestamp);

	/**
		 * \brief Get a report about a user-specified run-time quantity
		 * \param which Which quantity to report
		 * \return The report about the specified quantity
		 *
		 * CommandableFragmentGenerator only implements "latest_exception",
		 * a report on the last exception received. However, child classes
		 * can override the reportSpecific function to provide additional
		 * reports.
		 */
	std::string ReportCmd(std::string const& which = "");

	/**
		 * \brief Get the name used when reporting metrics
		 * \return The name used when reporting metrics
		 */
	virtual std::string metricsReportingInstanceName() const
	{
		return instance_name_for_metrics_;
	}

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
		 * \brief Get the current value of the exception flag
		 * \return The current value of the exception flag
		 */
	bool exception() const { return exception_.load(); }

	/**
		* \brief The meta-command is used for implementing user-specific commands in a CommandableFragmentGenerator
		* \param command Name of the command to run
		* \param arg Argument(s) for command
		* \return true if command succeeded or if command not supported
		*/
	virtual bool metaCommand(std::string const& command, std::string const& arg);

	void SetRequestBuffer(std::shared_ptr<RequestBuffer> buffer) { requestBuffer_ = buffer; }

protected:
	// John F., 12/6/13 -- need to figure out which of these getter
	// functions should be promoted to "public"

	// John F., 1/21/15 -- after more than a year, there hasn't been a
	// single complaint that a CommandableFragmentGenerator-derived
	// class hasn't allowed its users to access these quantities, so
	// they're probably fine as is

	/**
		 * \brief Get the current Run number
		 * \return The current Run number
		 */
	int run_number() const { return run_number_; }
	/**
		 * \brief Get the current Subrun number
		 * \return The current Subrun number
		 */
	int subrun_number() const { return subrun_number_; }
	/**
		 * \brief Timeout of last command
		 * \return Timeout of last command
		 */
	uint64_t timeout() const { return timeout_; }
	/**
		 * \brief Timestamp of last command
		 * \return Timestamp of last command
		 */
	uint64_t timestamp() const { return timestamp_; }

	/**
		 * \brief Get the Fragment ID of this Fragment generator
		 * \throws cet::exception("FragmentID") if there is more that one Fragment ID configured for this Fragment Generator
		 * \return Fragment ID for the Fragment Generator
		 */
	artdaq::Fragment::fragment_id_t fragment_id() const
	{
		if (expectedTypes_.size() > 1) throw cet::exception("FragmentID") << "fragment_id() was called, indicating that Fragment Generator was expecting one and only one Fragment ID, but " << expectedTypes_.size() << " were declared!";  // NOLINT(cert-err60-cpp)
		return (*expectedTypes_.begin()).first;
	}

	/**
		 * \brief Get the current value of the should_stop flag
		 * \return The current value of the should_stop flag
		 */
	bool should_stop() const { return should_stop_.load(); }

	/**
		 * \brief Routine used by applyRequests to make sure that all outstanding requests have been fulfilled before returning
		 * \return The logical AND of should_stop, mode is not Ignored, and requests list size equal to 0
		 */
	bool check_stop();

	/**
		 * \brief Gets the current board_id
		 * \return The current board_id
		 */
	int board_id() const { return board_id_; }

	/**
		 * \brief Increment the event counter
		 * \param step Amount to increment the event counter by
		 * \return The previous value of the event counter
		 */
	size_t ev_counter_inc(size_t step = 1);  // returns the prev value

	/**
		 * \brief Control the exception flag
		 * \param exception Whether an excpetion has occurred
		 */
	void set_exception(bool exception) { exception_.store(exception); }

	/**
		 * \brief Sets the name for metrics reporting
		 * \param name The new name for metrics reporting
		 */
	void metricsReportingInstanceName(std::string const& name)
	{
		instance_name_for_metrics_ = name;
	}

	// John F., 12/10/13
	// Is there a better way to handle mutex_ than leaving it a protected variable?

	// John F., 1/21/15
	// Translation above is "should mutex_ be a private variable,
	// accessible via a getter function". Probably, but at this point
	// it's not worth breaking code by implementing this.

	std::mutex mutex_;  ///< Mutex used to ensure that multiple transition commands do not run at the same time

	std::shared_ptr<RequestBuffer> GetRequestBuffer() { return requestBuffer_; }

private:
	CommandableFragmentGenerator(CommandableFragmentGenerator const&) = delete;
	CommandableFragmentGenerator(CommandableFragmentGenerator&&) = delete;
	CommandableFragmentGenerator& operator=(CommandableFragmentGenerator const&) = delete;
	CommandableFragmentGenerator& operator=(CommandableFragmentGenerator&&) = delete;

	// FHiCL-configurable variables. Note that the C++ variable names
	// are the FHiCL variable names with a "_" appended

	//Socket parameters

	std::map<Fragment::fragment_id_t, Fragment::type_t> expectedTypes_;

	bool useMonitoringThread_;
	boost::thread monitoringThread_;
	int64_t monitoringInterval_;  // Microseconds
	std::chrono::steady_clock::time_point lastMonitoringCall_;
	std::atomic<bool> isHardwareOK_;

	// In order to support the state-machine related behavior, all
	// CommandableFragmentGenerators must be able to remember a run number and a
	// subrun number.
	int run_number_, subrun_number_;

	// JCF, 8/28/14

	// Provide a user-adjustable timeout for the start transition
	uint64_t timeout_;

	// JCF, 8/21/14

	// In response to a need to synchronize various components using
	// different fragment generators in an experiment, keep a record
	// of a timestamp (see Redmine Issue #6783 for more)

	uint64_t timestamp_;

	std::atomic<bool> should_stop_, exception_;
	std::string latest_exception_report_;
	std::atomic<size_t> ev_counter_;

	int board_id_;
	std::string instance_name_for_metrics_;

	// Depending on what sleep_on_stop_us_ is set to, this gives the
	// stopping thread the chance to gather the required lock

	int sleep_on_stop_us_;

	// So that derived classes can access information about requests
	std::shared_ptr<RequestBuffer> requestBuffer_;

protected:
	/// <summary>
	/// Obtain the next group of Fragments, if any are available. Return false if readout cannot continue,
	/// if we are 'stopped', or if we are not running in state-machine mode.
	/// Note that getNext_() must return n of each fragmentID declared by fragmentIDs_().
	/// </summary>
	/// <param name="output">Reference to list of Fragment pointers to which additional Fragments should be added</param>
	/// <returns>True if readout should continue, false otherwise</returns>
	virtual bool getNext_(FragmentPtrs& output) = 0;

	/// <summary>
	/// Check any relavent hardware status registers. Return false if
	/// an error condition exists that should halt data-taking.
	/// This function should probably make MetricManager calls.
	/// </summary>
	/// <returns>False if a condition exists that should halt all data-taking, true otherwise</returns>
	virtual bool checkHWStatus_();

	//
	// State-machine related implementor interface below.
	//

	/// <summary>
	/// If a CommandableFragmentGenerator subclass is reading from a
	/// file, and start() is called, any run-, subrun-, and
	/// event-numbers in the data read from the file must be
	/// over-written by the specified run number, etc. After a call to
	/// StartCmd(), and until a call to StopCmd(), getNext_() is
	/// expected to return true as long as datataking is intended.
	///
	/// This is a pure virtual function, and must be overriden by Fragment Generator implementations
	/// </summary>
	virtual void start() = 0;

	/// <summary>
	/// On call to StopCmd, stopNoMutex() is called prior to StopCmd
	/// acquiring the mutex
	///
	/// This is a pure virtual function, and must be overriden by Fragment Generator implementations
	/// </summary>
	virtual void stopNoMutex() = 0;

	/// <summary>
	/// If a CommandableFragmentGenerator subclass is reading from a file, calling
	/// stop() should arrange that the next call to getNext_() returns
	/// false, rather than allowing getNext_() to read to the end of the
	/// file.
	///
	/// This is a pure virtual function, and must be overriden by Fragment Generator implementations
	/// </summary>
	virtual void stop() = 0;

	/// <summary>
	/// On call to PauseCmd, pauseNoMutex() is called prior to PauseCmd
	/// acquiring the mutex
	/// </summary>
	virtual void pauseNoMutex();

	/// <summary>
	/// If a CommandableFragmentGenerator subclass is reading from hardware, the
	/// implementation of pause() should tell the hardware to stop
	/// sending data.
	/// </summary>
	virtual void pause();

	/// <summary>
	/// The subrun number will be incremented *before* a call to resume.
	/// </summary>
	virtual void resume();

	/// <summary>
	/// Let's say that the contract with the report() functions is that they
	/// return a non-empty string if they have something useful to report,
	/// but if they don't know how to handle a given request, they simply
	/// return an empty string and the ReportCmd() takes care of saying
	/// "the xyz command is not currently supported".
	/// For backward compatibility, we keep the report function that takes
	/// no arguments and add one that takes a "which" argument. In the
	/// ReportCmd function, we'll call the more specific one first.
	/// </summary>
	/// <returns>Default report from the FragmentGenerator</returns>
	virtual std::string report();

	/// <summary>
	/// Report the status of a specific quantity
	/// </summary>
	/// <param name="what">Name of the quantity to report</param>
	/// <returns>Value of requested quantity. Null string if what is unsupported.</returns>
	virtual std::string reportSpecific(std::string const& what);
};
}  // namespace artdaq

#endif /* artdaq_Application_CommandableFragmentGenerator_hh */
