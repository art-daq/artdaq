#ifndef artdaq_Application_CommandableFragmentGenerator_hh
#define artdaq_Application_CommandableFragmentGenerator_hh

////////////////////////////////////////////////////////////////////////
// CommandableFragmentGenerator is a FragmentGenerator-derived
// abstract class that defines the interface for a FragmentGenerator
// designed as a state machine with start, stop, etc., transition
// commands. Users of classes derived from
// CommandableFragmentGenerator will call these transitions via the
// publically defined StartCmd(), StopCmd(), etc.; these public
// functions contain functionality considered properly universal to
// all CommandableFragmentGenerator-derived classes, including calls
// to private virtual functions meant to be overridden in derived
// classes. The same applies to this class's implementation of the
// FragmentGenerator::getNext() pure virtual function, which is
// declared final (i.e., non-overridable in derived classes) and which
// itself calls a pure virtual getNext_() function to be implemented
// in derived classes.

// State-machine related interface functions will be called only from a
// single thread. getNext() will be called only from a single
// thread. The thread from which state-machine interfaces functions are
// called may be a different thread from the one that calls getNext().

// John F., 3/24/14

// After some discussion with Kurt, CommandableFragmentGenerator has
// been updated such that it now contains a member vector
// fragment_ids_ ; if "fragment_id" is set in the FHiCL document
// controlling a class derived from CommandableFragmentGenerator,
// fragment_ids_ will be booked as a length-1 vector, and the value in
// this vector will be returned by fragment_id(). fragment_id() will
// throw an exception if the length of the vector isn't 1. If
// "fragment_ids" is set in the FHiCL document, then fragment_ids_ is
// filled with the values in the list which "fragment_ids" refers to,
// otherwise it is set to the empty vector (this is what should happen
// if the user sets the "fragment_id" variable in the FHiCL document,
// otherwise exceptions will end up thrown due to the logical
// conflict). If neither "fragment_id" nor "fragment_ids" is set in
// the FHiCL document, writers of classes derived from this one will
// be expected to override the virtual fragmentIDs() function with
// their own code (the CompositeDriver class is an example of this)


////////////////////////////////////////////////////////////////////////

// Socket Includes
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <chrono>
#include <array>
#include <list>

#include "fhiclcpp/fwd.h"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Data/Fragments.hh"
#include "artdaq-core/Generators/FragmentGenerator.hh"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "artdaq/DAQrate/detail/RequestMessage.hh"

namespace artdaq {
	enum class RequestMode {
		Single,
		Buffer,
		Window,
		Ignored
	};

	class CommandableFragmentGenerator : public FragmentGenerator {
	public:

		CommandableFragmentGenerator();
		CommandableFragmentGenerator(const fhicl::ParameterSet &);

		// Destroy the CommandableFragmentGenerator.
		virtual ~CommandableFragmentGenerator();

		virtual bool getNext(FragmentPtrs & output) final;

		// If we're using the dataThread, this method looks for active data requests
		// and returns matching data
		virtual bool applyRequests(FragmentPtrs & output) final;
		void setupRequestListener();
		bool sendEmptyFragment(FragmentPtrs& frags, size_t sequenceId, std::string desc);
		void sendEmptyFragments(FragmentPtrs& frags);

		void startDataThread();
		void startMonitoringThread();
		void startRequestReceiverThread();

		void getDataLoop();
		bool dataBufferIsTooLarge();
		void getDataBufferStats();
		void checkDataBuffer();
		void getMonitoringDataLoop();
		void receiveRequestsLoop();

		virtual std::vector<Fragment::fragment_id_t> fragmentIDs() {
			return fragment_ids_;
		}

		//
		// State-machine related interface below.
		//


		// After a call to 'StartCmd', all Fragments returned by getNext()
		// will be marked as part of a Run with the given run number, and
		// with subrun number 1. Calling StartCmd also resets the event
		// number to 1.  After a call to StartCmd(), and until a call to
		// StopCmd, getNext() -- and hence the virtual function it calls,
		// getNext_() -- should return true as long as datataking is meant
		// to take place, even if a particular call returns no fragments.

		void StartCmd(int run, uint64_t timeout, uint64_t timestamp);

		// After a call to StopCmd(), getNext() will eventually return
		// false. This may not happen for several calls, if the
		// implementation has data to be 'drained' from the system.
		void StopCmd(uint64_t timeout, uint64_t timestamp);

		// A call to PauseCmd() is advisory. It is an indication that the
		// BoardReader should stop the incoming flow of data, if it can do
		// so.
		void PauseCmd(uint64_t timeout, uint64_t timestamp);

		// After a call to ResumeCmd(), the next Fragments returned from
		// getNext() will be part of a new SubRun.
		void ResumeCmd(uint64_t timeout, uint64_t timestamp);

		// 14-May-2015, KAB: added a "which" argument to allow callers
		// to specify particular types of report information.
		std::string ReportCmd(std::string const& which = "");

		virtual std::string metricsReportingInstanceName() const {
			return instance_name_for_metrics_;
		}

		void SetMetricManager(MetricManager* metricMan) { metricMan_ = metricMan; }

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

		bool exception() const { return exception_.load(); }

	protected:

		// John F., 12/6/13 -- need to figure out which of these getter
		// functions should be promoted to "public"

		// John F., 1/21/15 -- after more than a year, there hasn't been a
		// single complaint that a CommandableFragmentGenerator-derived
		// class hasn't allowed its users to access these quantities, so
		// they're probably fine as is

		int run_number() const { return run_number_; }
		int subrun_number() const { return subrun_number_; }
		uint64_t timeout() const { return timeout_; }
		uint64_t timestamp() const { return timestamp_; }
		bool should_stop() const { return should_stop_.load(); }

		// ELF: 8/18/16: If we're running in a threaded way, we should let the applyRequests
		// routine clear out the request buffer before stopping.
		bool check_stop();

		int board_id() const { return board_id_; }

		int fragment_id() const;

		size_t ev_counter() const { return ev_counter_.load(); }

		size_t ev_counter_inc(size_t step = 1, bool force = false); // returns the prev value

		void set_exception(bool exception) { exception_.store(exception); }

		void metricsReportingInstanceName(std::string const& name) {
			instance_name_for_metrics_ = name;
		}

		std::string printMode_();

		// John F., 12/10/13 
		// Is there a better way to handle mutex_ than leaving it a protected variable?

		// John F., 1/21/15
		// Translation above is "should mutex_ be a private variable,
		// accessible via a getter function". Probably, but at this point
		// it's not worth breaking code by implementing this. 

		std::mutex mutex_;
		MetricManager* metricMan_;

	private:
		// FHiCL-configurable variables. Note that the C++ variable names
		// are the FHiCL variable names with a "_" appended
		bool listenForRequests_;
		int request_port_;
		std::string request_addr_;

		//Socket parameters
		struct sockaddr_in si_data_;
		int request_socket_;
		std::map<Fragment::sequence_id_t, Fragment::timestamp_t > requests_;
		std::mutex request_mutex_;
		std::thread requestThread_;

		RequestMode mode_;
		Fragment::timestamp_t windowOffset_;
		Fragment::timestamp_t windowWidth_;
		Fragment::timestamp_t staleTimeout_;
		size_t maxFragmentCount_;
		bool uniqueWindows_;

		bool useDataThread_;
		std::thread dataThread_;

		std::condition_variable requestCondition_;
		std::condition_variable dataCondition_;
		std::atomic<int> dataBufferDepthFragments_;
		std::atomic<size_t> dataBufferDepthBytes_;
		int maxDataBufferDepthFragments_;
		size_t maxDataBufferDepthBytes_;

		bool useMonitoringThread_;
		bool collectMonitoringData_;
		std::thread monitoringThread_;
		int64_t monitoringInterval_; // Microseconds
		std::chrono::steady_clock::time_point lastMonitoringCall_;
		bool isHardwareOK_;

		FragmentPtrs dataBuffer_;
		FragmentPtrs newDataBuffer_;
		std::mutex dataBufferMutex_;

		std::vector< artdaq::Fragment::fragment_id_t > fragment_ids_;

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

		// Obtain the next group of Fragments, if any are available. Return
		// false if no more data are available, if we are 'stopped', or if
		// we are not running in state-machine mode. Note that getNext_()
		// must return n of each fragmentID declared by fragmentIDs_().
		virtual bool getNext_(FragmentPtrs & output) = 0;


		// Check any relavent hardware status registers. Return false if
		// an error condition exists that should halt data-taking.
		// This function should probably make MetricManager calls.
		virtual bool checkHWStatus_();

		//
		// State-machine related implementor interface below.
		//

		// If a CommandableFragmentGenerator subclass is reading from a
		// file, and start() is called, any run-, subrun-, and
		// event-numbers in the data read from the file must be
		// over-written by the specified run number, etc. After a call to
		// StartCmd(), and until a call to StopCmd(), getNext_() is
		// expected to return true as long as datataking is intended.
		virtual void start() = 0;

		// On call to StopCmd, stopNoMutex() is called prior to StopCmd
		// acquiring the mutex
		virtual void stopNoMutex() = 0;

		// If a CommandableFragmentGenerator subclass is reading from a file, calling
		// stop() should arrange that the next call to getNext_() returns
		// false, rather than allowing getNext_() to read to the end of the
		// file.
		virtual void stop() = 0;

		// On call to PauseCmd, pauseNoMutex() is called prior to PauseCmd
		// acquiring the mutex
		virtual void pauseNoMutex();

		// If a CommandableFragmentGenerator subclass is reading from hardware, the
		// implementation of pause() should tell the hardware to stop
		// sending data.
		virtual void pause();

		// The subrun number will be incremented *before* a call to
		// resume. Subclasses are responsible for assuring that, after a
		// call to resume, that getNext_() will return Fragments marked with
		// the correct subrun number (and run number).
		virtual void resume();

		// Let's say that the contract with the report() functions is that they
		// return a non-empty string if they have something useful to report,
		// but if they don't know how to handle a given request, they simply
		// return an empty string and the ReportCmd() takes care of saying
		// "the xyz command is not currently supported".
		// For backward compatibility, we keep the report function that takes
		// no arguments and add one that takes a "which" argument. In the 
		// ReportCmd function, we'll call the more specific one first.
		virtual std::string report();
		virtual std::string reportSpecific(std::string const&);
	};

}

#endif /* artdaq_Application_CommandableFragmentGenerator_hh */
