#ifndef artdaq_Application_Commandable_hh
#define artdaq_Application_Commandable_hh

#include <string>
#include <vector>
#include <mutex>

#include "fhiclcpp/ParameterSet.h"
#include "canvas/Persistency/Provenance/RunID.h"
#include "artdaq/Application/Commandable_sm.h"  // must be included after others

namespace artdaq
{
	class Commandable;
}

class artdaq::Commandable
{
public:
	Commandable();

	Commandable(Commandable const&) = delete;

	virtual ~Commandable() = default;

	Commandable& operator=(Commandable const&) = delete;

	// these methods define the externally available commands
	bool initialize(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp);

	bool start(art::RunID id, uint64_t timeout, uint64_t timestamp);

	bool stop(uint64_t timeout, uint64_t timestamp);

	bool pause(uint64_t timeout, uint64_t timestamp);

	bool resume(uint64_t timeout, uint64_t timestamp);

	bool shutdown(uint64_t timeout);

	bool soft_initialize(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp);

	bool reinitialize(fhicl::ParameterSet const& ps, uint64_t timeout, uint64_t timestamp);

	bool in_run_failure();

	/* Report_ptr */
	virtual std::string report(std::string const&) const
	{
		std::lock_guard<std::mutex> lk(primary_mutex_);
		return report_string_;
	}

	std::string status() const;

	virtual bool reset_stats(std::string const& which)
	{
		std::lock_guard<std::mutex> lk(primary_mutex_);
		if (which == "fail")
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	virtual std::string register_monitor(fhicl::ParameterSet const&)
	{
		return "This string is returned from Commandable::register_monitor; register_monitor should either be overridden in a derived class or this process should not have been sent the register_monitor call";
	}

	virtual std::string unregister_monitor(std::string const&)
	{
		return "This string is returned from Commandable::unregister_monitor; unregister_monitor should either be overridden in a derived class or this process should not have been sent the unregister_monitor call";
	}

	std::vector<std::string> legal_commands() const;

	// these methods provide the operations that are used by the state machine
	virtual bool do_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

	virtual bool do_start(art::RunID, uint64_t, uint64_t);

	virtual bool do_stop(uint64_t, uint64_t);

	virtual bool do_pause(uint64_t, uint64_t);

	virtual bool do_resume(uint64_t, uint64_t);

	virtual bool do_shutdown(uint64_t);

	virtual bool do_reinitialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

	virtual bool do_soft_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t);

	virtual void badTransition(const std::string&);

	virtual void BootedEnter();

	virtual void InRunExit();

protected:
	std::string current_state() const;

	CommandableContext fsm_;
	bool external_request_status_;
	std::string report_string_;

private:
	// 06-May-2015, KAB: added a mutex to be used in avoiding problems when
	// requests are sent to a Commandable object from different threads. The
	// reason that we're doing this now is that we've added the in_run_failure()
	// transition that will generally be called from inside the Application.
	// Prior to this, the only way that transitions were requested was via
	// external XMLRPC commands, and those were presumed to be called one
	// at a time. The use of scoped locks based on the mutex will prevent
	// the in_run_failure() transition from being called at the same time as
	// an externally requested transition. We only lock the methods that
	// are externally called.
	mutable std::mutex primary_mutex_;
};

#endif /* artdaq_Application_Commandable_hh */
