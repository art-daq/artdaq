#include "artdaq/Application/Commandable.hh"
#include "artdaq/DAQdata/Globals.hh"

artdaq::Commandable::Commandable() : fsm_(*this)
, primary_mutex_() {}

// **********************************************************************
// *** The following methods implement the externally available commands.
// **********************************************************************

bool artdaq::Commandable::initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.init(pset, timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after an init transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.start(id, timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a start transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::stop(uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.stop(timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a stop transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::pause(uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.pause(timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a pause transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::resume(uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.resume(timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a resume transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::shutdown(uint64_t timeout)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.shutdown(timeout);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a shutdown transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.soft_init(pset, timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a soft_init transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "All is OK.";

	std::string initialState = fsm_.getState().getName();
	fsm_.reinit(pset, timeout, timestamp);
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after a reinit transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

bool artdaq::Commandable::in_run_failure()
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	external_request_status_ = true;
	report_string_ = "An error condition was reported while running.";

	std::string initialState = fsm_.getState().getName();
	fsm_.in_run_failure();
	if (external_request_status_)
	{
		std::string finalState = fsm_.getState().getName();
		TLOG_DEBUG("CommandableInterface")
			<< "States before and after an in_run_failure transition: "
			<< initialState << " and " << finalState << TLOG_ENDL;
	}

	return (external_request_status_);
}

std::string artdaq::Commandable::status() const
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	std::string currentState = this->current_state();
	if (currentState == "InRunError")
	{
		return "Error";
	}

	return currentState;
}

std::vector<std::string> artdaq::Commandable::legal_commands() const
{
	std::lock_guard<std::mutex> lk(primary_mutex_);
	std::string currentState = this->current_state();

	if (currentState == "Ready")
	{
		return { "init", "soft_init", "start", "shutdown" };
	}
	if (currentState == "Running")
	{
		// 12-May-2015, KAB: in_run_failure is also possible, but it
		// shouldn't be requested externally.
		return { "pause", "stop" };
	}
	if (currentState == "Paused")
	{
		return { "resume", "stop" };
	}
	if (currentState == "InRunError")
	{
		return { "pause", "stop" };
	}

	// Booted and Error
	return { "init", "shutdown" };
}

// *******************************************************************
// *** The following methods implement the state machine operations.
// *******************************************************************

bool artdaq::Commandable::do_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_initialize called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_start(art::RunID, uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_start called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_stop(uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_stop called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_pause(uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_pause called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_resume(uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_resume called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_shutdown(uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_shutdown called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_reinitialize(fhicl::ParameterSet const&, uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_reinitialize called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

bool artdaq::Commandable::do_soft_initialize(fhicl::ParameterSet const&, uint64_t, uint64_t)
{
	TLOG_DEBUG("CommandableInterface") << "do_soft_initialize called." << TLOG_ENDL;
	external_request_status_ = true;
	return external_request_status_;
}

void artdaq::Commandable::badTransition(const std::string& trans)
{
	std::string currentState = this->current_state();
	if (currentState == "InRunError")
	{
		currentState = "Error";
	}

	report_string_ = "An invalid transition (";
	report_string_.append(trans);
	report_string_.append(") was requested; transition not allowed from this process's current state of \"");
	report_string_.append(currentState);
	report_string_.append("\"");

	TLOG_WARNING("CommandableInterface") << report_string_ << TLOG_ENDL;

	external_request_status_ = false;
}

void artdaq::Commandable::BootedEnter()
{
	TLOG_DEBUG("CommandableInterface") << "BootedEnter called." << TLOG_ENDL;
}

void artdaq::Commandable::InRunExit()
{
	TLOG_DEBUG("CommandableInterface") << "InRunExit called." << TLOG_ENDL;
}


bool artdaq::Commandable::do_trace_memory_set(std::string const& name, uint64_t mask)
{
	TLOG_DEBUG("CommandableInterface") << "Setting mskM for name " << name << " to " << std::hex << std::showbase << mask << TLOG_ENDL;
	if (name != "TRACE")
	{
		TRACE_CNTL("lvlmsknM", name.c_str(), mask);
	}
	else
	{
		TRACE_CNTL("lvlmskMg", mask);
	}
	return true;
}

bool artdaq::Commandable::do_trace_msgfacility_set(std::string const& name, uint64_t mask)
{
	TLOG_DEBUG("CommandableInterface") << "Setting mskS for name " << name << " to " << std::hex << std::showbase << mask << TLOG_ENDL;
	if (name != "TRACE")
	{
		TRACE_CNTL("lvlmsknS", name.c_str(), mask);
	}
	else
	{
		TRACE_CNTL("lvlmskSg", mask);
	}
	return true;
}

bool artdaq::Commandable::do_meta_command(std::string const& cmd, std::string const& arg)
{
	TLOG_DEBUG("CommandableInterface") << "Meta-Command called: cmd=" << cmd << ", arg=" << arg << TLOG_ENDL;
	return true;
}

// *********************
// *** Utility methods. 
// *********************

std::string artdaq::Commandable::current_state() const
{
	std::string fullStateName = (const_cast<Commandable*>(this))->fsm_.getState().getName();
	size_t pos = fullStateName.rfind("::");
	if (pos != std::string::npos)
	{
		return fullStateName.substr(pos + 2);
	}
	else
	{
		return fullStateName;
	}
}
