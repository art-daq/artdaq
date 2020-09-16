#define TRACE_NAME "DispatcherApp"

#include "artdaq/Application/DispatcherApp.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/Application/DispatcherCore.hh"

#include <iostream>
#include <memory>

artdaq::DispatcherApp::DispatcherApp() = default;

// *******************************************************************
// *** The following methods implement the state machine operations.
// *******************************************************************

bool artdaq::DispatcherApp::do_initialize(fhicl::ParameterSet const& pset, uint64_t /*unused*/, uint64_t /*unused*/)
{
	report_string_ = "";

	//Dispatcher_ptr_.reset(nullptr);
	if (Dispatcher_ptr_ == nullptr)
	{
		Dispatcher_ptr_ = std::make_unique<DispatcherCore>();
	}
	external_request_status_ = Dispatcher_ptr_->initialize(pset);
	if (!external_request_status_)
	{
		report_string_ = "Error initializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}

	return external_request_status_;
}

bool artdaq::DispatcherApp::do_start(art::RunID id, uint64_t /*unused*/, uint64_t /*unused*/)
{
	report_string_ = "";
	external_request_status_ = Dispatcher_ptr_->start(id);
	if (!external_request_status_)
	{
		report_string_ = "Error starting ";
		report_string_.append(app_name + " ");
		report_string_.append("for run number ");
		report_string_.append(boost::lexical_cast<std::string>(id.run()));
		report_string_.append(".");
	}

	return external_request_status_;
}

bool artdaq::DispatcherApp::do_stop(uint64_t /*unused*/, uint64_t /*unused*/)
{
	report_string_ = "";
	external_request_status_ = Dispatcher_ptr_->stop();
	if (!external_request_status_)
	{
		report_string_ = "Error stopping ";
		report_string_.append(app_name + ".");
	}

	return external_request_status_;
}

bool artdaq::DispatcherApp::do_pause(uint64_t /*unused*/, uint64_t /*unused*/)
{
	report_string_ = "";
	external_request_status_ = Dispatcher_ptr_->pause();
	if (!external_request_status_)
	{
		report_string_ = "Error pausing ";
		report_string_.append(app_name + ".");
	}
	return external_request_status_;
}

bool artdaq::DispatcherApp::do_resume(uint64_t /*unused*/, uint64_t /*unused*/)
{
	report_string_ = "";
	external_request_status_ = Dispatcher_ptr_->resume();
	if (!external_request_status_)
	{
		report_string_ = "Error resuming ";
		report_string_.append(app_name + ".");
	}

	return external_request_status_;
}

bool artdaq::DispatcherApp::do_shutdown(uint64_t /*unused*/)
{
	report_string_ = "";
	external_request_status_ = Dispatcher_ptr_->shutdown();
	if (!external_request_status_)
	{
		report_string_ = "Error shutting down ";
		report_string_.append(app_name + ".");
	}

	return external_request_status_;
}

bool artdaq::DispatcherApp::do_soft_initialize(fhicl::ParameterSet const& /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
{
	return true;
}

bool artdaq::DispatcherApp::do_reinitialize(fhicl::ParameterSet const& /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
{
	return true;
}

std::string artdaq::DispatcherApp::report(std::string const& which) const
{
	std::string resultString;

	// if all that is requested is the latest state change result, return it
	if (which == "transition_status")
	{
		if (report_string_.length() > 0) { return report_string_; }

			return "Success";
		}

	//// if there is an outstanding report/message at the Commandable/Application
	//// level, prepend that
	//if (report_string_.length() > 0) {
	//  resultString.append("*** Overall status message:\r\n");
	//  resultString.append(report_string_ + "\r\n");
	//  resultString.append("*** Requested report response:\r\n");
	//}

	// pass the request to the DispatcherCore instance, if it's available
	if (Dispatcher_ptr_ != nullptr)
	{
		resultString.append(Dispatcher_ptr_->report(which));
	}
	else
	{
		resultString.append("This Dispatcher has not yet been initialized and ");
		resultString.append("therefore can not provide reporting.");
	}

	return resultString;
}

std::string artdaq::DispatcherApp::register_monitor(fhicl::ParameterSet const& info)
{
	TLOG(TLVL_DEBUG) << "DispatcherApp::register_monitor called with argument \"" << info.to_string() << "\"";

	if (Dispatcher_ptr_)
	{
		try
		{
			return Dispatcher_ptr_->register_monitor(info);
		}
		catch (...)
		{
			ExceptionHandler(ExceptionHandlerRethrow::no,
			                 "Error in call to DispatcherCore's register_monitor function");

			return "Error in artdaq::DispatcherApp::register_monitor: an exception was thrown in the call to DispatcherCore::register_monitor, possibly due to a problem with the argument";
		}
	}
	else
	{
		return "Error in artdaq::DispatcherApp::register_monitor: DispatcherCore object wasn't initialized";
	}
}

std::string artdaq::DispatcherApp::unregister_monitor(std::string const& label)
{
	TLOG(TLVL_DEBUG) << "DispatcherApp::unregister_monitor called with argument \"" << label << "\"";

	if (Dispatcher_ptr_)
	{
		try
		{
			return Dispatcher_ptr_->unregister_monitor(label);
		}
		catch (...)
		{
			ExceptionHandler(ExceptionHandlerRethrow::no,
			                 "Error in call to DispatcherCore's unregister_monitor function");

			return "Error in artdaq::DispatcherApp::unregister_monitor: an exception was thrown in the call to DispatcherCore::unregister_monitor, possibly due to a problem with the argument";
		}
	}
	else
	{
		return "Error in artdaq::DispatcherApp::unregister_monitor: DispatcherCore object wasn't initialized";
	}
}

bool artdaq::DispatcherApp::do_override_fragment_ids(uint64_t seqID, std::vector<uint32_t> frags)
{
	report_string_ = "";
	external_request_status_ = true;

	std::set<Fragment::fragment_id_t> frags_set;
	for (auto& f : frags)
	{
		if (!frags_set.count(f))
			frags_set.insert(f);
	}

	Dispatcher_ptr_->OverrideFragmentIDsForEvent(seqID, frags_set);

	return external_request_status_;
}

bool artdaq::DispatcherApp::do_update_default_fragment_ids(uint64_t seqID, std::vector<uint32_t> frags)
{
	report_string_ = "";
	external_request_status_ = true;
	std::set<Fragment::fragment_id_t> frags_set;
	for (auto& f : frags)
	{
		if (!frags_set.count(f))
			frags_set.insert(f);
	}

	Dispatcher_ptr_->SetDefaultFragmentIDs(frags_set, seqID);

	return external_request_status_;
}