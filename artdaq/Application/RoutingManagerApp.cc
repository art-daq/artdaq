#define TRACE_NAME "RoutingManagerApp"

#include <memory>

#include "artdaq/Application/RoutingManagerApp.hh"

/**
* Default constructor.
*/
artdaq::RoutingManagerApp::RoutingManagerApp() = default;

// *******************************************************************
// *** The following methods implement the state machine operations.
// *******************************************************************

bool artdaq::RoutingManagerApp::do_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = true;

	// in the following block, we first destroy the existing RoutingManager
	// instance, then create a new one.  Doing it in one step does not
	// produce the desired result since that creates a new instance and
	// then deletes the old one, and we need the opposite order.
	routing_manager_ptr_.reset(nullptr);
	routing_manager_ptr_ = std::make_unique<RoutingManagerCore>();
	external_request_status_ = routing_manager_ptr_->initialize(pset, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error initializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}

	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = routing_manager_ptr_->start(id, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error starting ";
		report_string_.append(app_name + " ");
		report_string_.append("for run number ");
		report_string_.append(boost::lexical_cast<std::string>(id.run()));
		report_string_.append(", timeout ");
		report_string_.append(boost::lexical_cast<std::string>(timeout));
		report_string_.append(", timestamp ");
		report_string_.append(boost::lexical_cast<std::string>(timestamp));
		report_string_.append(".");
	}

	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 8 MB
	try
	{
		routing_manager_thread_ = boost::thread(attrs, boost::bind(&RoutingManagerCore::process_event_table, routing_manager_ptr_.get()));
		char tname[16];                                             // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-Routing", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                            // assure term. snprintf is not too evil :)
		auto handle = routing_manager_thread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (const boost::exception& e)
	{
		TLOG(TLVL_ERROR) << "Caught boost::exception starting RoutingManagerCore thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Caught boost::exception starting RoutingManagerCore thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(5);
	}

	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_stop(uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = routing_manager_ptr_->stop(timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error stopping ";
		report_string_.append(app_name + ".");
		return false;
	}

	if (routing_manager_thread_.joinable())
	{
		routing_manager_thread_.join();
	}

	TLOG(TLVL_DEBUG + 32, app_name + "App") << "do_stop(uint64_t, uint64_t): "
	                             << "Number of table entries sent = " << routing_manager_ptr_->get_update_count()
	                             << ".";

	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_pause(uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = routing_manager_ptr_->pause(timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error pausing ";
		report_string_.append(app_name + ".");
	}
	if (routing_manager_thread_.joinable())
	{
		routing_manager_thread_.join();
	}

	TLOG(TLVL_DEBUG + 32, app_name + "App") << "do_pause(uint64_t, uint64_t): "
	                             << "Number of table entries sent = " << routing_manager_ptr_->get_update_count()
	                             << ".";

	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_resume(uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = routing_manager_ptr_->resume(timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error resuming ";
		report_string_.append(app_name + ".");
	}

	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 8000 KB

	TLOG(TLVL_INFO) << "Starting Routing Manager thread";
	try
	{
		routing_manager_thread_ = boost::thread(attrs, boost::bind(&RoutingManagerCore::process_event_table, routing_manager_ptr_.get()));
		char tname[16];                                             // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-Routing", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                            // assure term. snprintf is not too evil :)
		auto handle = routing_manager_thread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (boost::exception const& e)
	{
		TLOG(TLVL_ERROR) << "Exception encountered starting Routing Manager thread: " << boost::diagnostic_information(e) << ", errno=" << errno;
		std::cerr << "Exception encountered starting Routing Manager thread: " << boost::diagnostic_information(e) << ", errno=" << errno << std::endl;
		exit(3);
	}
	TLOG(TLVL_INFO) << "Started Routing Manager thread";

	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_shutdown(uint64_t timeout)
{
	report_string_ = "";
	external_request_status_ = routing_manager_ptr_->shutdown(timeout);
	if (!external_request_status_)
	{
		report_string_ = "Error shutting down ";
		report_string_.append(app_name + ".");
	}
	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = routing_manager_ptr_->soft_initialize(pset, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error soft-initializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}
	return external_request_status_;
}

bool artdaq::RoutingManagerApp::do_reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	external_request_status_ = routing_manager_ptr_->reinitialize(pset, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error reinitializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}
	return external_request_status_;
}

void artdaq::RoutingManagerApp::BootedEnter()
{
	TLOG(TLVL_DEBUG + 32, app_name + "App") << "Booted state entry action called.";

	// the destruction of any existing RoutingManagerCore has to happen in the
	// Booted Entry action rather than the Initialized Exit action because the
	// Initialized Exit action is only called after the "init" transition guard
	// condition is executed.
	routing_manager_ptr_.reset(nullptr);
}

std::string artdaq::RoutingManagerApp::report(std::string const& which) const
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

	// pass the request to the RoutingManagerCore instance, if it's available
	if (routing_manager_ptr_ != nullptr)
	{
		resultString.append(routing_manager_ptr_->report(which));
	}
	else
	{
		resultString.append("This RoutingManager has not yet been initialized and ");
		resultString.append("therefore can not provide reporting.");
	}

	return resultString;
}
