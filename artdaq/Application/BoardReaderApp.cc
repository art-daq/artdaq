#include "artdaq/Application/BoardReaderApp.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_BoardReaderApp").c_str()  // NOLINT

#include <memory>
#include <string>

artdaq::BoardReaderApp::BoardReaderApp()
    : fragment_receiver_ptr_(nullptr)
{
}

// *******************************************************************
// *** The following methods implement the state machine operations.
// *******************************************************************

bool artdaq::BoardReaderApp::do_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = true;

	// in the following block, we first destroy the existing BoardReader
	// instance, then create a new one.  Doing it in one step does not
	// produce the desired result since that creates a new instance and
	// then deletes the old one, and we need the opposite order.
	TLOG(TLVL_DEBUG + 32) << "Initializing first deleting old instance " << static_cast<void*>(fragment_receiver_ptr_.get());
	fragment_receiver_ptr_.reset(nullptr);
	fragment_receiver_ptr_ = std::make_unique<BoardReaderCore>(*this);
	TLOG(TLVL_DEBUG + 32) << "Initializing new BoardReaderCore at " << static_cast<void*>(fragment_receiver_ptr_.get()) << " with pset " << pset.to_string();
	external_request_status_ = fragment_receiver_ptr_->initialize(pset, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error initializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}

	TLOG(TLVL_DEBUG + 32) << "do_initialize(fhicl::ParameterSet, uint64_t, uint64_t): "
	                 << "Done initializing.";
	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_start(art::RunID id, uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	if (timeout == 0)
	{
		timeout = 3600;  // seconds
	}
	fragment_receiver_ptr_->SetStartTransitionTimeout(timeout);
	external_request_status_ = true;

	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 8 MB
	try
	{
		fragment_output_thread_ = boost::thread(attrs, boost::bind(&BoardReaderCore::send_fragments, fragment_receiver_ptr_.get()));
		char tname[16];                                                // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-FragOutput", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                               // assure term. snprintf is not too evil :)
		auto handle = fragment_output_thread_.native_handle();
		pthread_setname_np(handle, tname);
		fragment_input_thread_ = boost::thread(attrs, boost::bind(&BoardReaderCore::receive_fragments, fragment_receiver_ptr_.get()));
		snprintf(tname, sizeof(tname) - 1, "%d-FragInput", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                              // assure term. snprintf is not too evil :)
		handle = fragment_input_thread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (const boost::exception& e)
	{
		std::stringstream exception_string;
		exception_string << "Caught boost::exception starting Fragment Processing threads: " << boost::diagnostic_information(e) << ", errno=" << errno;

		ExceptionHandler(ExceptionHandlerRethrow::yes, exception_string.str());
	}

	auto start_wait = std::chrono::steady_clock::now();
	while (!fragment_receiver_ptr_->GetSenderThreadActive() || !fragment_receiver_ptr_->GetReceiverThreadActive())
	{
		if (TimeUtils::GetElapsedTime(start_wait) > timeout)
		{
			TLOG(TLVL_ERROR) << "Timeout occurred waiting for BoardReaderCore threads to start. Timeout = " << timeout << " s, Time waited = " << TimeUtils::GetElapsedTime(start_wait) << " s,"
			                 << " Receiver ready: " << std::boolalpha << fragment_receiver_ptr_->GetReceiverThreadActive() << ", Sender ready: " << fragment_receiver_ptr_->GetSenderThreadActive();
			external_request_status_ = false;
			break;
		}
		usleep(10000);
	}

	// Only if starting threads successful
	if (external_request_status_)
	{
		external_request_status_ = fragment_receiver_ptr_->start(id, timeout, timestamp);
	}

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

	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_stop(uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = fragment_receiver_ptr_->stop(timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error stopping ";
		report_string_.append(app_name + ".");
		return false;
	}
	if (fragment_input_thread_.joinable())
	{
		TLOG(TLVL_DEBUG + 32) << "Joining fragment input (Generator) thread";
		fragment_input_thread_.join();
	}
	if (fragment_output_thread_.joinable())
	{
		TLOG(TLVL_DEBUG + 32) << "Joining fragment output (Sender) thread";
		fragment_output_thread_.join();
	}

	TLOG(TLVL_DEBUG + 32) << "BoardReader Stopped. Getting run statistics";
	int number_of_fragments_sent = -1;
	if (fragment_receiver_ptr_)
	{
		number_of_fragments_sent = fragment_receiver_ptr_->GetFragmentsProcessed();
	}
	TLOG(TLVL_DEBUG + 32) << "do_stop(uint64_t, uint64_t): "
	                 << "Number of fragments sent = " << number_of_fragments_sent
	                 << ".";

	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_pause(uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = fragment_receiver_ptr_->pause(timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error pausing ";
		report_string_.append(app_name + ".");
	}

	if (fragment_input_thread_.joinable()) fragment_input_thread_.join();
	if (fragment_output_thread_.joinable()) fragment_output_thread_.join();
	int number_of_fragments_sent = fragment_receiver_ptr_->GetFragmentsProcessed();
	TLOG(TLVL_DEBUG + 32) << "do_pause(uint64_t, uint64_t): "
	                 << "Number of fragments sent = " << number_of_fragments_sent
	                 << ".";

	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_resume(uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	if (timeout == 0)
	{
		timeout = 3600;  // seconds
	}
	external_request_status_ = true;

	boost::thread::attributes attrs;
	attrs.set_stack_size(4096 * 2000);  // 8 MB
	try
	{
		fragment_output_thread_ = boost::thread(attrs, boost::bind(&BoardReaderCore::send_fragments, fragment_receiver_ptr_.get()));
		char tname[16];                                                // Size 16 - see man page pthread_setname_np(3) and/or prctl(2)
		snprintf(tname, sizeof(tname) - 1, "%d-FragOutput", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                               // assure term. snprintf is not too evil :)
		auto handle = fragment_output_thread_.native_handle();
		pthread_setname_np(handle, tname);
		fragment_input_thread_ = boost::thread(attrs, boost::bind(&BoardReaderCore::receive_fragments, fragment_receiver_ptr_.get()));

		snprintf(tname, sizeof(tname) - 1, "%d-FragInput", my_rank);  // NOLINT
		tname[sizeof(tname) - 1] = '\0';                              // assure term. snprintf is not too evil :)
		handle = fragment_input_thread_.native_handle();
		pthread_setname_np(handle, tname);
	}
	catch (const boost::exception& e)
	{
		std::stringstream exception_string;
		exception_string << "Caught boost::exception starting Fragment Processing threads: " << boost::diagnostic_information(e) << ", errno=" << errno;

		ExceptionHandler(ExceptionHandlerRethrow::yes, exception_string.str());
	}

	auto start_wait = std::chrono::steady_clock::now();
	while (!fragment_receiver_ptr_->GetSenderThreadActive() || !fragment_receiver_ptr_->GetReceiverThreadActive())
	{
		if (TimeUtils::GetElapsedTimeMicroseconds(start_wait) > timeout * 1000000)
		{
			TLOG(TLVL_ERROR) << "Timeout occurred waiting for BoardReaderCore threads to start. Timeout = " << timeout << " s, Time waited = " << TimeUtils::GetElapsedTime(start_wait) << " s,"
			                 << " Receiver ready: " << std::boolalpha << fragment_receiver_ptr_->GetReceiverThreadActive() << ", Sender ready: " << fragment_receiver_ptr_->GetSenderThreadActive();
			external_request_status_ = false;
			break;
		}
		usleep(10000);
	}

	// Only if starting threads successful
	if (external_request_status_)
	{
		external_request_status_ = fragment_receiver_ptr_->resume(timeout, timestamp);
	}
	if (!external_request_status_)
	{
		report_string_ = "Error resuming ";
		report_string_.append(app_name + ".");
	}
	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_shutdown(uint64_t timeout)
{
	report_string_ = "";
	external_request_status_ = fragment_receiver_ptr_->shutdown(timeout);
	// 02-Jun-2018, ELF & KAB: it's very, very unlikely that the following call is needed,
	// but just in case...
	if (fragment_input_thread_.joinable()) fragment_input_thread_.join();
	if (fragment_output_thread_.joinable()) fragment_output_thread_.join();
	if (!external_request_status_)
	{
		report_string_ = "Error shutting down ";
		report_string_.append(app_name + ".");
	}
	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_soft_initialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	report_string_ = "";
	external_request_status_ = fragment_receiver_ptr_->soft_initialize(pset, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error soft-initializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}
	return external_request_status_;
}

bool artdaq::BoardReaderApp::do_reinitialize(fhicl::ParameterSet const& pset, uint64_t timeout, uint64_t timestamp)
{
	external_request_status_ = fragment_receiver_ptr_->reinitialize(pset, timeout, timestamp);
	if (!external_request_status_)
	{
		report_string_ = "Error reinitializing ";
		report_string_.append(app_name + " ");
		report_string_.append("with ParameterSet = \"" + pset.to_string() + "\".");
	}
	return external_request_status_;
}

void artdaq::BoardReaderApp::BootedEnter()
{
	TLOG(TLVL_DEBUG + 32) << "Booted state entry action called.";

	// the destruction of any existing BoardReaderCore has to happen in the
	// Booted Entry action rather than the Initialized Exit action because the
	// Initialized Exit action is only called after the "init" transition guard
	// condition is executed.
	fragment_receiver_ptr_.reset(nullptr);
}

bool artdaq::BoardReaderApp::do_meta_command(std::string const& command, std::string const& arg)
{
	external_request_status_ = fragment_receiver_ptr_->metaCommand(command, arg);
	if (!external_request_status_)
	{
		report_string_ = "Error running meta-command on ";
		report_string_.append(app_name + " ");
		report_string_.append("with command = \"" + command + "\", arg = \"" + arg + "\".");
	}
	return external_request_status_;
}

std::string artdaq::BoardReaderApp::report(std::string const& which) const
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

	// pass the request to the BoardReaderCore instance, if it's available
	if (fragment_receiver_ptr_ != nullptr)
	{
		resultString.append(fragment_receiver_ptr_->report(which));
	}
	else
	{
		resultString.append("This BoardReader has not yet been initialized and ");
		resultString.append("therefore can not provide reporting.");
	}

	return resultString;
}
