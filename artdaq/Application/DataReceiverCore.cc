#include "canvas/Utilities/Exception.h"
#include "art/Framework/Art/artapp.h"

#define TRACE_NAME (app_name + "_DataReceiverCore").c_str() // include these 2 first -
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/Application/DataReceiverCore.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

#include <iomanip>

artdaq::DataReceiverCore::DataReceiverCore()
	: stop_requested_(false)
	, pause_requested_(false)
	, run_is_paused_(false)
{
	TLOG(TLVL_DEBUG) << "Constructor" ;
	metricMan = &metricMan_;
}

artdaq::DataReceiverCore::~DataReceiverCore()
{
	TLOG(TLVL_DEBUG) << "Destructor" ;
}

bool artdaq::DataReceiverCore::initializeDataReceiver(fhicl::ParameterSet const& pset, fhicl::ParameterSet const& data_pset, fhicl::ParameterSet const& metric_pset)
{
	// other parameters
	verbose_ = data_pset.get<bool>("verbose", true);

	if (metric_pset.is_empty())
	{
		TLOG(TLVL_INFO) << "No metric plugins appear to be defined" ;
	}
	try
	{
		metricMan_.initialize(metric_pset, app_name + "." + std::to_string(my_rank));
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
						 "Error loading metrics in DataReceiverCore::initialize()");
	}

	fhicl::ParameterSet tmp = pset;
	tmp.erase("daq");

	fhicl::ParameterSet data_tmp = data_pset;
	if (data_pset.has_key("expected_events_per_bunch"))
	{
		data_tmp.put<int>("expected_fragments_per_event", data_pset.get<int>("expected_events_per_bunch"));
	}

	if (data_pset.has_key("rank"))
	{
		if (my_rank >= 0 && data_pset.get<int>("rank") != my_rank) {
			TLOG(TLVL_WARNING) << "Rank specified at startup is different than rank specified at configure! Using rank received at configure!";
		}
		my_rank = data_pset.get<int>("rank");
	}
	if (my_rank == -1)
	{
		TLOG(TLVL_ERROR) << "Rank not specified at startup or in configuration! Aborting";
		exit(1);
	}

	event_store_ptr_.reset(new SharedMemoryEventManager(data_tmp, tmp));

	receiver_ptr_.reset(new artdaq::DataReceiverManager(data_tmp, event_store_ptr_));

	return true;
}

bool artdaq::DataReceiverCore::start(art::RunID id)
{
	logMessage_("Starting run " + boost::lexical_cast<std::string>(id.run()));
	stop_requested_.store(false);
	pause_requested_.store(false);
	run_is_paused_.store(false);
	metricMan_.do_start();
	event_store_ptr_->startRun(id.run());
	receiver_ptr_->start_threads();

	logMessage_("Completed the Start transition for run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	return true;
}

bool artdaq::DataReceiverCore::stop()
{
	logMessage_("Stopping run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()) +
	            ", subrun " + boost::lexical_cast<std::string>(event_store_ptr_->subrunID()));
	bool endSucceeded;
	int attemptsToEnd;
	receiver_ptr_->stop_threads();

	// 21-Jun-2013, KAB - the stop_requested_ variable must be set
	// before the flush lock so that the processFragments loop will
	// exit (after the timeout), the lock will be released (in the
	// processFragments method), and this method can continue.
	stop_requested_.store(true);

	if (!run_is_paused_.load())
	{
		endSucceeded = false;
		attemptsToEnd = 1;
		endSucceeded = event_store_ptr_->endSubrun();
		while (!endSucceeded && attemptsToEnd < 3)
		{
			++attemptsToEnd;
			TLOG(TLVL_DEBUG) << "Retrying EventStore::endSubrun()" ;
			endSucceeded = event_store_ptr_->endSubrun();
		}
		if (!endSucceeded)
		{
			TLOG(TLVL_ERROR)
				<< "EventStore::endSubrun in stop method failed after three tries." ;
		}
	}

	endSucceeded = false;
	attemptsToEnd = 1;
	endSucceeded = event_store_ptr_->endRun();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TLOG(TLVL_DEBUG) << "Retrying EventStore::endRun()" ;
		endSucceeded = event_store_ptr_->endRun();
	}
	if (!endSucceeded)
	{
		TLOG(TLVL_ERROR)
			<< "EventStore::endRun in stop method failed after three tries." ;
	}

	endSucceeded = false;
	attemptsToEnd = 1;
	TLOG(TLVL_DEBUG) << "stop: Calling EventStore::endOfData" ;
	endSucceeded = event_store_ptr_->endOfData();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TLOG(TLVL_DEBUG) << "Retrying EventStore::endOfData()" ;
		endSucceeded = event_store_ptr_->endOfData();
	}
	
	run_is_paused_.store(false);
	logMessage_("Completed the Stop transition for run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	return true;
}

bool artdaq::DataReceiverCore::pause()
{
	logMessage_("Pausing run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()) +
	            ", subrun " + boost::lexical_cast<std::string>(event_store_ptr_->subrunID()));
	pause_requested_.store(true);

	bool endSucceeded = false;
	int attemptsToEnd = 1;
	endSucceeded = event_store_ptr_->endSubrun();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TLOG(TLVL_DEBUG) << "Retrying EventStore::endSubrun()" ;
		endSucceeded = event_store_ptr_->endSubrun();
	}
	if (!endSucceeded)
	{
		TLOG(TLVL_ERROR)
			<< "EventStore::endSubrun in pause method failed after three tries." ;
	}

	run_is_paused_.store(true);
	logMessage_("Completed the Pause transition for run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	return true;
}

bool artdaq::DataReceiverCore::resume()
{
	logMessage_("Resuming run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	pause_requested_.store(false);
	metricMan_.do_start();
	event_store_ptr_->startSubrun();
	run_is_paused_.store(false);
	logMessage_("Completed the Resume transition for run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	return true;
}

bool artdaq::DataReceiverCore::shutdown()
{
	logMessage_("Starting Shutdown transition");

	/* We don't care about flushing data here.  The only way to transition to the
	   shutdown state is from a state where there is no data taking.  All we have
	   to do is signal the art input module that we're done taking data so that
	   it can wrap up whatever it needs to do. */

	TLOG(TLVL_DEBUG) << "shutdown: Shutting down DataReceiverManager" ;
	receiver_ptr_.reset(nullptr);

	bool endSucceeded = false;
	int attemptsToEnd = 1;
	TLOG(TLVL_DEBUG) << "shutdown: Calling EventStore::endOfData" ;
	endSucceeded = event_store_ptr_->endOfData();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TLOG(TLVL_DEBUG) << "Retrying EventStore::endOfData()" ;
		endSucceeded = event_store_ptr_->endOfData();
	}

	TLOG(TLVL_DEBUG) << "shutdown: Shutting down SharedMemoryEventManager" ;
	event_store_ptr_.reset();

	TLOG(TLVL_DEBUG) << "shutdown: Shutting down MetricManager" ;
	metricMan_.shutdown();

	TLOG(TLVL_DEBUG) << "shutdown: Complete" ;
	logMessage_("Completed Shutdown transition");
	return endSucceeded;
}

bool artdaq::DataReceiverCore::soft_initialize(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG) << "soft_initialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." ;
	return true;
}

bool artdaq::DataReceiverCore::reinitialize(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG) << "reinitialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." ;
	event_store_ptr_ = nullptr;
	return initialize(pset);
}

std::string artdaq::DataReceiverCore::report(std::string const& which) const
{
	if (which == "incomplete_event_count")
	{
		if (event_store_ptr_ != nullptr)
		{
			return boost::lexical_cast<std::string>(event_store_ptr_->GetIncompleteEventCount());
		}
		else
		{
			return "-1";
		}
	}
	if (which == "event_count")
	{
		if (receiver_ptr_ != nullptr)
			return boost::lexical_cast<std::string>(receiver_ptr_->GetReceivedFragmentCount()->count());

		return "0";
	}

	// lots of cool stuff that we can do here
	// - report on the number of fragments received and the number
	//   of events built (in the current or previous run
	// - report on the number of incomplete events in the EventStore
	//   (if running)
	std::string tmpString;
	if (event_store_ptr_ != nullptr)	tmpString.append(app_name + " run number = " + boost::lexical_cast<std::string>(event_store_ptr_->runID()) + ".\n");
	tmpString.append("Command \"" + which + "\" is not currently supported.");
	return tmpString;
}

void artdaq::DataReceiverCore::logMessage_(std::string const& text)
{
	if (verbose_)
	{
		TLOG(TLVL_INFO) << text ;
	}
	else
	{
		TLOG(TLVL_DEBUG) << text ;
	}
}
