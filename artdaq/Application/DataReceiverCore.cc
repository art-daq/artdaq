#include "canvas/Utilities/Exception.h"
#include "art/Framework/Art/artapp.h"

#include "artdaq-core/Core/SimpleMemoryReader.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "artdaq/Application/DataReceiverCore.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME "DataReceiverCore"

#include <iomanip>

artdaq::DataReceiverCore::DataReceiverCore(int rank, std::string name)
	: name_(name)
	, stop_requested_(false)
	, pause_requested_(false)
	, run_is_paused_(false)
{
	TLOG_DEBUG(name_) << "Constructor" << TLOG_ENDL;
	my_rank = rank;
	metricMan = &metricMan_;
}

artdaq::DataReceiverCore::~DataReceiverCore()
{
	TLOG_DEBUG(name_) << "Destructor" << TLOG_ENDL;
}

bool artdaq::DataReceiverCore::initializeDataReceiver(fhicl::ParameterSet const& pset, fhicl::ParameterSet const& metric_pset)
{
	// other parameters
	verbose_ = pset.get<bool>("verbose", false);

	if (metric_pset.is_empty())
	{
		TLOG_INFO(name_) << "No metric plugins appear to be defined" << TLOG_ENDL;
	}
	try
	{
		metricMan_.initialize(metric_pset, name_ + "." + std::to_string(my_rank));
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no,
						 "Error loading metrics in DataReceiverCore::initialize()");
	}

	fhicl::ParameterSet tmp = pset;
	tmp.erase("daq");
	
	event_store_ptr_ = std::shared_ptr<SharedMemoryEventManager>(new SharedMemoryEventManager(pset, tmp));

	receiver_ptr_.reset(new artdaq::DataReceiverManager(pset, event_store_ptr_));

	return true;
}

bool artdaq::DataReceiverCore::start(art::RunID id)
{
	stop_requested_.store(false);
	pause_requested_.store(false);
	run_is_paused_.store(false);
	metricMan_.do_start();
	event_store_ptr_->startRun(id.run());
	receiver_ptr_->start_threads();

	logMessage_("Started run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	return true;
}

bool artdaq::DataReceiverCore::stop()
{
	logMessage_("Stopping run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()) +
				", subrun " + boost::lexical_cast<std::string>(event_store_ptr_->subrunID()));
	bool endSucceeded;
	int attemptsToEnd;

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
			TLOG_DEBUG(name_) << "Retrying EventStore::endSubrun()" << TLOG_ENDL;
			endSucceeded = event_store_ptr_->endSubrun();
		}
		if (!endSucceeded)
		{
			TLOG_ERROR(name_)
				<< "EventStore::endSubrun in stop method failed after three tries." << TLOG_ENDL;
		}
	}

	endSucceeded = false;
	attemptsToEnd = 1;
	endSucceeded = event_store_ptr_->endRun();
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TLOG_DEBUG(name_) << "Retrying EventStore::endRun()" << TLOG_ENDL;
		endSucceeded = event_store_ptr_->endRun();
	}
	if (!endSucceeded)
	{
		TLOG_ERROR(name_)
			<< "EventStore::endRun in stop method failed after three tries." << TLOG_ENDL;
	}

	run_is_paused_.store(false);
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
		TLOG_DEBUG(name_) << "Retrying EventStore::endSubrun()" << TLOG_ENDL;
		endSucceeded = event_store_ptr_->endSubrun();
	}
	if (!endSucceeded)
	{
		TLOG_ERROR(name_)
			<< "EventStore::endSubrun in pause method failed after three tries." << TLOG_ENDL;
	}

	run_is_paused_.store(true);
	return true;
}

bool artdaq::DataReceiverCore::resume()
{
	logMessage_("Resuming run " + boost::lexical_cast<std::string>(event_store_ptr_->runID()));
	pause_requested_.store(false);
	metricMan_.do_start();
	event_store_ptr_->startSubrun();
	run_is_paused_.store(false);
	return true;
}

bool artdaq::DataReceiverCore::shutdown()
{
	/* We don't care about flushing data here.  The only way to transition to the
	   shutdown state is from a state where there is no data taking.  All we have
	   to do is signal the art input module that we're done taking data so that
	   it can wrap up whatever it needs to do. */
	bool endSucceeded = false;
	int attemptsToEnd = 1;
	TLOG_ARB(4, "DataReceiverCore") << "shutdown: Calling EventStore::endOfData" << TLOG_ENDL;
	std::vector<int> readerReturnValues;
	endSucceeded = event_store_ptr_->endOfData(readerReturnValues);
	while (!endSucceeded && attemptsToEnd < 3)
	{
		++attemptsToEnd;
		TLOG_DEBUG(name_) << "Retrying EventStore::endOfData()" << TLOG_ENDL;
		endSucceeded = event_store_ptr_->endOfData(readerReturnValues);
	}
	TLOG_ARB(4, "DataReceiverCore") << "shutdown: Shutting down MetricManager" << TLOG_ENDL;
	metricMan_.shutdown();
	TLOG_ARB(4, "DataReceiverCore") << "shutdown: Complete" << TLOG_ENDL;
	return endSucceeded;
}

bool artdaq::DataReceiverCore::soft_initialize(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "soft_initialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	return true;
}

bool artdaq::DataReceiverCore::reinitialize(fhicl::ParameterSet const& pset)
{
	TLOG_DEBUG(name_) << "reinitialize method called with DAQ "
		<< "ParameterSet = \"" << pset.to_string()
		<< "\"." << TLOG_ENDL;
	event_store_ptr_ = nullptr;
	return initialize(pset);
}

std::string artdaq::DataReceiverCore::report(std::string const& which) const
{
	if (which == "incomplete_event_count")
	{
		if (event_store_ptr_ != nullptr)
		{
			return boost::lexical_cast<std::string>(event_store_ptr_->GetOpenEventCount());
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
	if (event_store_ptr_ != nullptr)	tmpString.append(name_ + " run number = " + boost::lexical_cast<std::string>(event_store_ptr_->runID()) + ".\n");
	tmpString.append("Command \"" + which + "\" is not currently supported.");
	return tmpString;
}

void artdaq::DataReceiverCore::logMessage_(std::string const& text)
{
	if (verbose_)
	{
		TLOG_INFO(name_) << text << TLOG_ENDL;
	}
	else
	{
		TLOG_DEBUG(name_) << text << TLOG_ENDL;
	}
}
