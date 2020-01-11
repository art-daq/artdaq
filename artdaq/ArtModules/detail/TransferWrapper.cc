#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME "TransferWrapper"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/ArtModules/detail/TransferWrapper.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/ExternalComms/MakeCommanderPlugin.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"

#include "cetlib/BasicPluginFactory.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"

#include <TBufferFile.h>

#include <csignal>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

namespace {
volatile std::sig_atomic_t gSignalStatus = 0;  ///< Stores singal from signal handler
}

/**
 * \brief Handle a Unix signal
 * \param signal Signal to handle
 */
void signal_handler(int signal)
{
	gSignalStatus = signal;
}

artdaq::TransferWrapper::TransferWrapper(const fhicl::ParameterSet& pset)
    : timeoutInUsecs_(pset.get<std::size_t>("timeoutInUsecs", 100000))
    , transfer_(nullptr)
    , commander_(nullptr)
    , pset_(pset)
    , dispatcherHost_(pset.get<std::string>("dispatcherHost", "localhost"))
    , dispatcherPort_(pset.get<std::string>("dispatcherPort", "5266"))
    , serverUrl_(pset.get<std::string>("server_url", "http://" + dispatcherHost_ + ":" + dispatcherPort_ + "/RPC2"))
    , maxEventsBeforeInit_(pset.get<std::size_t>("maxEventsBeforeInit", 5))
    , allowedFragmentTypes_(pset.get<std::vector<int>>("allowedFragmentTypes", {226, 227, 229}))
    , runningStateTimeout_(pset.get<double>("dispatcherConnectTimeout", 0))
    , runningStateInterval_us_(pset.get<size_t>("dispatcherConnectRetryInterval_us", 1000000))
    , quitOnFragmentIntegrityProblem_(pset.get<bool>("quitOnFragmentIntegrityProblem", true))
    , multi_run_mode_(pset.get<bool>("allowMultipleRuns", false))
    , monitorRegistered_(false)
{
	std::signal(SIGINT, signal_handler);

	try
	{
		if (metricMan)
		{
			metricMan->initialize(pset.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), "Online Monitor");
			metricMan->do_start();
		}
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::no, "TransferWrapper: could not configure metrics");
	}

	// Clamp possible values
	if (runningStateInterval_us_ < 1000)
	{
		TLOG(TLVL_WARNING) << "Invalid value " << runningStateInterval_us_ << " us detected for dispatcherConnectRetryInterval_us. Setting to 1000 us";
		runningStateInterval_us_ = 1000;
	}
	if (runningStateInterval_us_ > 30000000)
	{
		TLOG(TLVL_WARNING) << "Invalid value " << runningStateInterval_us_ << " us detected for dispatcherConnectRetryInterval_us. Setting to 30,000,000 us";
		runningStateInterval_us_ = 30000000;
	}

	fhicl::ParameterSet new_pset(pset);
	if (!new_pset.has_key("server_url"))
	{
		new_pset.put<std::string>("server_url", serverUrl_);
	}

	artdaq::Commandable c;
	commander_ = MakeCommanderPlugin(new_pset, c);
}

void artdaq::TransferWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg)
{
	std::unique_ptr<artdaq::Fragment> fragmentPtr;
	bool receivedFragment = false;
	static bool initialized = false;
	static size_t fragments_received = 0;

	while (true && !gSignalStatus)
	{
		receivedFragment = false;
		fragmentPtr = std::make_unique<artdaq::Fragment>();

		while (!receivedFragment)
		{
			if (gSignalStatus)
			{
				TLOG(TLVL_INFO) << "Ctrl-C appears to have been hit";
				unregisterMonitor();
				return;
			}
			if (!monitorRegistered_)
			{
				registerMonitor();
				if (!monitorRegistered_) return;
			}

			try
			{
				auto result = transfer_->receiveFragment(*fragmentPtr, timeoutInUsecs_);

				if (result >= artdaq::TransferInterface::RECV_SUCCESS)
				{
					receivedFragment = true;
					fragments_received++;

					static size_t cntr = 0;
					auto mod = ++cntr % 10;
					auto suffix = "-th";
					if (mod == 1) suffix = "-st";
					if (mod == 2) suffix = "-nd";
					if (mod == 3) suffix = "-rd";
					TLOG(TLVL_INFO) << "Received " << cntr << suffix << " event, "
					                << "seqID == " << fragmentPtr->sequenceID()
					                << ", type == " << fragmentPtr->typeString();
					continue;
				}
				else if (result == artdaq::TransferInterface::DATA_END)
				{
					TLOG(TLVL_ERROR) << "Transfer Plugin disconnected or other unrecoverable error. Shutting down.";
					unregisterMonitor();
					initialized = false;
					continue;
				}
				else
				{
					// 02-Jun-2018, KAB: added status/result printout
					// to-do: add another else clause that explicitly checks for RECV_TIMEOUT
					TLOG(TLVL_WARNING) << "Timeout occurred in call to transfer_->receiveFragmentFrom; will try again"
					                   << ", status = " << result;
				}
			}
			catch (...)
			{
				ExceptionHandler(ExceptionHandlerRethrow::yes,
				                 "Problem receiving data in TransferWrapper::receiveMessage");
			}
		}

		if (fragmentPtr->type() == artdaq::Fragment::EndOfDataFragmentType)
		{
			//if (monitorRegistered_)
			//{
			//	unregisterMonitor();
			//}
			if (multi_run_mode_)
			{
				unregisterMonitor();
				initialized = false;
				continue;
			}
			else
			{
				return;
			}
		}

		try
		{
			extractTBufferFile(*fragmentPtr, msg);
		}
		catch (...)
		{
			ExceptionHandler(quitOnFragmentIntegrityProblem_ ? ExceptionHandlerRethrow::yes : ExceptionHandlerRethrow::no,
			                 "Problem extracting TBufferFile from artdaq::Fragment in TransferWrapper::receiveMessage");
		}

		checkIntegrity(*fragmentPtr);

		if (initialized || fragmentPtr->type() == artdaq::Fragment::InitFragmentType)
		{
			initialized = true;
			break;
		}
		else
		{
			receivedFragment = false;

			if (fragments_received > maxEventsBeforeInit_)
			{
				throw cet::exception("TransferWrapper") << "First " << maxEventsBeforeInit_ << " events received did not include the \"Init\" event containing necessary info for art; exiting...";
			}
		}
	}
}

void artdaq::TransferWrapper::extractTBufferFile(const artdaq::Fragment& fragment,
                                                 std::unique_ptr<TBufferFile>& tbuffer)
{
	const artdaq::NetMonHeader* header = fragment.metadata<artdaq::NetMonHeader>();
	char* buffer = (char*)malloc(header->data_length);
	memcpy(buffer, fragment.dataBeginBytes(), header->data_length);

	// TBufferFile takes ownership of the contents of memory passed to it
	tbuffer.reset(new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0));
}

void artdaq::TransferWrapper::checkIntegrity(const artdaq::Fragment& fragment) const
{
	const size_t artdaqheader = artdaq::detail::RawFragmentHeader::num_words() *
	                            sizeof(artdaq::detail::RawFragmentHeader::RawDataType);
	const size_t payload = static_cast<size_t>(fragment.dataEndBytes() - fragment.dataBeginBytes());
	const size_t metadata = sizeof(artdaq::NetMonHeader);
	const size_t totalsize = fragment.sizeBytes();

	const size_t type = static_cast<size_t>(fragment.type());

	if (totalsize != artdaqheader + metadata + payload)
	{
		std::stringstream errmsg;
		errmsg << "Error: artdaq fragment of type " << fragment.typeString() << ", sequence ID " << fragment.sequenceID() << " has internally inconsistent measures of its size, signalling data corruption: in bytes,"
		       << " total size = " << totalsize << ", artdaq fragment header = " << artdaqheader << ", metadata = " << metadata << ", payload = " << payload;

		TLOG(TLVL_ERROR) << errmsg.str();

		if (quitOnFragmentIntegrityProblem_)
		{
			throw cet::exception("TransferWrapper") << errmsg.str();
		}
		else
		{
			return;
		}
	}

	auto findloc = std::find(allowedFragmentTypes_.begin(), allowedFragmentTypes_.end(), static_cast<int>(type));

	if (findloc == allowedFragmentTypes_.end())
	{
		std::stringstream errmsg;
		errmsg << "Error: artdaq fragment appears to have type "
		       << type << ", not found in the allowed fragment types list";

		TLOG(TLVL_ERROR) << errmsg.str();
		if (quitOnFragmentIntegrityProblem_)
		{
			throw cet::exception("TransferWrapper") << errmsg.str();
		}
		else
		{
			return;
		}
	}
}

void artdaq::TransferWrapper::registerMonitor()
{
	try
	{
		transfer_.reset(nullptr);
		transfer_ = MakeTransferPlugin(pset_, "transfer_plugin", TransferInterface::Role::kReceive);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::yes,
		                 "TransferWrapper: failure in call to MakeTransferPlugin");
	}

	auto start = std::chrono::steady_clock::now();
	auto sts = getDispatcherStatus();
	while (sts != "Running" && (runningStateTimeout_ == 0 || TimeUtils::GetElapsedTime(start) < runningStateTimeout_))
	{
		TLOG(TLVL_DEBUG) << "Dispatcher state: " << sts;
		if (gSignalStatus)
		{
			TLOG(TLVL_INFO) << "Ctrl-C appears to have been hit";
			return;
		}
		TLOG(TLVL_INFO) << "Waited " << std::fixed << std::setprecision(2) << TimeUtils::GetElapsedTime(start) << " s / " << runningStateTimeout_ << " s for Dispatcher to enter the Running state";
		usleep(runningStateInterval_us_);
		sts = getDispatcherStatus();
	}
	if (sts != "Running") return;

	auto dispatcherConfig = pset_.get<fhicl::ParameterSet>("dispatcher_config");

	int retry = 3;

	while (retry > 0)
	{
		TLOG(TLVL_INFO) << "Attempting to register this monitor (\"" << transfer_->uniqueLabel()
		                << "\") with the dispatcher aggregator";

		auto status = commander_->send_register_monitor(dispatcherConfig.to_string());

		TLOG(TLVL_INFO) << "Response from dispatcher is \"" << status << "\"";

		if (status == "Success")
		{
			monitorRegistered_ = true;
			break;
		}
		else
		{
			TLOG(TLVL_WARNING) << "Error in TransferWrapper: attempt to register with dispatcher did not result in the \"Success\" response";
			usleep(100000);
		}
		retry--;
	}
}

void artdaq::TransferWrapper::unregisterMonitor()
{
	if (!monitorRegistered_)
	{
		TLOG(TLVL_WARNING) << "The function to unregister the monitor was called, but the monitor doesn't appear to be registered";
		return;
	}

	std::string sts = getDispatcherStatus();

	if (sts == "") return;

	if (sts != "Running" && sts != "Ready")
	{
		TLOG(TLVL_WARNING) << "The Dispatcher is not in the Running or Ready state, will not attempt to unregister";
		return;
	}

	int retry = 3;
	while (retry > 0)
	{
		TLOG(TLVL_INFO) << "Requesting that this monitor (" << transfer_->uniqueLabel()
		                << ") be unregistered from the dispatcher aggregator";

		auto status = commander_->send_unregister_monitor(transfer_->uniqueLabel());

		TLOG(TLVL_INFO) << "Response from dispatcher is \"" << status << "\"";

		if (status == "Success")
		{
			break;
		}
		else if (status == "busy")
		{}
		else
		{
			TLOG(TLVL_WARNING) << "The Dispatcher returned status " << status << " when attempting to unregister this monitor!";
			//throw cet::exception("TransferWrapper") << "Error in TransferWrapper: attempt to unregister with dispatcher did not result in the \"Success\" response";
		}
		retry--;
		usleep(500000);
	}
	monitorRegistered_ = false;
}

std::string artdaq::TransferWrapper::getDispatcherStatus()
{
	try
	{
		return commander_->send_status();
	}
	catch (std::exception const& ex)
	{
		TLOG(TLVL_WARNING) << "An exception was thrown trying to collect the Dispatcher's status. Most likely cause is the application is no longer running.";
		return "";
	}
}

artdaq::TransferWrapper::~TransferWrapper()
{
	if (monitorRegistered_)
	{
		try
		{
			unregisterMonitor();
		}
		catch (...)
		{
			ExceptionHandler(ExceptionHandlerRethrow::no,
			                 "An exception occurred when trying to unregister monitor during TransferWrapper's destruction");
		}
	}
	artdaq::Globals::CleanUpGlobals();
}
