#include "artdaq/TransferPlugins/TransferWrapper.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq-core/Data/Fragment.hh"

#include "cetlib/BasicPluginFactory.h"
#include "fhiclcpp/ParameterSet.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <xmlrpc-c/girerr.hpp>
#include <xmlrpc-c/base.hpp>
#include <xmlrpc-c/client_simple.hpp>
#pragma GCC diagnostic pop

#include "TBufferFile.h"

#include <limits>
#include <iostream>
#include <string>
#include <sstream>
#include <csignal>

namespace
{
	volatile std::sig_atomic_t gSignalStatus = 0; ///< Stores singal from signal handler
}

/**
 * \brief Handle a Unix signal
 * \param signal Signal to handle
 */
void signal_handler(int signal)
{
	gSignalStatus = signal;
}

artdaq::TransferWrapper::TransferWrapper(const fhicl::ParameterSet& pset) :
																		  timeoutInUsecs_(pset.get<std::size_t>("timeoutInUsecs", 100000))
																		  , dispatcherHost_(pset.get<std::string>("dispatcherHost"))
																		  , dispatcherPort_(pset.get<std::string>("dispatcherPort"))
																		  , serverUrl_("http://" + dispatcherHost_ + ":" + dispatcherPort_ + "/RPC2")
																		  , maxEventsBeforeInit_(pset.get<std::size_t>("maxEventsBeforeInit", 5))
																		  , allowedFragmentTypes_(pset.get<std::vector<int>>("allowedFragmentTypes", {226, 227, 229}))
																		  , quitOnFragmentIntegrityProblem_(pset.get<bool>("quitOnFragmentIntegrityProblem", true))
																		  , debugLevel_(pset.get<std::size_t>("debugLevel", 0))
																		  , monitorRegistered_(false)
{
	std::signal(SIGINT, signal_handler);

	try
	{
		transfer_ = MakeTransferPlugin(pset, "transfer_plugin", TransferInterface::Role::kReceive);
	}
	catch (...)
	{
		ExceptionHandler(ExceptionHandlerRethrow::yes,
						 "TransferWrapper: failure in call to MakeTransferPlugin");
	}

	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	TLOG_INFO("TransferWrapper") << "Attempting to register this monitor (\"" << transfer_->uniqueLabel()
		<< "\") with the dispatcher aggregator" << TLOG_ENDL;

	try
	{
		myClient.call(serverUrl_, "daq.register_monitor", "s", &result, pset.to_string().c_str());
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting XML-RPC call on host " << dispatcherHost_
			<< ", port " << dispatcherPort_ << "; possible causes are malformed FHiCL or nonexistent process at requested port";
		ExceptionHandler(ExceptionHandlerRethrow::yes,
						 errmsg.str());
	}

	const std::string status = xmlrpc_c::value_string(result);

	TLOG_INFO("TransferWrapper") << "Response from dispatcher is \""
		<< status << "\"" << TLOG_ENDL;

	if (status == "Success")
	{
		monitorRegistered_ = true;
	}
	else
	{
		throw cet::exception("TransferWrapper") << "Error in TransferWrapper: attempt to register with dispatcher did not result in the \"Success\" response";
	}
}

void artdaq::TransferWrapper::receiveMessage(std::unique_ptr<TBufferFile>& msg)
{
	std::unique_ptr<artdaq::Fragment> fragmentPtr;
	bool receivedFragment = false;
	static bool initialized = false;
	static size_t fragments_received = 0;

	while (true && !gSignalStatus)
	{
		fragmentPtr = std::make_unique<artdaq::Fragment>();

		while (!receivedFragment)
		{
			if (gSignalStatus)
			{
				TLOG_INFO("TransferWrapper") << "Ctrl-C appears to have been hit" << TLOG_ENDL;
				unregisterMonitor();
				return;
			}

			try
			{
				auto result = transfer_->receiveFragment(*fragmentPtr, timeoutInUsecs_);

				if (result != artdaq::TransferInterface::RECV_TIMEOUT)
				{
					receivedFragment = true;
					fragments_received++;

					static size_t cntr = 1;

					if (debugLevel_ > 1)
					{
						TLOG_INFO("TransferWrapper") << "Received " << cntr++ << "-th event, "
							<< "seqID == " << fragmentPtr->sequenceID()
							<< ", type == " << static_cast<int>(fragmentPtr->type()) << TLOG_ENDL;
					}
					continue;
				}
				else
				{
					if (debugLevel_ > 0)
					{
						TLOG_WARNING("TransferWrapper") << "Timeout occurred in call to transfer_->receiveFragmentFrom; will try again" << TLOG_ENDL;
					}
				}
			}
			catch (...)
			{
				ExceptionHandler(ExceptionHandlerRethrow::yes,
								 "Problem receiving data in TransferWrapper::receiveMessage");
			}
		}

		try
		{
			extractTBufferFile(*fragmentPtr, msg);
		}
		catch (...)
		{
			ExceptionHandler(ExceptionHandlerRethrow::yes,
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
				throw cet::exception("TransferWrapper") << "First " << maxEventsBeforeInit_ <<
					  " events received did not include the \"Init\" event containing necessary info for art; exiting...";
			}
		}
	}
}


void
artdaq::TransferWrapper::extractTBufferFile(const artdaq::Fragment& fragment,
											std::unique_ptr<TBufferFile>& tbuffer)
{
	const artdaq::NetMonHeader* header = fragment.metadata<artdaq::NetMonHeader>();
	char* buffer = (char *)malloc(header->data_length);
	memcpy(buffer, fragment.dataBeginBytes(), header->data_length);

	// TBufferFile takes ownership of the contents of memory passed to it
	tbuffer.reset(new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0));
}

void
artdaq::TransferWrapper::checkIntegrity(const artdaq::Fragment& fragment) const
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
		errmsg << "Error: artdaq fragment of type " <<
			type << ", sequence ID " <<
			fragment.sequenceID() <<
			" has internally inconsistent measures of its size, signalling data corruption: in bytes," <<
			" total size = " << totalsize << ", artdaq fragment header = " << artdaqheader <<
			", metadata = " << metadata << ", payload = " << payload;

		TLOG_ERROR("TransferWrapper") << errmsg.str() << TLOG_ENDL;

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

		TLOG_ERROR("TransferWrapper") << errmsg.str() << TLOG_ENDL;
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

void
artdaq::TransferWrapper::unregisterMonitor()
{
	if (!monitorRegistered_)
	{
		throw cet::exception("TransferWrapper") <<
			  "The function to unregister the monitor was called, but the monitor doesn't appear to be registered";
	}

	TLOG_INFO("TransferWrapper") << "Requesting that this monitor (" << transfer_->uniqueLabel()
		<< ") be unregistered from the dispatcher aggregator" << TLOG_ENDL;

	xmlrpc_c::clientSimple myClient;
	xmlrpc_c::value result;

	try
	{
		myClient.call(serverUrl_, "daq.unregister_monitor", "s", &result, (transfer_->uniqueLabel()).c_str());
	}
	catch (...)
	{
		std::stringstream errmsg;
		errmsg << "Problem attempting to unregister monitor via XML-RPC call on host " << dispatcherHost_
			<< ", port " << dispatcherPort_ << "; possible causes are that the monitor label \""
			<< transfer_->uniqueLabel()
			<< "\" is unrecognized by contacted process or process at requested port doesn't exist";
		ExceptionHandler(ExceptionHandlerRethrow::no,
						 errmsg.str());
	}

	const std::string status = xmlrpc_c::value_string(result);

	TLOG_INFO("TransferWrapper") << "Response from dispatcher is \""
		<< status << "\"" << TLOG_ENDL;

	if (status == "Success")
	{
		monitorRegistered_ = false;
	}
	else
	{
		throw cet::exception("TransferWrapper") << "Error in TransferWrapper: attempt to unregister with dispatcher did not result in the \"Success\" response";
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
}
