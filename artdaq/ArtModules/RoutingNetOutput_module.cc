#define TRACE_NAME (app_name + "_RoutingNetOutput").c_str()
#include "artdaq/DAQdata/Globals.hh"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/Selector.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "artdaq-utilities/Plugins/GPMPublisher.hh"
#include "artdaq-utilities/Plugins/MakeGPMPlugins.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"
#include "artdaq/DAQrate/TokenReceiver.hh"
#include "artdaq/RoutingPolicies/RoutingDestinationHelper.hh"
#include "artdaq/RoutingPolicies/makeRoutingMasterPolicy.hh"

#include <unistd.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace art {
class RoutingNetOutput;
}

using art::RoutingNetOutput;
using fhicl::ParameterSet;

/**
 * \brief An art::OutputModule which sends Fragments using DataSenderManager.
 * This module produces output identical to that of a BoardReader, for use in
 * systems which have multiple layers of EventBuilders.
 */
class art::RoutingNetOutput final : public OutputModule
{
public:
	/**
	 * \brief RoutingNetOutput Constructor
	 * \param ps ParameterSet used to configure RoutingNetOutput
	 *
	 * RoutingNetOutput forwards its ParameterSet to art::OutputModule,
	 * so any Parameters it requires are also required by RoutingNetOutput.
	 * RoutingNetOutput also forwards its ParameterSet to DataSenderManager,
	 * so any Parameters *it* requires are *also* required by RoutingNetOutput.
	 * Finally, RoutingNetOutput accpets the following parameters:
	 * "rt_priority" (Default: 0): Priority for this thread
	 * "module_name" (Default: RoutingNetOutput): Friendly name for this module (MessageFacility Category)
	 */
	explicit RoutingNetOutput(ParameterSet const& ps);

	/**
	 * \brief RoutingNetOutput Destructor
	 */
	virtual ~RoutingNetOutput();

private:
	void beginJob() override;
	void endJob() override;

	void beginRun(RunPrincipal const&) override;
	void endRun(RunPrincipal const&) override;

	void write(EventPrincipal&) override;

	void writeRun(RunPrincipal&) override{};
	void writeSubRun(SubRunPrincipal&) override{};

	void initialize_MPI_();

	void deinitialize_MPI_();

	bool readParameterSet_(fhicl::ParameterSet const& pset);

	void publish_good_status_(std::string const& marker) const;
	void publish_bad_status_(std::string const& marker) const;
	void publish_status_(std::string const& status, std::string const& marker) const;

	void run_buffer_monitor_();

private:
	ParameterSet full_pset_;
	fhicl::ParameterSet policy_pset_;
	fhicl::ParameterSet token_receiver_pset_;
	fhicl::ParameterSet inhibit_publisher_pset_;
	std::string name_ = "RoutingNetOutput";
	int rt_priority_;
	double wait_for_destination_timeout_sec_;
	size_t buffer_test_interval_usec_;
	int buffer_count_for_inhibit_;
	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_ = {nullptr};
	std::shared_ptr<artdaq::RoutingMasterPolicy> policy_ = {nullptr};
	std::unique_ptr<artdaq::TokenReceiver> token_receiver_ = {nullptr};
	std::unique_ptr<artdaq::RoutingDestinationHelper> destination_helper_ = {nullptr};
	std::unique_ptr<artdaq::GPMPublisher> inhibit_publisher_ = {nullptr};
	std::string app_name_no_underscore_;
	std::chrono::steady_clock::time_point run_start_time_;
	boost::thread buffer_monitor_thread_;
	bool stop_requested_;
};

art::RoutingNetOutput::RoutingNetOutput(ParameterSet const& ps)
    : OutputModule(ps)
{
	TLOG(TLVL_DEBUG) << "Begin: RoutingNetOutput::RoutingNetOutput(ParameterSet const& ps)\n";
	readParameterSet_(ps);

	const auto target = std::regex{"_"};
	const auto replacement = std::string{"-"};
	app_name_no_underscore_ = std::regex_replace(app_name, target, replacement);
	TLOG(TLVL_DEBUG) << "End: RoutingNetOutput::RoutingNetOutput(ParameterSet const& ps)\n";
}

art::RoutingNetOutput::~RoutingNetOutput() { TLOG(TLVL_DEBUG) << "Begin/End: RoutingNetOutput::~RoutingNetOutput()\n"; }

void art::RoutingNetOutput::beginJob()
{
	TLOG(TLVL_DEBUG) << "Begin: RoutingNetOutput::beginJob()\n";
	initialize_MPI_();
	TLOG(TLVL_DEBUG) << "End:   RoutingNetOutput::beginJob()\n";
}

void art::RoutingNetOutput::endJob()
{
	TLOG(TLVL_DEBUG) << "Begin: RoutingNetOutput::endJob()\n";
	deinitialize_MPI_();
	TLOG(TLVL_DEBUG) << "End:   RoutingNetOutput::endJob()\n";
}

void art::RoutingNetOutput::beginRun(art::RunPrincipal const& rp)
{
	if (token_receiver_.get() != nullptr)
	{
		token_receiver_->setRunNumber(rp.run());
		token_receiver_->resumeTokenReception();
	}
	run_start_time_ = std::chrono::steady_clock::now();

	stop_requested_ = false;
	if (inhibit_publisher_.get() != nullptr)
	{
		boost::thread::attributes attrs;
		attrs.set_stack_size(4096 * 2000);  // 2000 KB
		try
		{
			buffer_monitor_thread_ = boost::thread(attrs, boost::bind(&RoutingNetOutput::run_buffer_monitor_, this));
			TLOG(TLVL_DEBUG) << "Created buffer monitor thread";
		}
		catch (const boost::exception& excpt)
		{
			TLOG(TLVL_ERROR) << "Caught boost::exception starting buffer monitor thread: " << boost::diagnostic_information(excpt) << ", errno=" << errno;
			std::cerr << "Caught boost::exception starting buffer monitor thread: " << boost::diagnostic_information(excpt) << ", errno=" << errno << std::endl;
			exit(5);
		}
	}
}

void art::RoutingNetOutput::endRun(art::RunPrincipal const& rp)
{
	stop_requested_ = true;

	if (token_receiver_.get() != nullptr)
	{
		TLOG(TLVL_INFO) << "Stopping run " << rp.run() << " after " << token_receiver_->getReceivedTokenCount() << " received tokens.";
		token_receiver_->pauseTokenReception();
	}

	if (buffer_monitor_thread_.joinable()) { buffer_monitor_thread_.join(); }
}

void art::RoutingNetOutput::initialize_MPI_()
{
	if (rt_priority_ > 0)
	{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
		sched_param s_param = {};
		s_param.sched_priority = rt_priority_;
		int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
		if (status != 0)
		{
			TLOG(TLVL_ERROR) << name_ << "Failed to set realtime priority to " << rt_priority_
			                 << ", return code = " << status;
		}
#pragma GCC diagnostic pop
	}

	sender_ptr_ = std::make_unique<artdaq::DataSenderManager>(full_pset_);
	assert(sender_ptr_);

	auto policy_plugin_spec = policy_pset_.get<std::string>("policy", "");
	if (policy_plugin_spec.length() == 0)
	{
		TLOG(TLVL_ERROR)
		    << "No policy type (parameter name = \"policy\") was "
		    << "specified in the policy ParameterSet.  The full "
		    << "initialization PSet was \"" << full_pset_.to_string() << "\".";
	}
	else
	{
		try
		{
			policy_ = artdaq::makeRoutingMasterPolicy(policy_plugin_spec, policy_pset_);
		}
		catch (...)
		{
			TLOG(TLVL_ERROR) << "Unable to create a Routing Policy of type \"" << policy_plugin_spec << "\" from ParameterSet: \"" + policy_pset_.to_string() + "\".";
		}

		if (policy_.get() != nullptr)
		{
			token_receiver_.reset(new artdaq::TokenReceiver(token_receiver_pset_, policy_, artdaq::detail::RoutingMasterMode::RouteBySequenceID, 1, 100));
			token_receiver_->startTokenReception();
			token_receiver_->pauseTokenReception();

			destination_helper_.reset(new artdaq::RoutingDestinationHelper(policy_));
		}
	}

	auto inhibit_publisher_plugin_spec = inhibit_publisher_pset_.get<std::string>("publisher", "");
	if (inhibit_publisher_plugin_spec.length() == 0)
	{
		TLOG(TLVL_WARNING)
		    << "No publisher type (parameter name = \"publisher\") was "
		    << "specified in the inhibit_publisher ParameterSet, so there will be *no* "
		    << "publishing of Inhibit messages from the " << TRACE_NAME << " code.  The full "
		    << "initialization PSet was \"" << full_pset_.to_string() << "\".";
	}
	else
	{
		try
		{
			inhibit_publisher_ = artdaq::makeGPMPublisher(inhibit_publisher_plugin_spec, inhibit_publisher_pset_, app_name);
			std::string bind_address = inhibit_publisher_pset_.get<std::string>("bind_address", "");
			int retcode = inhibit_publisher_->bind(bind_address);
			if (retcode == 0)
			{
				TLOG(TLVL_DEBUG) << "Successfully created Inhibit Publisher with bind address \"" << bind_address << "\"";
			}
			else
			{
				TLOG(TLVL_ERROR) << "Unable to bind Inhibit Publisher to bind address \"" << bind_address
				                 << "\" (bind return code = " << retcode << ").";
			}
		}
		catch (...)
		{
			TLOG(TLVL_ERROR) << "Unable to create an Inhibit Publisher of type \"" << inhibit_publisher_plugin_spec << "\" from ParameterSet: \"" + inhibit_publisher_pset_.to_string() + "\".";
		}
	}
}

void art::RoutingNetOutput::deinitialize_MPI_()
{
	sender_ptr_.reset(nullptr);

	if (token_receiver_.get() != nullptr)
	{
		token_receiver_->stopTokenReception(true);
	}
}

bool art::RoutingNetOutput::readParameterSet_(fhicl::ParameterSet const& pset)
{
	TLOG(TLVL_DEBUG) << name_ << "RoutingNetOutput::readParameterSet_ method called with "
	                 << "ParameterSet = \"" << pset.to_string() << "\".";

	// determine the data sending parameters
	full_pset_ = pset;
	name_ = pset.get<std::string>("module_name", "RoutingNetOutput");
	rt_priority_ = pset.get<int>("rt_priority", 0);
	wait_for_destination_timeout_sec_ = pset.get<double>("wait_for_destination_timeout_sec", 20.0);
	buffer_test_interval_usec_ = pset.get<size_t>("buffer_test_interval_usec", 100000);
	buffer_count_for_inhibit_ = pset.get<int>("buffer_count_for_inhibit", 0);

	try
	{
		policy_pset_ = full_pset_.get<fhicl::ParameterSet>("policy");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR) << "Unable to find the policy parameters in the RoutingNetOutput initialization ParameterSet: \"" + full_pset_.to_string() + "\".";
	}

	try
	{
		token_receiver_pset_ = full_pset_.get<fhicl::ParameterSet>("token_receiver");
	}
	catch (...)
	{
		TLOG(TLVL_ERROR) << "Unable to find the token_receiver parameters in the RoutingNetOutput initialization ParameterSet: \"" + full_pset_.to_string() + "\".";
	}

	try
	{
		inhibit_publisher_pset_ = full_pset_.get<fhicl::ParameterSet>("inhibit_publisher");
	}
	catch (...)
	{
		TLOG(TLVL_WARNING) << "Unable to find the inhibit_publisher parameters in the RoutingNetOutput initialization ParameterSet: \"" + full_pset_.to_string() + "\".";
	}

	TLOG(TLVL_TRACE) << "RoutingNetOutput::readParameterSet()";

	return true;
}

void art::RoutingNetOutput::write(EventPrincipal& ep)
{
	assert(sender_ptr_);

	using RawEvent = artdaq::Fragments;
	using RawEvents = std::vector<RawEvent>;
	using RawEventHandle = art::Handle<RawEvent>;
	using RawEventHandles = std::vector<RawEventHandle>;

	auto result_handles = std::vector<art::GroupQueryResult>();

	auto const& wrapped = art::WrappedTypeID::make<RawEvent>();
#if ART_HEX_VERSION >= 0x30000
	ModuleContext const mc{moduleDescription()};
	ProcessTag const processTag{"", mc.moduleDescription().processName()};

	result_handles = ep.getMany(mc, wrapped, art::MatchAllSelector{}, processTag);
#else
	result_handles = ep.getMany(wrapped, art::MatchAllSelector{});
#endif

	for (auto const& result_handle : result_handles)
	{
		auto const raw_event_handle = RawEventHandle(result_handle);

		if (!raw_event_handle.isValid()) continue;

		for (auto const& fragment : *raw_event_handle)
		{
			auto fragment_copy = fragment;
			auto fragid_id = fragment_copy.fragmentID();
			auto sequence_id = fragment_copy.sequenceID();

			// fetch the next destination, waiting for it, if needed
			int dest_rank = destination_helper_->GetNextDestinationRank();
			if (dest_rank == artdaq::RoutingDestinationHelper::INVALID_DESTINATION)
			{
				// loop until a destination becomes available, or we time out
				TLOG(TLVL_DEBUG) << "RoutingNetOutput::write waiting for valid destination rank";
				auto start_time = std::chrono::steady_clock::now();
				while (dest_rank == artdaq::RoutingDestinationHelper::INVALID_DESTINATION &&
				       artdaq::TimeUtils::GetElapsedTime(start_time) < wait_for_destination_timeout_sec_)
				{
					usleep(10000);
					dest_rank = destination_helper_->GetNextDestinationRank();
				}

				// if we successfully found a destination rank, report good status
				if (dest_rank != artdaq::RoutingDestinationHelper::INVALID_DESTINATION)
				{
					TLOG(TLVL_DEBUG) << "RoutingNetOutput::write found destination rank " << dest_rank;
				}
			}

			// send the fragment to the destination
			if (dest_rank != artdaq::RoutingDestinationHelper::INVALID_DESTINATION)
			{
				TLOG(TLVL_DEBUG) << "RoutingNetOutput::write seq=" << sequence_id << " frag=" << fragid_id << " dest_rank=" << dest_rank << " start";
				sender_ptr_->sendFragment(std::move(fragment_copy), dest_rank);
				TLOG(TLVL_DEBUG) << "RoutingNetOutput::write seq=" << sequence_id << " frag=" << fragid_id << " dest_rank=" << dest_rank << " done";
				// Events are unique in art, so this will be the only send with this sequence ID!
				sender_ptr_->RemoveRoutingTableEntry(sequence_id);
			}
			else
			{
				TLOG(TLVL_ERROR) << "Unable to determine a valid destination rank! This event has been lost: " << sequence_id;
			}
		}
	}

	return;
}

void art::RoutingNetOutput::run_buffer_monitor_()
{
	std::chrono::steady_clock::time_point inhibit_status_report_time = run_start_time_;
	bool buffers_available = true;
	bool inhibit_is_on = false;
	while (!stop_requested_)
	{
		usleep(buffer_test_interval_usec_);
		if (static_cast<int>(destination_helper_->GetAvailableDestinationCount()) <= buffer_count_for_inhibit_)
		{
			buffers_available = false;
		}
		else
		{
			buffers_available = true;
		}

		if ((buffers_available && inhibit_is_on))
		{
			inhibit_is_on = false;
			publish_good_status_("*");
			inhibit_status_report_time = std::chrono::steady_clock::now();
		}
		else if ((!buffers_available && !inhibit_is_on))
		{
			inhibit_is_on = true;
			publish_bad_status_("NoAvailableTokens");
			inhibit_status_report_time = std::chrono::steady_clock::now();
		}

		// At the begining of a run, periodically publish the current status.
		// The goal of this is to ensure that all listeners get at least one notification.
		// (Some types of listeners have a delay between when we create the publisher in
		// this code and when they connect to it and get their first update.)
		else if (artdaq::TimeUtils::GetElapsedTime(run_start_time_) < 15.0 &&
		         artdaq::TimeUtils::GetElapsedTime(inhibit_status_report_time) >= 1.0)
		{
			if (buffers_available)
			{
				publish_good_status_("*");
			}
			else
			{
				publish_bad_status_("NoAvailableTokens");
			}
			inhibit_status_report_time = std::chrono::steady_clock::now();
		}
	}
}

void art::RoutingNetOutput::publish_good_status_(std::string const& marker) const
{
	publish_status_("GOOD", marker);
}

void art::RoutingNetOutput::publish_bad_status_(std::string const& marker) const
{
	publish_status_("BAD", marker);
}

void art::RoutingNetOutput::publish_status_(std::string const& status, std::string const& marker) const
{
	if (inhibit_publisher_.get() != nullptr)
	{
		auto now = std::chrono::system_clock::now();
		auto itt = std::chrono::system_clock::to_time_t(now);
		std::ostringstream time_string;
		time_string << std::put_time(gmtime(&itt), "%FT%TZ");

		std::string inhibit_message = "STATUSMSG_" + app_name_no_underscore_ + "_" + marker + "_" + status + "_" + time_string.str();
		int retcode = inhibit_publisher_->send(inhibit_message);
		if (retcode > 0)
		{
			TLOG(TLVL_TRACE) << "Sent Inhibit message \"" << inhibit_message << "\".";
		}
		else
		{
			TLOG(TLVL_ERROR) << "Error sending Inhibit message \"" << inhibit_message
			                 << "\" (return code = " << retcode << ").";
		}
	}
}
DEFINE_ART_MODULE(art::RoutingNetOutput)
