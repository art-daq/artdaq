#include "TRACE/tracemf.h"

#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"
#include "artdaq/DAQdata/Globals.hh"

#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "art/Framework/Services/Registry/ServiceDefinitionMacros.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Comment.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/Name.h"

#include <cstdint>
#include <memory>

#define TRACE_NAME "ArtdaqSharedMemoryService"
// ----------------------------------------------------------------------

/**
 * \brief ArtdaqSharedMemoryService extends ArtdaqSharedMemoryServiceInterface.
 * It receives events from shared memory using SharedMemoryEventReceiver. It also manages the artdaq Global varaibles my_rank and app_name.
 * Users should retrieve a ServiceHandle to this class before using artdaq Globals to ensure the correct values are used.
 */
class ArtdaqSharedMemoryService : public ArtdaqSharedMemoryServiceInterface
{
public:
	/// <summary>
	/// Allowed Configuration parameters of NetMonTransportService. May be used for configuration validation
	/// </summary>
	struct Config
	{
		/// "shared_memory_key" (Default: 0xBEE70000 + pid): Key to use when connecting to shared memory. Will default to 0xBEE70000 + getppid().
		fhicl::Atom<uint32_t> shared_memory_key{fhicl::Name{"shared_memory_key"}, fhicl::Comment{"Key to use when connecting to shared memory. Will default to 0xBEE70000 + getppid()."}, 0xBEE70000};
		/// "shared_memory_key" (Default: 0xCEE70000 + pid): Key to use when connecting to broadcast shared memory. Will default to 0xCEE70000 + getppid().
		fhicl::Atom<uint32_t> broadcast_shared_memory_key{fhicl::Name{"broadcast_shared_memory_key"}, fhicl::Comment{"Key to use when connecting to broadcast shared memory. Will default to 0xCEE70000 + getppid()."}, 0xCEE70000};
		/// "rank" (OPTIONAL) : The rank of this applicaiton, for use by non - artdaq applications running NetMonTransportService
		fhicl::Atom<int> rank{fhicl::Name{"rank"}, fhicl::Comment{"Rank of this artdaq application. Used for data transfers"}};
		/// "subrun_closure_threshold" (Default: 5) Minimum number of events in event ordering list before releasing a subrun/run change event
		fhicl::Atom<size_t> subrun_closure_threshold{fhicl::Name{"subrun_closure_threshold"}, fhicl::Comment{"Minimum number of events in event ordering list before releasing a subrun/run change event"}, 1};
		/// "safety_valve_timeout_s" (Default: 10.0): Maximum time (in s) to wait before releasing the front of the event ordering list
		fhicl::Atom<double> safety_valve_timeout_s{fhicl::Name{"safety_valve_timeout_s"}, fhicl::Comment{"Maximum time (in s) to wait before releasing the front of the event ordering list"}, 10.0};
	};
	/// Used for ParameterSet validation (if desired)
	using Parameters = fhicl::WrappedTable<Config>;

	/**
	 * \brief NetMonTransportService Destructor. Calls disconnect().
	 */
	virtual ~ArtdaqSharedMemoryService();

	/**
	 * \brief NetMonTransportService Constructor
	 * \param pset ParameterSet used to configure NetMonTransportService and DataSenderManager. See NetMonTransportService::Config
	 */
	ArtdaqSharedMemoryService(fhicl::ParameterSet const& pset, art::ActivityRegistry&);

	/**
	 * \brief Receive event(s) from the shared memory
	 * \param broadcast Whether to only attempt to receive a broadcast (broadcasts are always preferentially received over data)
	 * \return Map of Fragment types retrieved from shared memory
	 */
	std::shared_ptr<ArtdaqEvent> ReceiveEvent(bool broadcast) override;

	/**
	 * \brief Get the number of events which are ready to be read
	 * \return The number of events which can be read
	 */
	size_t GetQueueSize() override { return incoming_events_->ReadReadyCount(); }

	/**
	 * \brief Get the maximum number of events which can be stored in the shared memory
	 * \return The maximum number of events which can be stored in the shared memory
	 */
	size_t GetQueueCapacity() override { return incoming_events_->size(); }

	/**
	 * \brief Get the ID of this art process
	 * \return The ID of this art process from the shared memory segment
	 */
	size_t GetMyId() override { return incoming_events_->GetMyId(); }

private:
	ArtdaqSharedMemoryService(ArtdaqSharedMemoryService const&) = delete;
	ArtdaqSharedMemoryService(ArtdaqSharedMemoryService&&) = delete;
	ArtdaqSharedMemoryService& operator=(ArtdaqSharedMemoryService const&) = delete;
	ArtdaqSharedMemoryService& operator=(ArtdaqSharedMemoryService&&) = delete;

	std::shared_ptr<ArtdaqEvent> ReadEventFromSharedMemory(bool broadcast);

private:
	std::unique_ptr<artdaq::SharedMemoryEventReceiver> incoming_events_;
	std::list<std::shared_ptr<ArtdaqEvent>> event_ordering_;
	std::set<artdaq::Fragment::sequence_id_t> released_broadcast_sequence_ids_;
	size_t read_timeout_;
	size_t subrun_closure_threshold_{1};
	double safety_valve_timeout_s_{10.0};
	bool last_read_timeout_{false};
	bool resume_after_timeout_;
	bool printed_exit_message_{false};
	bool end_of_data_received_{false};
	bool subrun_has_events_{false};
	uint32_t current_subrun_{0};
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqSharedMemoryService, ArtdaqSharedMemoryServiceInterface, LEGACY)

static fhicl::ParameterSet empty_pset;

// clang-format off
#define TLVL_CONSTRUCTOR    TLVL_DEBUG + 32
#define TLVL_READEVENT      TLVL_DEBUG + 33
#define TLVL_READEVENT_2    TLVL_DEBUG + 34
#define TLVL_READEVENT_3    TLVL_DEBUG + 35
#define TLVL_RECEIVEEVENT   TLVL_DEBUG + 36
#define TLVL_RECEIVEEVENT_2 TLVL_DEBUG + 37
#define TLVL_RECEIVEEVENT_3 TLVL_DEBUG + 38
#define TLVL_RECEIVEEVENT_4 TLVL_DEBUG + 39
// clang-format on

ArtdaqSharedMemoryService::ArtdaqSharedMemoryService(fhicl::ParameterSet const& pset, art::ActivityRegistry& /*unused*/)
    : incoming_events_(nullptr)
    , event_ordering_()
    , read_timeout_(pset.get<size_t>("read_timeout_us", static_cast<size_t>(pset.get<double>("waiting_time", 600.0) * 1000000)))
    , subrun_closure_threshold_(pset.get<size_t>("subrun_closure_threshold", artdaq::SharedMemoryManager::GetCatchUpFactor() + 2))  // +2, one for ESRF itself, one for extra padding to ensure that no catch-up is being performed
    , safety_valve_timeout_s_(pset.get<double>("safety_valve_timeout_s", 10.0))
    , resume_after_timeout_(pset.get<bool>("resume_after_timeout", true))
{
	TLOG(TLVL_CONSTRUCTOR) << "ArtdaqSharedMemoryService CONSTRUCTOR";

	incoming_events_ = std::make_unique<artdaq::SharedMemoryEventReceiver>(
	    pset.get<int>("shared_memory_key", artdaq::Globals::SharedMemoryKey(0xEE000000, true)),
	    pset.get<int>("broadcast_shared_memory_key", artdaq::Globals::SharedMemoryKey(0xBB000000, true)));

	char const* artapp_env = getenv("ARTDAQ_APPLICATION_NAME");
	std::string artapp_str;
	if (artapp_env != nullptr)
	{
		artapp_str = std::string(artapp_env) + "_";
	}

	TLOG(TLVL_CONSTRUCTOR) << "Setting app_name";
	artdaq::Globals::my_art_id_ = incoming_events_->GetMyId();
	app_name = artapp_str + "art" + std::to_string(artdaq::Globals::my_art_id_);
	// artdaq::configureMessageFacility(app_name.c_str()); // ELF 11/20/2020: MessageFacility already configured by initialization pset

	artapp_env = getenv("ARTDAQ_RANK");
	if (artapp_env != nullptr && my_rank < 0)
	{
		TLOG(TLVL_CONSTRUCTOR) << "Setting rank from envrionment";
		my_rank = strtol(artapp_env, nullptr, 10);
	}
	else
	{
		TLOG(TLVL_CONSTRUCTOR) << "Setting my_rank from shared memory";
		my_rank = incoming_events_->GetRank();
	}

	try
	{
		if (metricMan)
		{
			metricMan->initialize(pset.get<fhicl::ParameterSet>("metrics", fhicl::ParameterSet()), app_name);
			metricMan->do_start();
		}
	}
	catch (...)
	{
		artdaq::ExceptionHandler(artdaq::ExceptionHandlerRethrow::no, "Error loading metrics in ArtdaqSharedMemoryService()");
	}

	TLOG(TLVL_INFO) << "app_name is " << app_name << ", rank " << my_rank;
}

ArtdaqSharedMemoryService::~ArtdaqSharedMemoryService()
{
	artdaq::Globals::CleanUpGlobals();
}

std::shared_ptr<ArtdaqEvent> ArtdaqSharedMemoryService::ReadEventFromSharedMemory(bool broadcast)
{
	TLOG(TLVL_READEVENT) << "ReadEventFromSharedMemory BEGIN";
	std::shared_ptr<ArtdaqEvent> output_event;

	while (output_event == nullptr)
	{
		TLOG(TLVL_READEVENT_2) << "ReadEventFromSharedMemory: Waiting for available buffer";
		bool got_event = false;
		auto start_time = std::chrono::steady_clock::now();
		auto read_timeout_to_use = read_timeout_ > 100000 ? 100000 : read_timeout_;
		if (!resume_after_timeout_ || broadcast) read_timeout_to_use = read_timeout_;
		while (!incoming_events_->IsEndOfData() && !got_event)
		{
			got_event = incoming_events_->ReadyForRead(broadcast, read_timeout_to_use);
			if (!got_event && (!resume_after_timeout_ || broadcast))  // Only try broadcasts once!
			{
				TLOG(TLVL_ERROR) << "Timeout occurred! No data received after " << read_timeout_to_use << " us. Returning empty Fragment list!";
				last_read_timeout_ = true;
				return nullptr;
			}
			if (!got_event && artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time) > read_timeout_)
			{
				TLOG(TLVL_READEVENT_2) << "Timeout occurred! No data received after " << artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time) << " us. Retrying.";
				last_read_timeout_ = true;
			}
		}

		if (incoming_events_->IsEndOfData())
		{
			if (!printed_exit_message_)
			{
				TLOG(TLVL_INFO) << "End of Data signal received, exiting";
				printed_exit_message_ = true;
			}
			return nullptr;
		}

		TLOG(TLVL_READEVENT) << "ReadEventFromSharedMemory: Reading buffer header";
		last_read_timeout_ = false;
		output_event = std::make_shared<ArtdaqEvent>();
		auto errflag = false;
		auto hdrPtr = incoming_events_->ReadHeader(errflag);
		if (errflag || hdrPtr == nullptr)
		{  // Buffer was changed out from under reader!
			incoming_events_->ReleaseBuffer();
			continue;  // retry
		}
		output_event->header = std::make_shared<artdaq::detail::RawEventHeader>(*hdrPtr);
		TLOG(TLVL_READEVENT) << "ReadEventFromSharedMemory: Getting Fragment types";
		auto fragmentTypes = incoming_events_->GetFragmentTypes(errflag);
		if (errflag)
		{  // Buffer was changed out from under reader!
			incoming_events_->ReleaseBuffer();
			continue;  // retry
		}
		if (fragmentTypes.empty())
		{
			TLOG(TLVL_ERROR) << "Event has no Fragments! Aborting!";
			incoming_events_->ReleaseBuffer();
			return nullptr;
		}

		for (auto const& type : fragmentTypes)
		{
			TLOG(TLVL_READEVENT_3) << "ReadEventFromSharedMemory: Getting all Fragments of type " << static_cast<int>(type);
			output_event->fragments[type] = incoming_events_->GetFragmentsByType(errflag, type);
			if (!output_event->fragments[type])
			{
				TLOG(TLVL_WARNING) << "Error retrieving Fragments from shared memory! (Most likely due to a buffer overwrite) Retrying...";
				incoming_events_->ReleaseBuffer();
				output_event->fragments.clear();
				continue;
			}
			// Events coming out of the EventStore are not sorted but need to be sorted by sequence ID before they can be passed to art.
			std::sort(output_event->fragments[type]->begin(), output_event->fragments[type]->end(), artdaq::fragmentSequenceIDCompare);
		}
		TLOG(TLVL_READEVENT) << "ReadEventFromSharedMemory: Releasing buffer";
		incoming_events_->ReleaseBuffer();
	}

	TLOG(TLVL_READEVENT) << "ReadEventFromSharedMemory END";
	return output_event;
}

std::shared_ptr<ArtdaqEvent> ArtdaqSharedMemoryService::ReceiveEvent(bool broadcast)
{
	TLOG(TLVL_RECEIVEEVENT) << "ReceiveEvent BEGIN";
	std::shared_ptr<ArtdaqEvent> output_event;
	auto start_time = std::chrono::steady_clock::now();

	while (output_event == nullptr)
	{
		// If we experienced a timeout, or have an EndOfData event, drain any held Start/End Run/SubRun events
		if (last_read_timeout_ || end_of_data_received_)
		{
			if (event_ordering_.size() > 0)
			{
				output_event = event_ordering_.front();
				event_ordering_.pop_front();
				break;  // while(output_event == nullptr)
			}
			// Don't try to get more data if we have an EndOfData event
			if (end_of_data_received_) { break; }
		}

		if (event_ordering_.size() > 0)
		{
			auto first_type = event_ordering_.front()->FirstFragmentType();
			auto first_sr = event_ordering_.front()->header->subrun_id;
			// If there is an Init Fragment, return it
			if (first_type == artdaq::Fragment::InitFragmentType)
			{
				TLOG(TLVL_RECEIVEEVENT) << "Returning Init Fragment";
				output_event = event_ordering_.front();
				event_ordering_.pop_front();
				break;  // while(output_event == nullptr)
			}
			if (current_subrun_ != 0 && first_sr <= current_subrun_)
			{
				if (artdaq::Fragment::isUserFragmentType(first_type) || first_type == artdaq::Fragment::ContainerFragmentType || first_type == artdaq::Fragment::EmptyFragmentType || first_type == artdaq::Fragment::DataFragmentType)
				{
					TLOG(TLVL_RECEIVEEVENT) << "Returning Fragment due to subrun match";
					output_event = event_ordering_.front();
					event_ordering_.pop_front();
					break;  // while(output_event == nullptr)
				}
				else if (event_ordering_.size() > subrun_closure_threshold_)
				{
					// First Fragment is broadcast (begin/end run/subrun), but there's more in event ordering!
					TLOG(TLVL_RECEIVEEVENT) << "Returning Broadcast Fragment due to subrun closure";

					if (released_broadcast_sequence_ids_.count(event_ordering_.front()->header->sequence_id) == 0)
					{
						output_event = event_ordering_.front();
						released_broadcast_sequence_ids_.insert(event_ordering_.front()->header->sequence_id);
					}
					event_ordering_.pop_front();
					continue;  // while(output_event == nullptr)
				}
			}
			else if (current_subrun_ != 0 && first_sr > current_subrun_ + 1)
			{
				TLOG(TLVL_RECEIVEEVENT) << "Returning Fragment due to stale current_subrun_ (first_sr=" << first_sr << ", current_subrun_=" << current_subrun_ << ")";
				output_event = event_ordering_.front();
				event_ordering_.pop_front();
				break;  // while(output_event == nullptr)
			}

			if (event_ordering_.size() == 1 && (first_type == artdaq::Fragment::EndOfRunFragmentType || first_type == artdaq::Fragment::RunDataFragmentType))
			{
				TLOG(TLVL_RECEIVEEVENT) << "Returning Broadcast Fragment due to end-of-run";
				output_event = event_ordering_.front();
				event_ordering_.pop_front();
				break;  // while(output_event == nullptr)
			}

			if (current_subrun_ == 0)
			{
				if (artdaq::Fragment::isUserFragmentType(first_type) || first_type == artdaq::Fragment::ContainerFragmentType || first_type == artdaq::Fragment::EmptyFragmentType || first_type == artdaq::Fragment::DataFragmentType)
				{
					TLOG(TLVL_RECEIVEEVENT) << "Returning Fragment due to unset subrun";
					output_event = event_ordering_.front();
					event_ordering_.pop_front();
					break;  // while(output_event == nullptr)
				}
				// We cannot close a subrun that has not yet been opened
				if (first_type == artdaq::Fragment::EndOfSubrunFragmentType || first_type == artdaq::Fragment::SubrunDataFragmentType)
				{
					TLOG(TLVL_WARNING) << "Subrun is unset, discarding EndOfSubrun Fragment(s) for subrun " << first_sr;
					event_ordering_.pop_front();
				}
				// Likewise, we cannot close a run that is not open
				if (first_type == artdaq::Fragment::EndOfRunFragmentType || first_type == artdaq::Fragment::RunDataFragmentType)
				{
					TLOG(TLVL_WARNING) << "Subrun is unset, discarding EndOfRun Fragment(s) for run " << event_ordering_.front()->header->run_id;
					event_ordering_.pop_front();
				}
			}

			if (artdaq::TimeUtils::GetElapsedTime(start_time) > safety_valve_timeout_s_)
			{
				TLOG(TLVL_WARNING) << "Returning Fragment due to safety valve timeout (" << safety_valve_timeout_s_ << " s). event_ordering_ size=" << event_ordering_.size()
				                   << " (th=" << subrun_closure_threshold_ << "), first event type=" << static_cast<int>(first_type) << " sr="
				                   << first_sr << " (c=" << current_subrun_ << ")";
				output_event = event_ordering_.front();
				event_ordering_.pop_front();
				break;  // while(output_event == nullptr)
			}
		}

		auto next_event = ReadEventFromSharedMemory(broadcast);
		if (next_event == nullptr)
		{
			if (event_ordering_.size() > 0)
			{
				output_event = event_ordering_.front();
				event_ordering_.pop_front();
			}
			else
			{
				// Will return nullptr
				break;  // while(output_event == nullptr)
			}
		}
		else
		{
			// Reset start time when new event arrives
			start_time = std::chrono::steady_clock::now();
			TLOG(TLVL_RECEIVEEVENT_2) << "Adding ArtdaqEvent with run=" << next_event->header->run_id << ", subrun=" << next_event->header->subrun_id << ", seq=" << next_event->header->sequence_id << ", and type " << static_cast<int>(next_event->FirstFragmentType()) << " to event ordering list";
			if (next_event->FirstFragmentType() == artdaq::Fragment::EndOfDataFragmentType) { end_of_data_received_ = true; }
			else if (next_event->header->subrun_id < current_subrun_ && next_event->FirstFragmentType() != artdaq::Fragment::RunDataFragmentType && next_event->FirstFragmentType() == artdaq::Fragment::EndOfRunFragmentType)
			{
				auto seq_mask = 0xFFFFFFFF & next_event->header->sequence_id;
				TLOG(TLVL_WARNING) << "ArtdaqEvent with run = " << next_event->header->run_id << ", subrun = " << next_event->header->subrun_id << ", seq = " << next_event->header->sequence_id << " (32b mask " << seq_mask << "), and type " << static_cast<int>(next_event->FirstFragmentType()) << " is from a previous subrun! (current=" << current_subrun_ << ")";
			}
			event_ordering_.push_back(next_event);
			if (event_ordering_.size() > 1)
			{
				TLOG(TLVL_RECEIVEEVENT_2) << "event_ordering_ size is now " << event_ordering_.size();
			}
			event_ordering_.sort();
		}
	}

	if (output_event != nullptr)
	{
		auto type = output_event->FirstFragmentType();
		TLOG(TLVL_RECEIVEEVENT_3) << "Returning ArtdaqEvent with run=" << output_event->header->run_id << ", subrun=" << output_event->header->subrun_id
		                          << ", seq=" << output_event->header->sequence_id << ", and type " << static_cast<int>(type);
		if (output_event->header->subrun_id > current_subrun_ && output_event->header->subrun_id != 65535)  // EndOfRun Fragments have subrun -1
		{
			if (current_subrun_ != 0)
			{
				TLOG(TLVL_WARNING) << "Event subrun " << output_event->header->subrun_id << " is greater than current_subrun_ (" << current_subrun_ << "), incrementing";
			}
			else
			{
				TLOG(TLVL_DEBUG) << "Incrementing current_subrun_ from 0 to " << output_event->header->subrun_id << " due to unset subrun";
			}
			current_subrun_ = output_event->header->subrun_id;
		}
		if (type == artdaq::Fragment::EndOfSubrunFragmentType || type == artdaq::Fragment::SubrunDataFragmentType || type == artdaq::Fragment::EndOfRunFragmentType || type == artdaq::Fragment::RunDataFragmentType || type == artdaq::Fragment::InitFragmentType)
		{
			if (type == artdaq::Fragment::EndOfSubrunFragmentType || type == artdaq::Fragment::SubrunDataFragmentType)
			{
				TLOG(TLVL_RECEIVEEVENT_4) << "EndOfSubrun or SubrunData Fragment recieved, incrementing current_subrun from " << current_subrun_ << " to " << (current_subrun_ + 1);
				current_subrun_++;
				subrun_has_events_ = false;
			}
			else
			{
				TLOG(TLVL_DEBUG) << "Due to run/subrun/control conditions, setting current_subrun to 0";
				current_subrun_ = 0;
				subrun_has_events_ = false;
			}
		}
		else
		{
			subrun_has_events_ = true;
		}
	}

	TLOG(TLVL_RECEIVEEVENT) << "ReceiveEvent END";

	return output_event;
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqSharedMemoryService, ArtdaqSharedMemoryServiceInterface)
