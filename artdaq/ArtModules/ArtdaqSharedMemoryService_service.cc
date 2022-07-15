#include "TRACE/tracemf.h"
#define TRACE_NAME "ArtdaqSharedMemoryService"

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
	 * \brief Receive an event from the shared memory
	 * \param broadcast Whether to only attempt to receive a broadcast (broadcasts are always preferentially received over data)
	 * \return Map of Fragment types retrieved from shared memory
	 */
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> ReceiveEvent(bool broadcast) override;

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
	 * \brief Get a shared_ptr to the current event header, if any
	 * \return std::shared_ptr to current event header. May be nullptr if no event is currently being read
	 */
	std::shared_ptr<artdaq::detail::RawEventHeader> GetEventHeader() override { return evtHeader_; }

private:
	ArtdaqSharedMemoryService(ArtdaqSharedMemoryService const&) = delete;
	ArtdaqSharedMemoryService(ArtdaqSharedMemoryService&&) = delete;
	ArtdaqSharedMemoryService& operator=(ArtdaqSharedMemoryService const&) = delete;
	ArtdaqSharedMemoryService& operator=(ArtdaqSharedMemoryService&&) = delete;

private:
	std::unique_ptr<artdaq::SharedMemoryEventReceiver> incoming_events_;
	std::shared_ptr<artdaq::detail::RawEventHeader> evtHeader_;
	size_t read_timeout_;
	bool resume_after_timeout_;
};

DECLARE_ART_SERVICE_INTERFACE_IMPL(ArtdaqSharedMemoryService, ArtdaqSharedMemoryServiceInterface, LEGACY)

#define build_key(seed) ((seed) + ((GetPartitionNumber() + 1) << 16) + (getppid() & 0xFFFF))

static fhicl::ParameterSet empty_pset;

ArtdaqSharedMemoryService::ArtdaqSharedMemoryService(fhicl::ParameterSet const& pset, art::ActivityRegistry& /*unused*/)
    : incoming_events_(nullptr)
    , evtHeader_(nullptr)
    , read_timeout_(pset.get<size_t>("read_timeout_us", static_cast<size_t>(pset.get<double>("waiting_time", 600.0) * 1000000)))
    , resume_after_timeout_(pset.get<bool>("resume_after_timeout", true))
{
	TLOG(TLVL_DEBUG + 33) << "ArtdaqSharedMemoryService CONSTRUCTOR";

	incoming_events_ = std::make_unique<artdaq::SharedMemoryEventReceiver>(
	    pset.get<int>("shared_memory_key", build_key(0xEE000000)),
	    pset.get<int>("broadcast_shared_memory_key", build_key(0xBB000000)));

	char const* artapp_env = getenv("ARTDAQ_APPLICATION_NAME");
	std::string artapp_str;
	if (artapp_env != nullptr)
	{
		artapp_str = std::string(artapp_env) + "_";
	}

	TLOG(TLVL_DEBUG + 33) << "Setting app_name";
	app_name = artapp_str + "art" + std::to_string(incoming_events_->GetMyId());
	// artdaq::configureMessageFacility(app_name.c_str()); // ELF 11/20/2020: MessageFacility already configured by initialization pset

	artapp_env = getenv("ARTDAQ_RANK");
	if (artapp_env != nullptr && my_rank < 0)
	{
		TLOG(TLVL_DEBUG + 33) << "Setting rank from envrionment";
		my_rank = strtol(artapp_env, nullptr, 10);
	}
	else
	{
		TLOG(TLVL_DEBUG + 33) << "Setting my_rank from shared memory";
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

std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> ArtdaqSharedMemoryService::ReceiveEvent(bool broadcast)
{
	TLOG(TLVL_DEBUG + 33) << "ReceiveEvent BEGIN";
	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> recvd_fragments;

	while (recvd_fragments.empty())
	{
		TLOG(TLVL_DEBUG + 33) << "ReceiveEvent: Waiting for available buffer";
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
				return recvd_fragments;
			}
			if (!got_event && artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time) > read_timeout_)
			{
				TLOG(TLVL_WARNING) << "Timeout occurred! No data received after " << artdaq::TimeUtils::GetElapsedTimeMicroseconds(start_time) << " us. Retrying.";
			}
		}
		if (incoming_events_->IsEndOfData())
		{
			TLOG(TLVL_INFO) << "End of Data signal received, exiting";
			return recvd_fragments;
		}

		TLOG(TLVL_DEBUG + 33) << "ReceiveEvent: Reading buffer header";
		auto errflag = false;
		auto hdrPtr = incoming_events_->ReadHeader(errflag);
		if (errflag || hdrPtr == nullptr)
		{  // Buffer was changed out from under reader!
			incoming_events_->ReleaseBuffer();
			continue;  // retry
			           // return recvd_fragments;
		}
		evtHeader_ = std::make_shared<artdaq::detail::RawEventHeader>(*hdrPtr);
		TLOG(TLVL_DEBUG + 33) << "ReceiveEvent: Getting Fragment types";
		auto fragmentTypes = incoming_events_->GetFragmentTypes(errflag);
		if (errflag)
		{  // Buffer was changed out from under reader!
			incoming_events_->ReleaseBuffer();
			continue;  // retry
			           // return recvd_fragments;
		}
		if (fragmentTypes.empty())
		{
			TLOG(TLVL_ERROR) << "Event has no Fragments! Aborting!";
			incoming_events_->ReleaseBuffer();
			return recvd_fragments;
		}

		for (auto const& type : fragmentTypes)
		{
			TLOG(TLVL_DEBUG + 33) << "ReceiveEvent: Getting all Fragments of type " << static_cast<int>(type);
			recvd_fragments[type] = incoming_events_->GetFragmentsByType(errflag, type);
			if (!recvd_fragments[type])
			{
				TLOG(TLVL_ERROR) << "Error retrieving Fragments from shared memory! (Most likely due to a buffer overwrite) Retrying...";
				incoming_events_->ReleaseBuffer();
				recvd_fragments.clear();
				continue;
			}
			/* Events coming out of the EventStore are not sorted but need to be
	   sorted by sequence ID before they can be passed to art.
	*/
			std::sort(recvd_fragments[type]->begin(), recvd_fragments[type]->end(), artdaq::fragmentSequenceIDCompare);
		}
		TLOG(TLVL_DEBUG + 33) << "ReceiveEvent: Releasing buffer";
		incoming_events_->ReleaseBuffer();
	}

	TLOG(TLVL_DEBUG + 33) << "ReceiveEvent END";
	return recvd_fragments;
}

DEFINE_ART_SERVICE_INTERFACE_IMPL(ArtdaqSharedMemoryService, ArtdaqSharedMemoryServiceInterface)
