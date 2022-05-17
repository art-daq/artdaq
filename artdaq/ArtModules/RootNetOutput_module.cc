
#include <memory>

#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "artdaq/ArtModules/ArtdaqOutput.hh"
#include "artdaq/ArtModules/ArtdaqSharedMemoryService.h"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/DAQrate/DataSenderManager.hh"

// if TRACE_NAME has varible, it is safest to define after includes
#define TRACE_NAME (app_name + "_RootNetOutput").c_str()

#define DUMP_SEND_MESSAGE 0

namespace art {
class RootNetOutput;
}

/**
 * \brief An art::OutputModule which sends events using DataSenderManager.
 * This module is designed for transporting Fragment-wrapped art::Events after
 * they have been read into art, for example between the EventBuilder and the Aggregator.
 */
class art::RootNetOutput : public ArtdaqOutput
{
public:
	/**
	 * \brief RootNetOutput Constructor
	 * \param ps ParameterSet used to configure RootNetOutput
	 *
	 * RootNetOutput accepts no Parameters beyond those which art::OutputModule takes.
	 * See the art::OutputModule documentation for more details on those Parameters.
	 */
	explicit RootNetOutput(fhicl::ParameterSet const& ps);

	/**
	 * \brief RootNetOutput Destructor
	 */
	~RootNetOutput() override;

	/**
	 * \brief Get the number of data receivers
	 * \return The number of data receivers
	 */
	size_t dataReceiverCount() const { return sender_ptr_->destinationCount(); }

protected:
	/// <summary>
	/// Send a message using DataSenderManager
	/// </summary>
	/// <param name="fragment">Fragment to send</param>
	void SendMessage(artdaq::FragmentPtr& fragment) override;

private:
	RootNetOutput(RootNetOutput const&) = delete;
	RootNetOutput(RootNetOutput&&) = delete;
	RootNetOutput& operator=(RootNetOutput const&) = delete;
	RootNetOutput& operator=(RootNetOutput&&) = delete;

	void connect();
	void disconnect();

	std::unique_ptr<artdaq::DataSenderManager> sender_ptr_;
	fhicl::ParameterSet data_pset_;
	double init_timeout_s_;
};

art::RootNetOutput::RootNetOutput(fhicl::ParameterSet const& ps)
    : ArtdaqOutput(ps)
    , sender_ptr_(nullptr)
    , data_pset_(ps)
{
	TLOG(TLVL_DEBUG + 32) << "Begin: RootNetOutput::RootNetOutput(ParameterSet const& ps)";
	// Make sure the ArtdaqSharedMemoryService is available
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;
	init_timeout_s_ = ps.get<double>("init_fragment_timeout_seconds", 1.0);
	connect();
	TLOG(TLVL_DEBUG + 32) << "End:   RootNetOutput::RootNetOutput(ParameterSet const& ps)";
}

art::RootNetOutput::~RootNetOutput()
{
	TLOG(TLVL_DEBUG + 32) << "Begin: RootNetOutput::~RootNetOutput()";
	disconnect();
	TLOG(TLVL_DEBUG + 32) << "End:   RootNetOutput::~RootNetOutput()";
}

void art::RootNetOutput::SendMessage(artdaq::FragmentPtr& fragment)
{
	//
	//  Send message.
	//
	{
		TLOG(TLVL_WRITE) << "RootNetOutput::SendMessage Sending a message with type code "
		                 << artdaq::detail::RawFragmentHeader::SystemTypeToString(fragment->type());
		if (sender_ptr_ == nullptr)
		{
			TLOG(TLVL_DEBUG + 32) << "Reconnecting DataSenderManager";
			connect();
		}

#if DUMP_SEND_MESSAGE
		std::string fileName = "sendMessage_" + std::to_string(my_rank) + "_" + std::to_string(getpid()) + "_" +
		                       std::to_string(sequenceId) + ".bin";
		std::fstream ostream(fileName, std::ios::out | std::ios::binary);
		ostream.write(msg.Buffer(), msg.Length());
		ostream.close();
#endif

		auto sequenceId = fragment->sequenceID();
		TLOG(TLVL_DEBUG + 32) << "Sending message with sequenceID=" << sequenceId << ", type=" << static_cast<int>(fragment->type())
		                 << ", length=" << fragment->dataSizeBytes();

		sender_ptr_->sendFragment(std::move(*fragment));
		// Events are unique in art, so this will be the only send with this sequence ID!
		sender_ptr_->RemoveRoutingTableEntry(sequenceId);
		TLOG(TLVL_WRITE) << "RootNetOutput::SendMessage: Message sent.";
	}
}

void art::RootNetOutput::connect()
{
	auto start_time = std::chrono::steady_clock::now();

	char const* artapp_env = getenv("ARTDAQ_RANK");
	if (artapp_env != nullptr && my_rank < 0)
	{
		my_rank = strtol(artapp_env, nullptr, 10);
	}

	while (my_rank == -1 && artdaq::TimeUtils::GetElapsedTime(start_time) < init_timeout_s_)
	{
		usleep(1000);
	}
	sender_ptr_ = std::make_unique<artdaq::DataSenderManager>(data_pset_);
}

void art::RootNetOutput::disconnect()
{
	if (sender_ptr_)
	{
		sender_ptr_.reset(nullptr);
	}
}

DEFINE_ART_MODULE(art::RootNetOutput)  // NOLINT(performance-unnecessary-value-param)
