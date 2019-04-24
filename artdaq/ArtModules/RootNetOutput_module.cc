
#define TRACE_NAME (app_name + "_RootNetOutput").c_str()
#include "artdaq/ArtModules/ArtdaqOutput.hh"

#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq/DAQdata/NetMonHeader.hh"

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
	~RootNetOutput();

protected:
	/// <summary>
	/// Send a message using DataSenderManager
	/// </summary>
	/// <param name="sequence_id">Sequence ID of message</param>
	/// <param name="messageType">Type of message</param>
	/// <param name="msg">Contents of message</param>
	virtual void SendMessage(artdaq::Fragment::sequence_id_t sequence_id, artdaq::Fragment::type_t messageType, TBufferFile& msg);
};

art::RootNetOutput::RootNetOutput(fhicl::ParameterSet const& ps)
    : ArtdaqOutput(ps)
{
	TLOG(TLVL_DEBUG) << "Begin: RootNetOutput::RootNetOutput(ParameterSet const& ps)";
	ServiceHandle<NetMonTransportService> transport;
	transport->connect();
	TLOG(TLVL_DEBUG) << "End:   RootNetOutput::RootNetOutput(ParameterSet const& ps)";
}

art::RootNetOutput::~RootNetOutput()
{
	TLOG(TLVL_DEBUG) << "Begin: RootNetOutput::~RootNetOutput()";
	ServiceHandle<NetMonTransportService> transport;
	transport->disconnect();
	TLOG(TLVL_DEBUG) << "End:   RootNetOutput::~RootNetOutput()";
}

void art::RootNetOutput::SendMessage(artdaq::Fragment::sequence_id_t sequence_id, artdaq::Fragment::type_t messageType, TBufferFile& msg)
{
	//
	//  Send message.
	//
	{
		ServiceHandle<NetMonTransportService> transport;
		if (!transport.get())
		{
			TLOG(TLVL_ERROR) << "Could not get handle to NetMonTransportService!";
			return;
		}
		TLOG(TLVL_WRITE) << "RootNetOutput::SendMessage Sending a message with type code "
		                 << artdaq::detail::RawFragmentHeader::SystemTypeToString(messageType);
		transport->sendMessage(sequence_id, messageType, msg);
		TLOG(TLVL_WRITE) << "RootNetOutput::SendMessage: Message sent.";
	}
}

DEFINE_ART_MODULE(art::RootNetOutput)
