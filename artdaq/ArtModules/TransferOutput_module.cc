#define TRACE_NAME "TransferOutput"
#include "artdaq/ArtModules/ArtdaqOutput.hh"

#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"


namespace art {
class TransferOutput;
}

/**
 * \brief An art::OutputModule which sends events using DataSenderManager.
 * This module is designed for transporting Fragment-wrapped art::Events after
 * they have been read into art, for example between the EventBuilder and the Aggregator.
 */
class art::TransferOutput : public ArtdaqOutput {
 public:
  /**
   * \brief TransferOutput Constructor
   * \param ps ParameterSet used to configure TransferOutput
   *
   * TransferOutput accepts no Parameters beyond those which art::OutputModule takes.
   * See the art::OutputModule documentation for more details on those Parameters.
   */
  explicit TransferOutput(fhicl::ParameterSet const& ps);

  /**
   * \brief TransferOutput Destructor
   */
  ~TransferOutput();

 protected:

	 /// <summary>
	 /// Send a message using the Transfer Plugin
	 /// </summary>
	 /// <param name="sequenceId">Sequence ID of message</param>
	 /// <param name="messageType">Type of message</param>
	 /// <param name="msg">Contents of message</param>
  virtual void SendMessage(artdaq::Fragment::sequence_id_t sequenceId, artdaq::Fragment::type_t messageType, TBufferFile & msg);

 private:
  size_t send_timeout_us_;
  size_t send_retry_count_;
  std::unique_ptr<artdaq::TransferInterface> transfer_;

};

art::TransferOutput::TransferOutput(fhicl::ParameterSet const& ps)
    : ArtdaqOutput(ps),
      send_timeout_us_(ps.get<size_t>("send_timeout_us", 5000000)),
      send_retry_count_(ps.get<size_t>("send_retry_count", 5)) {
  TLOG(TLVL_DEBUG) << "Begin: TransferOutput::TransferOutput(ParameterSet const& ps)";
  transfer_ = artdaq::MakeTransferPlugin(ps, "transfer_plugin", artdaq::TransferInterface::Role::kSend);
  TLOG(TLVL_DEBUG) << "END: TransferOutput::TransferOutput";
}

art::TransferOutput::~TransferOutput() {
  TLOG(TLVL_DEBUG) << "Begin: TransferOutput::~TransferOutput()";

  auto sts = transfer_->transfer_fragment_reliable_mode(std::move(*artdaq::Fragment::eodFrag(0)));
  if (sts != artdaq::TransferInterface::CopyStatus::kSuccess) TLOG(TLVL_ERROR) << "Error sending EOD Fragment!";
  transfer_.reset(nullptr);
  TLOG(TLVL_DEBUG) << "End: TransferOutput::~TransferOutput()";
}

void art::TransferOutput::SendMessage(artdaq::Fragment::sequence_id_t sequenceId, artdaq::Fragment::type_t messageType, TBufferFile & msg) {
  TLOG(TLVL_DEBUG) << "Sending message with sequenceID=" << sequenceId << ", type=" << (int)messageType
                   << ", length=" << msg.Length();
  artdaq::NetMonHeader header;
  header.data_length = static_cast<uint64_t>(msg.Length());
  artdaq::Fragment fragment(std::ceil(msg.Length() / static_cast<double>(sizeof(artdaq::RawDataType))), sequenceId, 0,
                            messageType, header);

  memcpy(&*fragment.dataBegin(), msg.Buffer(), msg.Length());
  auto sts = artdaq::TransferInterface::CopyStatus::kErrorNotRequiringException;
  size_t retries = 0;
  while (sts != artdaq::TransferInterface::CopyStatus::kSuccess && retries <= send_retry_count_) {
    sts = transfer_->transfer_fragment_min_blocking_mode(fragment, send_timeout_us_);
    retries++;
  }

#if 0
	if (messageType == artdaq::Fragment::InitFragmentType)
	{
		std::fstream ostream("sendInitMessage_TransferOutput.bin", std::ios::out | std::ios::binary);
		ostream.write(msg.Buffer(), msg.Length());
		ostream.close();
	}
#endif
}

DEFINE_ART_MODULE(art::TransferOutput)
