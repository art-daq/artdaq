#include "TRACE/tracemf.h"
#define TRACE_NAME "TransferOutputReliable"
#include "artdaq/ArtModules/ArtdaqOutput.hh"

#include <csignal>
#include "art/Framework/Core/ModuleMacros.h"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq/TransferPlugins/TransferInterface.hh"

namespace art {
class TransferOutputReliable;
}

/**
 * \brief An art::OutputModule which sends events using DataSenderManager.
 * This module is designed for transporting Fragment-wrapped art::Events after
 * they have been read into art, for example between the EventBuilder and the Aggregator.
 */
class art::TransferOutputReliable : public ArtdaqOutput
{
public:
	/**
	 * \brief TransferOutputReliable Constructor
	 * \param ps ParameterSet used to configure TransferOutputReliable
	 *
	 * TransferOutputReliable accepts no Parameters beyond those which art::OutputModule takes.
	 * See the art::OutputModule documentation for more details on those Parameters.
	 */
	explicit TransferOutputReliable(fhicl::ParameterSet const& ps);

	/**
	 * \brief TransferOutputReliable Destructor
	 */
	~TransferOutputReliable() override;

protected:
	/// <summary>
	/// Send a message using the Transfer Plugin
	/// </summary>
	/// <param name="fragment">Fragment to send</param>
	void SendMessage(artdaq::FragmentPtr& fragment) override;

private:
	TransferOutputReliable(TransferOutputReliable const&) = delete;
	TransferOutputReliable(TransferOutputReliable&&) = delete;
	TransferOutputReliable& operator=(TransferOutputReliable const&) = delete;
	TransferOutputReliable& operator=(TransferOutputReliable&&) = delete;

	std::unique_ptr<artdaq::TransferInterface> transfer_;
};

art::TransferOutputReliable::TransferOutputReliable(fhicl::ParameterSet const& ps)
    : ArtdaqOutput(ps)
{
	TLOG(TLVL_DEBUG + 32) << "Begin: TransferOutputReliable::TransferOutputReliable(ParameterSet const& ps)";
	transfer_ = artdaq::MakeTransferPlugin(ps, "transfer_plugin", artdaq::TransferInterface::Role::kSend);
	TLOG(TLVL_DEBUG + 32) << "END: TransferOutputReliable::TransferOutputReliable";
}

art::TransferOutputReliable::~TransferOutputReliable()
{
	TLOG(TLVL_DEBUG + 32) << "Begin: TransferOutputReliable::~TransferOutputReliable()";

	auto sts = transfer_->transfer_fragment_reliable_mode(std::move(*artdaq::Fragment::eodFrag(0)));
	if (sts != artdaq::TransferInterface::CopyStatus::kSuccess)
	{
		TLOG(TLVL_ERROR) << "Error sending EOD Fragment!";
	}
	transfer_.reset(nullptr);
	TLOG(TLVL_DEBUG + 32) << "End: TransferOutputReliable::~TransferOutputReliable()";
}

void art::TransferOutputReliable::SendMessage(artdaq::FragmentPtr& fragment)
{
	auto seqID = fragment->sequenceID();
	auto type = static_cast<int>(fragment->type());
	auto length = fragment->dataSizeBytes();
	TLOG(TLVL_DEBUG + 32) << "Sending message with sequenceID=" << fragment->sequenceID() << ", type=" << static_cast<int>(fragment->type())
	                      << ", length=" << fragment->dataSizeBytes();
	auto sts = artdaq::TransferInterface::CopyStatus::kErrorNotRequiringException;
	sts = transfer_->transfer_fragment_reliable_mode(std::move(*fragment));
	if(sts != artdaq::TransferInterface::CopyStatus::kSuccess) 
	{
TLOG(TLVL_ERROR) << "Error sending Fragment " << seqID << " of type " << type << " and length " << length;
	}
}

DEFINE_ART_MODULE(art::TransferOutputReliable)  // NOLINT(performance-unnecessary-value-param)
