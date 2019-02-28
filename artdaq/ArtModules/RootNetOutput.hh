#include "artdaq/ArtModules/ArtdaqOutput.hh"

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

private:
	virtual void openFile(FileBlock const&);

	virtual void closeFile();

	virtual void respondToCloseInputFile(FileBlock const&);

	virtual void respondToCloseOutputFiles(FileBlock const&);

	virtual void endJob();

	virtual void beginRun(RunPrincipal const&);

	virtual void beginSubRun(SubRunPrincipal const&);

	virtual void write(EventPrincipal&);

	virtual void writeRun(RunPrincipal&);

	virtual void writeSubRun(SubRunPrincipal&);

	void writeDataProducts(TBufferFile&, const Principal&, std::vector<BranchKey*>&);

	void extractProducts_(Principal const&);

	void send_init_message();

private:
	bool initMsgSent_;
	ProductList productList_;
};
