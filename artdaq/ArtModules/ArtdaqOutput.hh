
#include <TBufferFile.h>
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/OutputHandle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"

#define TRACE_NAME "ArtdaqOutput"
#include "artdaq/DAQdata/Globals.hh"

namespace art {
class ArtdaqOutput;
}

class art::ArtdaqOutput : public art::OutputModule 
{
public:
explicit ArtdaqOutput(fhicl::ParameterSet const& ps) : OutputModule(ps) {}

virtual ~RootNetOutput();

protected:
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

	TBufferFile make_init_message();

private:
bool initMsgSent_;
ProductList productList_;
}
