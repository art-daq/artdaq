#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"

#include "artdaq-core/Data/Fragments.hh"

#define TRACE_NAME "BinaryFileOutput"
#include "tracelib.h"		// TRACE

#include <iomanip>
#include <iostream>
#include <fstream> 
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include "unistd.h"
#include <stdio.h>

struct Config {
  fhicl::Atom<std::string> fileName { fhicl::Name("fileName") };
};

namespace art {
class BinaryFileOutput;
}

using art::BinaryFileOutput;
using fhicl::ParameterSet;

class art::BinaryFileOutput final: public OutputModule {
public:
    explicit BinaryFileOutput(ParameterSet const&);
    ~BinaryFileOutput();    
private:
    void beginJob() override;
    void endJob() override;
    void write(EventPrincipal&) override;
    void writeRun(RunPrincipal&) override {};
    void writeSubRun(SubRunPrincipal&) override {};

    void initialize_FILE_();
    void deinitialize_FILE_();
    bool readParameterSet_(fhicl::ParameterSet const& pset);

private:
  std::string name_="BinaryFileOutput";
  std::string file_name_="/tmp/artdaqdemo.binary";
  bool do_direct_=false;
  int fd_=-1;					// Used for direct IO
  std::unique_ptr<std::ofstream> file_ptr_= {nullptr};
};
                                         
art::BinaryFileOutput::
BinaryFileOutput(ParameterSet const& ps)
  : OutputModule(ps)
{
    FDEBUG(1) << "Begin: BinaryFileOutput::BinaryFileOutput(ParameterSet const& ps)\n";    
    readParameterSet_(ps); 
    FDEBUG(1) << "End: BinaryFileOutput::BinaryFileOutput(ParameterSet const& ps)\n";      
}

art::BinaryFileOutput::
~BinaryFileOutput()
{
    FDEBUG(1) << "Begin/End: BinaryFileOutput::~BinaryFileOutput()\n";
}

void 
art::BinaryFileOutput::
beginJob()
{
    FDEBUG(1) << "Begin: BinaryFileOutput::beginJob()\n";
    initialize_FILE_();
    FDEBUG(1) << "End:   BinaryFileOutput::beginJob()\n";
}

void
art::BinaryFileOutput::
endJob()
{
    FDEBUG(1) << "Begin: BinaryFileOutput::endJob()\n";
    deinitialize_FILE_();
    FDEBUG(1) << "End:   BinaryFileOutput::endJob()\n";
}



void
art::BinaryFileOutput::
initialize_FILE_()
{
	if (do_direct_) {
		fd_ = open( file_name_.c_str(), O_WRONLY|O_CREAT|O_DIRECT, 0660 );
		TRACE( 3, "BinaryFileOutput::initialize_FILE_ fd_=%d", fd_ );
	} else {
		file_ptr_= std::make_unique<std::ofstream>(file_name_,std::ofstream::binary);
		file_ptr_->rdbuf()->pubsetbuf(0, 0);
	}
}

void
art::BinaryFileOutput::
deinitialize_FILE_()
{
	if (do_direct_) {
		close(fd_);
		fd_=-1;
	} else
		file_ptr_.reset(nullptr);
}

bool
art::BinaryFileOutput::
readParameterSet_(fhicl::ParameterSet const& pset)
{
  mf::LogDebug(name_) << "BinaryFileOutput::readParameterSet_ method called with "
                                   << "ParameterSet = \"" << pset.to_string()
                                   << "\".";
  // determine the data sending parameters
  try {
    file_name_ = pset.get<std::string>("fileName");
  }
  catch (...) {
    mf::LogError(name_)
      << "The fileName parameter was not specified "
      << "in the BinaryMPIOutput initialization PSet: \""
      << pset.to_string() << "\".";
    return false;
  }
  do_direct_ = pset.get<bool>("directIO",false);
  // determine the data sending parameters
  return true;
}

void
art::BinaryFileOutput::
write(EventPrincipal& ep)
{
    using RawEvent  = artdaq::Fragments;
    using RawEvents = std::vector<RawEvent>;
    using RawEventHandle = art::Handle<RawEvent>;
    using RawEventHandles = std::vector<RawEventHandle>;

    auto result_handles = std::vector<art::GroupQueryResult>();
    ep.getManyByType(art::TypeID(typeid(RawEvent)),result_handles);

    for(auto const& result_handle : result_handles){
		auto const raw_event_handle= RawEventHandle(result_handle);

		if (!raw_event_handle.isValid())
			continue;

		for(auto const& fragment: *raw_event_handle) {
			auto sequence_id = fragment.sequenceID();
			auto fragid_id = fragment.fragmentID();
			TRACE( 1, "BinaryFileOutput::write seq=%lu frag=%i %p bytes=0x%lx start"
			      , sequence_id, fragid_id, fragment.headerBeginBytes(), fragment.sizeBytes() );
			if (do_direct_) {
				ssize_t sts=::write(fd_,reinterpret_cast<const char*>(fragment.headerBeginBytes()),fragment.sizeBytes());
				TRACE( 2, "BinaryFileOutput::write seq=%lu frag=%i done sts=%ld errno=%d"
				      , sequence_id, fragid_id, sts, errno );
			} else {
				file_ptr_->write(reinterpret_cast<const char*>(fragment.headerBeginBytes()),fragment.sizeBytes());
				TRACE( 2, "BinaryFileOutput::write seq=%lu frag=%i done", sequence_id, fragid_id );
			}
		}
    }
    
    return;
}

DEFINE_ART_MODULE(art::BinaryFileOutput)

