#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Principal/Handle.h"
#include "art/Persistency/Common/GroupQueryResult.h"
#ifdef CANVAS
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#else
#include "art/Utilities/DebugMacros.h"
#include "art/Utilities/Exception.h"
#endif
#include "fhiclcpp/ParameterSet.h"

#include "artdaq/DAQrate/SHandles.hh"
#include "artdaq-core/Data/Fragments.hh"

#define TRACE_NAME "BinaryMPIOutput"
#include "tracelib.h"		// TRACE

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include "unistd.h"

#if ART_MAJOR_VERSION == 1 && ART_MINOR_VERSION >= 16 || ART_MAJOR_VERSION > 1
#  define CONST_WRITE
struct Config {
  fhicl::Atom<uint64_t> max_fragment_size_words  { fhicl::Name("max_fragment_size_words") };
  fhicl::Atom<size_t>   mpi_buffer_count         { fhicl::Name("mpi_buffer_count") };
  fhicl::Atom<size_t>   first_evb_rank           { fhicl::Name("first_event_builder_rank")};
  fhicl::Atom<size_t>   evb_count                { fhicl::Name("event_builder_count")};
  fhicl::Atom<int>      rt_priority              { fhicl::Name("rt_priority"), 0};
  fhicl::Atom<bool>     synchronous_sends        { fhicl::Name("synchronous_sends"), true};

};
#else
#  define CONST_WRITE const
#endif

namespace art {
class BinaryMPIOutput;
}

using art::BinaryMPIOutput;
using fhicl::ParameterSet;

class art::BinaryMPIOutput final: public OutputModule {
public:
    explicit BinaryMPIOutput(ParameterSet const&);
    ~BinaryMPIOutput();    
private:
    void beginJob() override;
    void endJob() override;
    void write(EventPrincipal CONST_WRITE&) override;
    void writeRun(RunPrincipal CONST_WRITE &) override {};
    void writeSubRun(SubRunPrincipal CONST_WRITE &) override {};

    void initialize_MPI_();
    void deinitialize_MPI_();
    bool readParameterSet_(fhicl::ParameterSet const& pset);
private:
  std::string name_="BinaryMPIOutput";
  uint64_t max_fragment_size_words_=0;
  size_t mpi_buffer_count_=0;
  size_t first_evb_rank_=0;
  size_t evb_count_=0;
  int rt_priority_=0;
  bool synchronous_sends_=true;    
  std::unique_ptr<artdaq::SHandles> sender_ptr_= {nullptr};
};

art::BinaryMPIOutput::
BinaryMPIOutput(ParameterSet const& ps)
#if (ART_MAJOR_VERSION == 1 && ART_MINOR_VERSION >= 18) || (ART_MAJOR_VERSION == 1 && ART_MINOR_VERSION == 17 && ART_PATCH_VERSION >= 8) || ART_MAJOR_VERSION > 1
  : OutputModule(ps)
#elif ART_MAJOR_VERSION == 1 && ART_MINOR_VERSION >= 16
  : OutputModule(OutputModule::Table<Config>(ps))
#else
	: OutputModule(ps)
#endif
{
    FDEBUG(1) << "Begin: BinaryMPIOutput::BinaryMPIOutput(ParameterSet const& ps)\n";
    readParameterSet_(ps); 
    FDEBUG(1) << "End: BinaryMPIOutput::BinaryMPIOutput(ParameterSet const& ps)\n";      
}

art::BinaryMPIOutput::
~BinaryMPIOutput()
{
    FDEBUG(1) << "Begin/End: BinaryMPIOutput::~BinaryMPIOutput()\n";
}

void 
art::BinaryMPIOutput::
beginJob()
{
    FDEBUG(1) << "Begin: BinaryMPIOutput::beginJob()\n";
    initialize_MPI_();
    FDEBUG(1) << "End:   BinaryMPIOutput::beginJob()\n";
}

void
art::BinaryMPIOutput::
endJob()
{
    FDEBUG(1) << "Begin: BinaryMPIOutput::endJob()\n";
    deinitialize_MPI_();
    FDEBUG(1) << "End:   BinaryMPIOutput::endJob()\n";
}



void
art::BinaryMPIOutput::
initialize_MPI_(){
  if (rt_priority_ > 0) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
    sched_param s_param = {};
    s_param.sched_priority = rt_priority_;
    int status = pthread_setschedparam(pthread_self(), SCHED_RR, &s_param);
    if (status != 0) {
      mf::LogError(name_)
        << "Failed to set realtime priority to " << rt_priority_
        << ", return code = " << status;
    }
#pragma GCC diagnostic pop
  }

  TRACE( 3, "BinaryMPIOutput::initializeMPI(mpi_buffer_count=%lu max_fragment_size_words=%lu first_evb_rank=%lu evb_count=%lu synchronous_sends=%i )"\
					    ,mpi_buffer_count_ ,  max_fragment_size_words_,   first_evb_rank_,   evb_count_, int(synchronous_sends_));

  sender_ptr_=std::make_unique<artdaq::SHandles>(mpi_buffer_count_,
                                         max_fragment_size_words_,
                                         evb_count_,
                                         first_evb_rank_,
                                         false,
                                         synchronous_sends_);
  assert(sender_ptr_);
}

void
art::BinaryMPIOutput::
deinitialize_MPI_() {
  sender_ptr_.reset(nullptr);
}

bool
art::BinaryMPIOutput::
readParameterSet_(fhicl::ParameterSet const& pset)
{
  mf::LogDebug(name_) << "BinaryMPIOutput::readParameterSet_ method called with "
                                   << "ParameterSet = \"" << pset.to_string()
                                   << "\".";

  // determine the data sending parameters
  try {
    max_fragment_size_words_ = pset.get<uint64_t>("max_fragment_size_words");
  }
  catch (...) {
    mf::LogError(name_)
      << "The max_fragment_size_words parameter was not specified "
      << "in the BinaryMPIOutput initialization PSet: \""
      << pset.to_string() << "\".";
    return false;
  }
  try {mpi_buffer_count_ = pset.get<size_t>("mpi_buffer_count");}
  catch (...) {
    mf::LogError(name_)
      << "The mpi_buffer_count parameter was not specified "
      << "in the fragment_receiver initialization PSet: \""
      << pset.to_string() << "\".";
    return false;
  }
  try {first_evb_rank_ = pset.get<size_t>("first_event_builder_rank");}
  catch (...) {
    mf::LogError(name_)
      << "The first_event_builder_rank parameter was not specified "
      << "in the fragment_receiver initialization PSet: \""
      << pset.to_string() << "\".";
    return false;
  }
  try {evb_count_ = pset.get<size_t>("event_builder_count");}
  catch (...) {
    mf::LogError(name_)
      << "The event_builder_count parameter was not specified "
      << "in the fragment_receiver initialization PSet: \""
      << pset.to_string() << "\".";
    return false;
  }
  rt_priority_ = pset.get<int>("rt_priority", 0);
  synchronous_sends_ = pset.get<bool>("synchronous_sends", true);

  TRACE( 4, "BinaryMPIOutput::readParameterSet()");

  return true;
}

void
art::BinaryMPIOutput::
write(CONST_WRITE EventPrincipal& ep)
{  
    assert(sender_ptr_);
    
    using RawEvent  = artdaq::Fragments;;
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
	auto fragment_copy=fragment;
	auto fragid_id = fragment_copy.fragmentID();
	auto sequence_id = fragment_copy.sequenceID();
	TRACE( 1, "BinaryMPIOutput::write seq=%lu frag=%i start",  sequence_id, fragid_id);
	sender_ptr_->sendFragment(std::move(fragment_copy));
	TRACE( 2, "BinaryMPIOutput::write seq=%lu frag=%i done",  sequence_id, fragid_id);
      }
    }
    
    return;
}

DEFINE_ART_MODULE(art::BinaryMPIOutput)

