#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/Source.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"

#include "art/Persistency/Provenance/BranchIDListHelper.h"
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "art/Persistency/Provenance/MasterProductRegistry.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductMetaData.h"

#ifdef CANVAS
#include "canvas/Persistency/Common/EDProduct.h"
#include "canvas/Persistency/Common/Wrapper.h"
#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/BranchIDList.h"
#include "canvas/Persistency/Provenance/BranchKey.h"
#include "canvas/Persistency/Provenance/History.h"
#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ProcessConfiguration.h"
#include "canvas/Persistency/Provenance/ProcessHistory.h"
#include "canvas/Persistency/Provenance/ProcessHistoryID.h"
#include "canvas/Persistency/Provenance/ProductList.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"
#include "canvas/Utilities/DebugMacros.h"
#else
#include "art/Persistency/Common/EDProduct.h"
#include "art/Persistency/Common/Wrapper.h"
#include "art/Persistency/Provenance/BranchDescription.h"
#include "art/Persistency/Provenance/BranchIDList.h"
#include "art/Persistency/Provenance/BranchKey.h"
#include "art/Persistency/Provenance/History.h"
#include "art/Persistency/Provenance/ParentageRegistry.h"
#include "art/Persistency/Provenance/ProcessConfiguration.h"
#include "art/Persistency/Provenance/ProcessHistory.h"
#include "art/Persistency/Provenance/ProcessHistoryID.h"
#include "art/Persistency/Provenance/ProductList.h"
#include "art/Persistency/Provenance/ProductProvenance.h"
#include "art/Utilities/DebugMacros.h"
#endif

#include "fhiclcpp/make_ParameterSet.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "TClass.h"
#include "TMessage.h"
#include "TBufferFile.h"

#include "artdaq/ArtModules/InputUtilities.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <sys/time.h>
#include <iostream>

// JCF, May-27-2016

// ArtdaqInput is a template class which takes, as a parameter, a
// class which it uses to receive data; the instance of this class is
// called "communicationWrapper_". As of this writing, this wrapper
// class is implemented by NetMonWrapper (for reading data into the
// aggregator from the eventbuilder) and TransferWrapper (for reading
// data into an art process). This class presents a unified approach
// to handling art provenance, regardless of the communication
// protocol used to read data in.

namespace art {
  template <typename U>
  class ArtdaqInput;
}

template <typename U>
class art::ArtdaqInput {
public:
    ArtdaqInput(const ArtdaqInput&) = delete;
    ArtdaqInput& operator=(const ArtdaqInput&) = delete;

    ~ArtdaqInput();

    ArtdaqInput(const fhicl::ParameterSet&, art::ProductRegistryHelper&,
                      const art::SourceHelper&);

    void closeCurrentFile();

    void readFile(const std::string&, art::FileBlock*&);

    bool hasMoreData() const;

    bool readNext(art::RunPrincipal* const inR,
                  art::SubRunPrincipal* const inSR, art::RunPrincipal*& outR,
                  art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE);

private:
    void
        readAndConstructPrincipal(std::unique_ptr<TBufferFile>&, 
			      unsigned long,
                              art::RunPrincipal* const,
                              art::SubRunPrincipal* const,
                              art::RunPrincipal*&,
                              art::SubRunPrincipal*&,
                              art::EventPrincipal*&);

    template <class T>
    void readDataProducts(std::unique_ptr<TBufferFile>&, T*&);

  void putInPrincipal(RunPrincipal*&, std::unique_ptr<EDProduct>&&, const BranchDescription&, std::unique_ptr<const ProductProvenance>&&);
  void putInPrincipal(SubRunPrincipal*&, std::unique_ptr<EDProduct>&&, const BranchDescription&, std::unique_ptr<const ProductProvenance>&&);
  void putInPrincipal(EventPrincipal*&, std::unique_ptr<EDProduct>&&, const BranchDescription&, std::unique_ptr<const ProductProvenance>&&);


private:
    bool shutdownMsgReceived_;
    bool outputFileCloseNeeded_;
    const art::SourceHelper& pm_;
  U communicationWrapper_;
};

template <typename U>
art::ArtdaqInput<U>::
ArtdaqInput(const fhicl::ParameterSet& ps,
                  art::ProductRegistryHelper& helper,
                  const art::SourceHelper& pm)
  : shutdownMsgReceived_(false), 
    outputFileCloseNeeded_(false), 
    pm_(pm),
    communicationWrapper_(ps)
{
  
  // JCF, May-27-2016

  // Something will have to be done about the labeling of this class,
  // since it's just a template class- the user will care about the
  // specific instantiation when it comes to messages

    mf::LogDebug("ArtdaqInput") << "Begin: ArtdaqInput::ArtdaqInput("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
    (void) helper;

    std::unique_ptr<TBufferFile> msg( nullptr );
    communicationWrapper_.receiveMessage( msg );

    if (!msg) {
      throw art::Exception(art::errors::DataCorruption) <<
	"ArtdaqInput: Could not receive message!";
    }

    // This first unsigned long is the message type code, ignored here in the constructor
    unsigned long dummy = 0;
    msg->ReadULong(dummy);

    //
    //  Read the ParameterSetRegistry.
    //
    unsigned long ps_cnt = 0;
    msg->ReadULong(ps_cnt);
    mf::LogDebug("ArtdaqInput") << "ArtdaqInput: parameter set count: " << ps_cnt << '\n';
    mf::LogDebug("ArtdaqInput") << "ArtdaqInput: reading parameter sets ...\n";
    for (unsigned long I = 0; I < ps_cnt; ++I) {

      std::string* pset_str = ReadObjectAny<std::string>(msg, "std::string", "ArtdaqInput::ArtdaqInput");

        fhicl::ParameterSet pset;
        fhicl::make_ParameterSet(*pset_str, pset);
        // Force id calculation.
        pset.id();
        fhicl::ParameterSetRegistry::put(pset);
    }
    mf::LogDebug("ArtdaqInput") << "ArtdaqInput: finished reading parameter sets.\n";

    //
    //  Read the MasterProductRegistry.
    //

    art::ProductList* productlist = ReadObjectAny<art::ProductList>(msg, "map<art::BranchKey,art::BranchDescription>", "ArtdaqInput::ArtdaqInput");
    helper.productList( productlist );

    mf::LogDebug("ArtdaqInput") << "ArtdaqInput: got product list\n";
    if (art::debugit() >= 1) {
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput: before BranchIDLists\n";
        BranchIDLists* bil = &BranchIDListRegistry::instance()->data();
        int max_bli = bil->size();
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput: max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
            int max_prdidx = (*bil)[i].size();
            mf::LogDebug("ArtdaqInput") << "ArtdaqInput: max_prdidx: "
                      << max_prdidx << '\n';
            for (int j = 0; j < max_prdidx; ++j) {
                mf::LogDebug("ArtdaqInput") << "ArtdaqInput:"
                          << " bli: "
                          << i
                          << " prdidx: "
                          << j
                          << " bid: 0x"
                          << std::hex
                          << static_cast<unsigned long>((*bil)[i][j])
                          << std::dec
                          << '\n';
            }
        }
    }

    art::ProcessHistoryMap* phm = ReadObjectAny<art::ProcessHistoryMap>(msg, "std::map<const art::Hash<2>,art::ProcessHistory>", "ArtdaqInput::ArtdaqInput");

    printProcessMap(*phm, "ArtdaqInput's ProcessHistoryMap");

    ProcessHistoryRegistry::put(*phm);

    printProcessMap(ProcessHistoryRegistry::get(), "ArtdaqInput's ProcessHistoryRegistry");
    //
    //  Read the ParentageRegistry.
    //

    art::ParentageMap* parentageMap = ReadObjectAny<art::ParentageMap>(msg, "std::map<const art::Hash<5>,art::Parentage>", "ArtdaqInput::ArtdaqInput");

    mf::LogDebug("ArtdaqInput") << "ArtdaqInput: got ParentageMap\n";
    ParentageRegistry::put(*parentageMap);
    //
    //  Finished with init message.
    //
    mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::ArtdaqInput("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
}

template <typename U>
art::ArtdaqInput<U>::
~ArtdaqInput()
{

}

template <typename U>
void
art::ArtdaqInput<U>::
closeCurrentFile()
{
    mf::LogDebug("ArtdaqInput") << "Begin/End: ArtdaqInput::closeCurrentFile()\n";
}

template <typename U>
void
art::ArtdaqInput<U>::
readFile(const std::string& name, art::FileBlock*& fb)
{
    mf::LogDebug("ArtdaqInput") << "Begin: ArtdaqInput::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
    (void) name;
    fb = new art::FileBlock(art::FileFormatVersion(1, "ArtdaqInput2013"),
                            "nothing");
    mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
}

template <typename U>
bool
art::ArtdaqInput<U>::
hasMoreData() const
{
    mf::LogDebug("ArtdaqInput") << "Begin: ArtdaqInput::hasMoreData()\n";
    if (shutdownMsgReceived_) {
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput::hasMoreData(): "
                     "returning false on shutdownMsgReceived_.\n";
        mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::hasMoreData()\n";
        return false;
    }
    mf::LogDebug("ArtdaqInput") << "ArtdaqInput::hasMoreData(): "
                 "returning true on not shutdownMsgReceived_.\n";
    mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::hasMoreData()\n";
    return true;
}

template <typename U>
void
art::ArtdaqInput<U>::
readAndConstructPrincipal(std::unique_ptr<TBufferFile>& msg, 
			  unsigned long msg_type_code,
                          art::RunPrincipal* const inR,
                          art::SubRunPrincipal* const inSR,
                          art::RunPrincipal*& outR,
                          art::SubRunPrincipal*& outSR,
                          art::EventPrincipal*& outE)
{
    //
    //  Process the message.
    //
    std::unique_ptr<art::RunAuxiliary> run_aux;
    std::unique_ptr<art::SubRunAuxiliary> subrun_aux;
    std::unique_ptr<art::EventAuxiliary> event_aux;
    std::shared_ptr<History> history;

    if (msg_type_code == 2) {         // EndRun message.

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"processing EndRun message ...\n";

      run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary", 
						     "ArtdaqInput::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", run_aux.get() );

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"making flush RunPrincipal ...\n";
      outR = pm_.makeRunPrincipal(RunID::flushRun(), run_aux->beginTime());

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"making flush SubRunPrincipal ...\n";
      outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(),
				      run_aux->beginTime());

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"making flush EventPrincipal ...\n";
      outE = pm_.makeEventPrincipal(EventID::flushEvent(),
				    run_aux->endTime(), true,
				    EventAuxiliary::Any);

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"finished processing EndRun message.\n";

    }
    else if (msg_type_code == 3) {         // EndSubRun message.

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"processing EndSubRun message ...\n";

      subrun_aux.reset(ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary",
							   "ArtdaqInput::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get() );

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"making flush RunPrincipal ...\n";
      outR = pm_.makeRunPrincipal(RunID::flushRun(), subrun_aux->beginTime());

      // 28-Feb-2014, KAB: added the setting of the end time in the *current*
      // run and subrun.  This is how we set the endTime in the RunPrincipal
      // and SubRunPrincipal objects that are written to the disk file.
      // This needs to happen here because:
      // A) setting them in every event doesn't work because the RunPrincipal
      //    only allows us to set an end time value once
      // B) setting them in the "outputFileCloseNeeded_" block in the readNext()
      //    method below doesn't work because that is too late.  When this
      //    method returns an outR with a different run number (flushRun),
      //    the art output system closes the current file then.
      // C) setting them in the EndRun message block immediately above this
      //    block wouldn't work because a) we're not currently sending endRun
      //    events from the EBs to the AG, and b) because presumably that would
      //    be too late, also.

      art::Timestamp currentTime = time(0);
      if (inR != nullptr) {inR->setEndTime(currentTime);}
      if (inSR != nullptr) {inSR->setEndTime(currentTime);}

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"making flush SubRunPrincipal ...\n";
      outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(),
				      subrun_aux->beginTime());

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"making flush EventPrincipal ...\n";
      outE = pm_.makeEventPrincipal(EventID::flushEvent(),
				    subrun_aux->endTime(), true,
				    EventAuxiliary::Any);

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"finished processing EndSubRun message.\n";
    }
    else if (msg_type_code == 4) {    // Event message.

      mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
	"processing Event message ...\n";

      run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary",
						     "ArtdaqInput::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", run_aux.get());
            
      subrun_aux.reset(ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary",
							   "ArtdaqInput::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());
	    
      event_aux.reset(ReadObjectAny<art::EventAuxiliary>(msg, "art::EventAuxiliary",
							 "ArtdaqInput::readAndConstructPrincipal"));
      
      history.reset(ReadObjectAny<art::History>(msg, "art::History",
						"ArtdaqInput::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", history.get());

      // Can probably improve on error choice of "art::errors:Unknown"
      if (! history->processHistoryID().isValid() ) { 
	throw art::Exception(art::errors::Unknown) <<
	  "readAndConstructPrincipal: processHistoryID of history in "
	  "Event message is invalid!";
      }

      if ((inR == nullptr) || !inR->id().isValid() ||
	  (inR->run() != event_aux->run())) {
            // New run, either we have no input RunPrincipal, or the
            // input run number does not match the event run number.
            mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: making RunPrincipal ...\n";
	    outR = pm_.makeRunPrincipal(*run_aux.get());
        }
        if ((inSR == nullptr) || !inSR->id().isValid() ||
                (inSR->subRun() != event_aux->subRun())) {
            // New SubRun, either we have no input SubRunPrincipal, or the
            // input subRun number does not match the event subRun number.
            mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
                         "making SubRunPrincipal ...\n";
	    outSR = pm_.makeSubRunPrincipal(*subrun_aux.get());
        }
        mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: making EventPrincipal ...\n";
	outE = pm_.makeEventPrincipal(*event_aux.get(), std::move(history));

        mf::LogDebug("ArtdaqInput") << "readAndConstructPrincipal: "
                     "finished processing Event message.\n";
    }

}

template <typename U>
template <class T>
void
art::ArtdaqInput<U>::
readDataProducts(std::unique_ptr<TBufferFile>& msg, T*& outPrincipal)
{

    unsigned long prd_cnt = 0;
    {
        mf::LogDebug("ArtdaqInput") << "readDataProducts: reading data product count ...\n";
        msg->ReadULong(prd_cnt);
        mf::LogDebug("ArtdaqInput") << "readDataProducts: product count: " << prd_cnt << '\n';
    }
    //
    //  Read the data products.
    //
    const ProductList& productList = ProductMetaData::instance().productList();

    for (unsigned long I = 0; I < prd_cnt; ++I) {
        std::unique_ptr<BranchKey> bk;

	{
	mf::LogDebug("ArtdaqInput") << "readDataProducts: Reading branch key.\n";
	bk.reset(ReadObjectAny<BranchKey>(msg,"art::BranchKey",
					  "ArtdaqInput::readDataProducts"));
	}
	  
        if (art::debugit() >= 1) {
            mf::LogDebug("ArtdaqInput") << "readDataProducts: got product class: '"
                      << bk->friendlyClassName_
                      << "' modlbl: '"
                      << bk->moduleLabel_
                      << "' instnm: '"
                      << bk->productInstanceName_
                      << "' procnm: '"
                      << bk->processName_
                      << "'\n";
        }
        ProductList::const_iterator iter;
        {
            mf::LogDebug("ArtdaqInput") << "readDataProducts: looking up product ...\n";
            iter = productList.find(*bk);
            if (iter == productList.end()) {
                throw art::Exception(art::errors::InsertFailure)
                        << "No product is registered for\n"
                        << "  process name:                '"
                        << bk->processName_ << "'\n"
                        << "  module label:                '"
                        << bk->moduleLabel_ << "'\n"
                        << "  product friendly class name: '"
                        << bk->friendlyClassName_ << "'\n"
                        << "  product instance name:       '"
                        << bk->productInstanceName_ << "'\n";
            }
        }
        // Note: This must be a reference to the unique copy in
        //       the master product registry!
        const BranchDescription& bd = iter->second;
        std::unique_ptr<EDProduct> prd;
        {
            mf::LogDebug("ArtdaqInput") << "readDataProducts: Reading product.\n";

	    // JCF, May-25-2016
	    // Currently unclear why the templatized version of ReadObjectAny doesn't work here...

	    //	    prd.reset(ReadObjectAny<EDProduct>(msg, bd.wrappedName()));

	    void* p = msg->ReadObjectAny(TClass::GetClass(bd.wrappedName().c_str()));

	    prd.reset(reinterpret_cast<EDProduct*>(p));
	    p = nullptr;

        }
        std::unique_ptr<const ProductProvenance> prdprov;
        {
            mf::LogDebug("ArtdaqInput") << "readDataProducts: Reading product provenance.\n";
	    prdprov.reset(ReadObjectAny<ProductProvenance>(msg,
							   "art::ProductProvenance",
							   "ArtdaqInput::readDataProducts"));
        }
        {
            mf::LogDebug("ArtdaqInput") << "readDataProducts: inserting product: class: '"
                      << bd.friendlyClassName()
                      << "' modlbl: '"
                      << bd.moduleLabel()
                      << "' instnm: '"
                      << bd.productInstanceName()
                      << "' procnm: '"
                      << bd.processName()
                      << "'\n";
	    putInPrincipal(outPrincipal, std::move(prd), bd, std::move(prdprov));
        }
    }
}

template <typename U>
void
art::ArtdaqInput<U>::
putInPrincipal(RunPrincipal*& rp, std::unique_ptr<EDProduct>&& prd, const BranchDescription& bd, std::unique_ptr<const ProductProvenance>&& prdprov)
{
#if (ART_MAJOR_VERSION == 2 && ART_MINOR_VERSION >= 1) || ART_MAJOR_VERSION > 2
  rp->put(std::move(prd), bd, std::move(prdprov), RangeSet::forRun(rp->id()));
#else
  rp->put(std::move(prd), bd, std::move(prdprov));
#endif
}

template <typename U>
void
art::ArtdaqInput<U>::
putInPrincipal(SubRunPrincipal*& srp, std::unique_ptr<EDProduct>&& prd, const BranchDescription& bd, std::unique_ptr<const ProductProvenance>&& prdprov)
 {
#if (ART_MAJOR_VERSION == 2 && ART_MINOR_VERSION >= 1) || ART_MAJOR_VERSION > 2
  srp->put(std::move(prd), bd, std::move(prdprov), RangeSet::forSubRun(srp->id()));
#else
  srp->put(std::move(prd), bd, std::move(prdprov));
#endif
}

template <typename U>
void
art::ArtdaqInput<U>::
putInPrincipal(EventPrincipal*& ep, std::unique_ptr<EDProduct>&& prd, const BranchDescription& bd, std::unique_ptr<const ProductProvenance>&& prdprov)
 {
  ep->put(std::move(prd), bd, std::move(prdprov));
}


template <typename U>
bool
art::ArtdaqInput<U>::
readNext(art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR,
         art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR,
         art::EventPrincipal*& outE)
{
    mf::LogDebug("ArtdaqInput") << "Begin: ArtdaqInput::readNext\n";
    if (outputFileCloseNeeded_) {
        outputFileCloseNeeded_ = false;
        // Signal that we need the output file closed by returning false,
        // but answering true to the hasMoreData() query.
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput::readNext: "
                     "returning false on outputFileCloseNeeded_\n";
        mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::readNext\n";
        return false;
    }

    std::unique_ptr<TBufferFile> msg;
    communicationWrapper_.receiveMessage(msg);

    if (!msg) {
      shutdownMsgReceived_ = true;
      return false;
    }

    //
    //  Read message type code.
    //
    unsigned long msg_type_code = 0;
    {
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput::readNext: "
                     "getting message type code ...\n";
        msg->ReadULong(msg_type_code);
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput::readNext: "
                     "message type: " <<  msg_type_code << '\n';
    }
    if (msg_type_code == 5) {
        // Shutdown message.
        shutdownMsgReceived_ = true;
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput::readNext: "
                     "returning false on Shutdown message.\n";
        mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::readNext\n";
        return false;
    }

    readAndConstructPrincipal(msg, msg_type_code, inR, inSR, outR,
                              outSR, outE);
    //
    //  Read per-event metadata needed to construct principal.
    //
    if (msg_type_code == 2) {
        // EndRun message.
        // FIXME: We need to merge these into the input RunPrincipal.
      readDataProducts(msg, outR);
        // Signal that we should close the input and output file.
        mf::LogDebug("ArtdaqInput") << "ArtdaqInput::readNext: "
                     "returning false on EndRun message.\n";
        mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::readNext\n";
        return false;
    }
    else if (msg_type_code == 3) {
        // EndSubRun message.
	// From the code above, EndRun and EndSubRun messages cause
	// the construction of principals that have:
	//    Run:Subrun:Event=flush:flush:flush.
	// This is a problem when you have two neighboring EndSubRuns
	// which are both associated with empty subruns because art will
	// complain that you a new subrun with a subrun number identical
	// to that of the previous subrun.  So the solution is to not
	// return new principals.
        if(inR!=0 && inSR!=0 && outR!=0 && outSR!=0){
	  if( inR->id().isFlush() &&  inSR->id().isFlush() &&
	      outR->id().isFlush() && outSR->id().isFlush()){
            outR=0;
	    outSR=0;
            outputFileCloseNeeded_ = true;
	    return true;
	  }
	}
        // FIXME: We need to merge these into the input SubRunPrincipal.
        readDataProducts(msg, outSR);
        // Remember that we should ask for file close next time
        // we are called.
        outputFileCloseNeeded_ = true;
        mf::LogDebug("ArtdaqInput") << "readNext: returning true on EndSubRun message.\n";
        mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::readNext\n";
        return true;
    }
    else if (msg_type_code == 4) {
        // Event message.
      readDataProducts(msg, outE);
      mf::LogDebug("ArtdaqInput") << "readNext: returning true on Event message.\n";
      mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::readNext\n";
        return true;
    }
    // Unknown message.
    // FIXME: What do we throw here for invalid message code?
    mf::LogDebug("ArtdaqInput") << "readNext: returning false on unknown msg_type_code!\n";
    mf::LogDebug("ArtdaqInput") << "End:   ArtdaqInput::readNext\n";
    return false;
}
