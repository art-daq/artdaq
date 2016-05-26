#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/Source.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Persistency/Common/EDProduct.h"
#include "art/Persistency/Common/Wrapper.h"
#include "art/Persistency/Provenance/BranchDescription.h"
#include "art/Persistency/Provenance/BranchIDList.h"
#include "art/Persistency/Provenance/BranchIDListHelper.h"
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "art/Persistency/Provenance/BranchKey.h"
#include "art/Persistency/Provenance/History.h"
#include "art/Persistency/Provenance/MasterProductRegistry.h"
#include "art/Persistency/Provenance/ParentageRegistry.h"
#include "art/Persistency/Provenance/ProcessConfiguration.h"
#include "art/Persistency/Provenance/ProcessHistory.h"
#include "art/Persistency/Provenance/ProcessHistoryID.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductList.h"
#include "art/Persistency/Provenance/ProductMetaData.h"
#include "art/Persistency/Provenance/ProductProvenance.h"
#include "art/Utilities/DebugMacros.h"
#include "fhiclcpp/make_ParameterSet.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include "TClass.h"
#include "TMessage.h"
#include "TBufferFile.h"

#include "artdaq/ArtModules/NetMonTransportService.h"
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

namespace art {
class NetMonInputDetail;
}

class art::NetMonInputDetail {
public:
    NetMonInputDetail(const NetMonInputDetail&) = delete;
    NetMonInputDetail& operator=(const NetMonInputDetail&) = delete;

    ~NetMonInputDetail();

    NetMonInputDetail(const fhicl::ParameterSet&, art::ProductRegistryHelper&,
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
    void
    readDataProducts(std::unique_ptr<TBufferFile>&, T*&);

private:
    bool shutdownMsgReceived_;
    bool outputFileCloseNeeded_;
    const art::SourceHelper& pm_;
};

art::NetMonInputDetail::
NetMonInputDetail(const fhicl::ParameterSet& ps,
                  art::ProductRegistryHelper& helper,
                  const art::SourceHelper& pm)
    : shutdownMsgReceived_(false), outputFileCloseNeeded_(false), pm_(pm)
{
    mf::LogDebug("NetMonInput") << "Begin: NetMonInputDetail::NetMonInputDetail("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
    (void) ps;
    (void) helper;

    //
    //  Start server and listen for a connection.
    //
    mf::LogDebug("NetMonInput") << "NetMonInputDetail: Starting server ...\n";
    ServiceHandle<NetMonTransportService> transport;
    transport->listen();
    //
    //  Got a connection, now receive the init message.
    //
    mf::LogDebug("NetMonInput") << "NetMonInputDetail: Connect request received.\n";
    TBufferFile* msg_ptr = 0;
    transport->receiveMessage(msg_ptr);
    mf::LogDebug("NetMonInput") << "NetMonInputDetail: receiveMessage returned.  ptr: 0x"
              << std::hex << (unsigned long) msg_ptr << std::dec << '\n';
    if (msg_ptr == nullptr) {
        throw art::Exception(art::errors::DataCorruption) <<
            "NetMonInputDetail: Could not receive message!";
    }
    std::unique_ptr<TBufferFile> msg(msg_ptr);
    msg_ptr = nullptr;

    // This first unsigned long is the message type code, ignored here in the constructor
    unsigned long dummy = 0;
    msg->ReadULong(dummy);

    //
    //  Read the ParameterSetRegistry.
    //
    unsigned long ps_cnt = 0;
    msg->ReadULong(ps_cnt);
    mf::LogDebug("NetMonInput") << "NetMonInputDetail: parameter set count: " << ps_cnt << '\n';
    mf::LogDebug("NetMonInput") << "NetMonInputDetail: reading parameter sets ...\n";
    for (unsigned long I = 0; I < ps_cnt; ++I) {

      std::string* pset_str = ReadObjectAny<std::string>(msg, "std::string", "NetMonInputDetail::NetMonInputDetail");

        fhicl::ParameterSet pset;
        fhicl::make_ParameterSet(*pset_str, pset);
        // Force id calculation.
        pset.id();
        fhicl::ParameterSetRegistry::put(pset);
    }
    mf::LogDebug("NetMonInput") << "NetMonInputDetail: finished reading parameter sets.\n";

    //
    //  Read the MasterProductRegistry.
    //

    art::ProductList* productlist = ReadObjectAny<art::ProductList>(msg, "map<art::BranchKey,art::BranchDescription>", "NetMonInputDetail::NetMonInputDetail");
    helper.productList( productlist );

    mf::LogDebug("NetMonInput") << "NetMonInputDetail: got product list\n";
    if (art::debugit() >= 1) {
        mf::LogDebug("NetMonInput") << "NetMonInputDetail: before BranchIDLists\n";
        BranchIDLists* bil = &BranchIDListRegistry::instance()->data();
        int max_bli = bil->size();
        mf::LogDebug("NetMonInput") << "NetMonInputDetail: max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
            int max_prdidx = (*bil)[i].size();
            mf::LogDebug("NetMonInput") << "NetMonInputDetail: max_prdidx: "
                      << max_prdidx << '\n';
            for (int j = 0; j < max_prdidx; ++j) {
                mf::LogDebug("NetMonInput") << "NetMonInputDetail:"
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

    art::ProcessHistoryMap* phm = ReadObjectAny<art::ProcessHistoryMap>(msg, "std::map<const art::Hash<2>,art::ProcessHistory>", "NetMonInputDetail::NetMonInputDetail");

    printProcessMap(*phm, "NetMonInputDetail's ProcessHistoryMap");

    ProcessHistoryRegistry::put(*phm);

    printProcessMap(ProcessHistoryRegistry::get(), "NetMonInputDetail's ProcessHistoryRegistry");
    //
    //  Read the ParentageRegistry.
    //

    art::ParentageMap* parentageMap = ReadObjectAny<art::ParentageMap>(msg, "std::map<const art::Hash<5>,art::Parentage>", "NetMonInputDetail::NetMonInputDetail");

    mf::LogDebug("NetMonInput") << "NetMonInputDetail: got ParentageMap\n";
    ParentageRegistry::put(*parentageMap);
    //
    //  Finished with init message.
    //
    mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::NetMonInputDetail("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
}

art::NetMonInputDetail::
~NetMonInputDetail()
{
    mf::LogDebug("NetMonInput") << "Begin: NetMonInputDetail::~NetMonInputDetail()\n";
    ServiceHandle<NetMonTransportService> transport;
    transport->disconnect();
    mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::~NetMonInputDetail()\n";
}

void
art::NetMonInputDetail::
closeCurrentFile()
{
    mf::LogDebug("NetMonInput") << "Begin/End: NetMonInputDetail::closeCurrentFile()\n";
}

void
art::NetMonInputDetail::
readFile(const std::string& name, art::FileBlock*& fb)
{
    mf::LogDebug("NetMonInput") << "Begin: NetMonInputDetail::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
    (void) name;
    fb = new art::FileBlock(art::FileFormatVersion(1, "NetMonInput2013"),
                            "nothing");
    mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
}

bool
art::NetMonInputDetail::
hasMoreData() const
{
    mf::LogDebug("NetMonInput") << "Begin: NetMonInputDetail::hasMoreData()\n";
    if (shutdownMsgReceived_) {
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::hasMoreData(): "
                     "returning false on shutdownMsgReceived_.\n";
        mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::hasMoreData()\n";
        return false;
    }
    mf::LogDebug("NetMonInput") << "NetMonInputDetail::hasMoreData(): "
                 "returning true on not shutdownMsgReceived_.\n";
    mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::hasMoreData()\n";
    return true;
}

void
art::NetMonInputDetail::
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

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"processing EndRun message ...\n";

      run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary", 
						     "NetMonInputDetail::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", run_aux.get() );

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"making flush RunPrincipal ...\n";
      outR = pm_.makeRunPrincipal(RunID::flushRun(), run_aux->beginTime());

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"making flush SubRunPrincipal ...\n";
      outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(),
				      run_aux->beginTime());

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"making flush EventPrincipal ...\n";
      outE = pm_.makeEventPrincipal(EventID::flushEvent(),
				    run_aux->endTime(), true,
				    EventAuxiliary::Any);

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"finished processing EndRun message.\n";

    }
    else if (msg_type_code == 3) {         // EndSubRun message.

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"processing EndSubRun message ...\n";

      subrun_aux.reset(ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary",
							   "NetMonInputDetail::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get() );

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
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

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"making flush SubRunPrincipal ...\n";
      outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(),
				      subrun_aux->beginTime());

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"making flush EventPrincipal ...\n";
      outE = pm_.makeEventPrincipal(EventID::flushEvent(),
				    subrun_aux->endTime(), true,
				    EventAuxiliary::Any);

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"finished processing EndSubRun message.\n";
    }
    else if (msg_type_code == 4) {    // Event message.

      mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
	"processing Event message ...\n";

      run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary",
						     "NetMonInputDetail::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", run_aux.get());
            
      subrun_aux.reset(ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary",
							   "NetMonInputDetail::readAndConstructPrincipal"));
      printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());
	    
      event_aux.reset(ReadObjectAny<art::EventAuxiliary>(msg, "art::EventAuxiliary",
							 "NetMonInputDetail::readAndConstructPrincipal"));
      
      history.reset(ReadObjectAny<art::History>(msg, "art::History",
						"NetMonInputDetail::readAndConstructPrincipal"));
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
            mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: making RunPrincipal ...\n";
	    outR = pm_.makeRunPrincipal(*run_aux.get());
        }
        if ((inSR == nullptr) || !inSR->id().isValid() ||
                (inSR->subRun() != event_aux->subRun())) {
            // New SubRun, either we have no input SubRunPrincipal, or the
            // input subRun number does not match the event subRun number.
            mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
                         "making SubRunPrincipal ...\n";
	    outSR = pm_.makeSubRunPrincipal(*subrun_aux.get());
        }
        mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: making EventPrincipal ...\n";
	outE = pm_.makeEventPrincipal(*event_aux.get(), std::move(history));

        mf::LogDebug("NetMonInput") << "readAndConstructPrincipal: "
                     "finished processing Event message.\n";
    }

}

template <class T>
void
art::NetMonInputDetail::
readDataProducts(std::unique_ptr<TBufferFile>& msg, T*& outPrincipal)
{

    unsigned long prd_cnt = 0;
    {
        mf::LogDebug("NetMonInput") << "readDataProducts: reading data product count ...\n";
        msg->ReadULong(prd_cnt);
        mf::LogDebug("NetMonInput") << "readDataProducts: product count: " << prd_cnt << '\n';
    }
    //
    //  Read the data products.
    //
    const ProductList& productList = ProductMetaData::instance().productList();

    for (unsigned long I = 0; I < prd_cnt; ++I) {
        std::unique_ptr<BranchKey> bk;

	{
	mf::LogDebug("NetMonInput") << "readDataProducts: Reading branch key.\n";
	bk.reset(ReadObjectAny<BranchKey>(msg,"art::BranchKey",
					  "NetMonInputDetail::readDataProducts"));
	}
	  
        if (art::debugit() >= 1) {
            mf::LogDebug("NetMonInput") << "readDataProducts: got product class: '"
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
            mf::LogDebug("NetMonInput") << "readDataProducts: looking up product ...\n";
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
            mf::LogDebug("NetMonInput") << "readDataProducts: Reading product.\n";

	    // JCF, May-25-2016
	    // Currently unclear why the templatized version of ReadObjectAny doesn't work here...

	    //	    prd.reset(ReadObjectAny<EDProduct>(msg, bd.wrappedName()));

	    void* p = msg->ReadObjectAny(TClass::GetClass(bd.wrappedName().c_str()));

	    prd.reset(reinterpret_cast<EDProduct*>(p));
	    p = nullptr;

        }
        std::unique_ptr<const ProductProvenance> prdprov;
        {
            mf::LogDebug("NetMonInput") << "readDataProducts: Reading product provenance.\n";
	    prdprov.reset(ReadObjectAny<ProductProvenance>(msg,
							   "art::ProductProvenance",
							   "NetMonInputDetail::readDataProducts"));
        }
        {
            mf::LogDebug("NetMonInput") << "readDataProducts: inserting product: class: '"
                      << bd.friendlyClassName()
                      << "' modlbl: '"
                      << bd.moduleLabel()
                      << "' instnm: '"
                      << bd.productInstanceName()
                      << "' procnm: '"
                      << bd.processName()
                      << "'\n";
            outPrincipal->put(std::move(prd), bd, std::move(prdprov));
        }
    }
}


bool
art::NetMonInputDetail::
readNext(art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR,
         art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR,
         art::EventPrincipal*& outE)
{
    mf::LogDebug("NetMonInput") << "Begin: NetMonInputDetail::readNext\n";
    if (outputFileCloseNeeded_) {
        outputFileCloseNeeded_ = false;
        // Signal that we need the output file closed by returning false,
        // but answering true to the hasMoreData() query.
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "returning false on outputFileCloseNeeded_\n";
        mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::readNext\n";
        return false;
    }
    //
    //  Read the next message.
    //
    std::unique_ptr<TBufferFile> msg;
    {
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "Calling receiveMessage ...\n";
        ServiceHandle<NetMonTransportService> transport;
        TBufferFile* msg_ptr = 0;
        transport->receiveMessage(msg_ptr);
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "receiveMessage returned." << '\n';
        msg.reset(msg_ptr);
        msg_ptr = 0;
    }

    if (!msg) {
      shutdownMsgReceived_ = true;
      return false;
    }

    //
    //  Read message type code.
    //
    unsigned long msg_type_code = 0;
    {
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "getting message type code ...\n";
        msg->ReadULong(msg_type_code);
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "message type: " <<  msg_type_code << '\n';
    }
    if (msg_type_code == 5) {
        // Shutdown message.
        shutdownMsgReceived_ = true;
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "returning false on Shutdown message.\n";
        mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::readNext\n";
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
        mf::LogDebug("NetMonInput") << "NetMonInputDetail::readNext: "
                     "returning false on EndRun message.\n";
        mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::readNext\n";
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
        mf::LogDebug("NetMonInput") << "readNext: returning true on EndSubRun message.\n";
        mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::readNext\n";
        return true;
    }
    else if (msg_type_code == 4) {
        // Event message.
      readDataProducts(msg, outE);
      mf::LogDebug("NetMonInput") << "readNext: returning true on Event message.\n";
      mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::readNext\n";
        return true;
    }
    // Unknown message.
    // FIXME: What do we throw here for invalid message code?
    mf::LogDebug("NetMonInput") << "readNext: returning false on unknown msg_type_code!\n";
    mf::LogDebug("NetMonInput") << "End:   NetMonInputDetail::readNext\n";
    return false;
}

namespace art {

// Trait definition (must precede source typedef).
template <>
struct Source_generator<art::NetMonInputDetail> {
    static constexpr bool value = true;
};

// Source declaration.
typedef art::Source<NetMonInputDetail> NetMonInput;

} // namespace art

DEFINE_ART_INPUT_SOURCE(art::NetMonInput)

