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
    FDEBUG(1) << "Begin: NetMonInputDetail::NetMonInputDetail("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
    (void) ps;
    (void) helper;

    //
    //  Start server and listen for a connection.
    //
    FDEBUG(1) << "NetMonInputDetail: Starting server ...\n";
    ServiceHandle<NetMonTransportService> transport;
    transport->listen();
    //
    //  Got a connection, now receive the init message.
    //
    FDEBUG(1) << "NetMonInputDetail: Connect request received.\n";
    TBufferFile* msg_ptr = 0;
    transport->receiveMessage(msg_ptr);
    FDEBUG(1) << "NetMonInputDetail: receiveMessage returned.  ptr: 0x"
              << std::hex << (unsigned long) msg_ptr << std::dec << '\n';
    if (msg_ptr == nullptr) {
        throw art::Exception(art::errors::DataCorruption) <<
            "NetMonInputDetail: Could not receive message!";
    }
    std::unique_ptr<TBufferFile> msg(msg_ptr);
    msg_ptr = 0;

    //
    //  Read message type.
    //
    FDEBUG(1) << "NetMonInputDetail: getting message type code ...\n";
    unsigned long msg_type_code = 0;
    msg->ReadULong(msg_type_code);
    FDEBUG(1) << "NetMonInputDetail: message type: " << msg_type_code << '\n';
    //
    //  Read the ParameterSetRegistry.
    //
    unsigned long ps_cnt = 0;
    msg->ReadULong(ps_cnt);
    FDEBUG(1) << "NetMonInputDetail: parameter set count: " << ps_cnt << '\n';
    FDEBUG(1) << "NetMonInputDetail: reading parameter sets ...\n";
    for (unsigned long I = 0; I < ps_cnt; ++I) {

      std::string* pset_str = ReadObjectAny<std::string>(msg, "std::string");

        fhicl::ParameterSet pset;
        fhicl::make_ParameterSet(*pset_str, pset);
        // Force id calculation.
        pset.id();
        fhicl::ParameterSetRegistry::put(pset);
    }
    FDEBUG(1) << "NetMonInputDetail: finished reading parameter sets.\n";
    //
    //  Read the MasterProductRegistry.
    //

    art::ProductList* productlist = ReadObjectAny<art::ProductList>(msg, "map<art::BranchKey,art::BranchDescription>");
    helper.productList( productlist );

    FDEBUG(1) << "NetMonInputDetail: got product list\n";
    if (art::debugit() >= 1) {
        FDEBUG(1) << "NetMonInputDetail: before BranchIDLists\n";
        BranchIDLists* bil = &BranchIDListRegistry::instance()->data();
        int max_bli = bil->size();
        FDEBUG(1) << "NetMonInputDetail: max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
            int max_prdidx = (*bil)[i].size();
            FDEBUG(1) << "NetMonInputDetail: max_prdidx: "
                      << max_prdidx << '\n';
            for (int j = 0; j < max_prdidx; ++j) {
                FDEBUG(1) << "NetMonInputDetail:"
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

    art::ProcessHistoryMap* phm = ReadObjectAny<art::ProcessHistoryMap>(msg, "std::map<const art::Hash<2>,art::ProcessHistory>");

    FDEBUG(1) << "NetMonInputDetail: got ProcessHistoryMap\n";
    if (art::debugit() >= 1) {
        FDEBUG(1) << "NetMonInputDetail: dumping ProcessHistoryMap ...\n";
        FDEBUG(1) << "NetMonInputDetail: phm: size: "
                  << (unsigned long) phm->size() << '\n';
        for (auto I = phm->begin(), E = phm->end(); I != E; ++I) {
            std::ostringstream OS;
            I->first.print(OS);
            FDEBUG(1) << "NetMonInputDetail: phm: id: '" << OS.str() << "'\n";
        }
    }
    ProcessHistoryRegistry::put(*phm);
    if (art::debugit() >= 1) {
        FDEBUG(1) << "NetMonInputDetail: dumping ProcessHistoryRegistry ...\n";
        //typedef std::map<const ProcessHistoryID,ProcessHistory>
        //    ProcessHistoryMap;
        ProcessHistoryMap const& phr = ProcessHistoryRegistry::get();
        FDEBUG(1) << "NetMonInputDetail: phr: size: "
                  << (unsigned long) phr.size() << '\n';
        for (auto I = phr.begin(), E = phr.end(); I != E; ++I) {
            std::ostringstream OS;
            I->first.print(OS);
            FDEBUG(1) << "NetMonInputDetail: phr: id: '" << OS.str() << "'\n";
            OS.str("");
            FDEBUG(1) << "NetMonInputDetail: phr: data.size(): "
                      << I->second.data().size() << '\n';
            I->second.data().back().id().print(OS);
            FDEBUG(1) << "NetMonInputDetail: phr: data.back().id(): '"
                      << OS.str() << "'\n";
        }
    }
    //
    //  Read the ParentageRegistry.
    //

    art::ParentageMap* parentageMap = ReadObjectAny<art::ParentageMap>(msg, "std::map<const art::Hash<5>,art::Parentage>");

    FDEBUG(1) << "NetMonInputDetail: got ParentageMap\n";
    ParentageRegistry::put(*parentageMap);
    //
    //  Finished with init message.
    //
    FDEBUG(1) << "End:   NetMonInputDetail::NetMonInputDetail("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
}

art::NetMonInputDetail::
~NetMonInputDetail()
{
    FDEBUG(1) << "Begin: NetMonInputDetail::~NetMonInputDetail()\n";
    ServiceHandle<NetMonTransportService> transport;
    transport->disconnect();
    FDEBUG(1) << "End:   NetMonInputDetail::~NetMonInputDetail()\n";
}

void
art::NetMonInputDetail::
closeCurrentFile()
{
    FDEBUG(1) << "Begin/End: NetMonInputDetail::closeCurrentFile()\n";
}

void
art::NetMonInputDetail::
readFile(const std::string& name, art::FileBlock*& fb)
{
    FDEBUG(1) << "Begin: NetMonInputDetail::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
    (void) name;
    fb = new art::FileBlock(art::FileFormatVersion(1, "NetMonInput2013"),
                            "nothing");
    FDEBUG(1) << "End:   NetMonInputDetail::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
}

bool
art::NetMonInputDetail::
hasMoreData() const
{
    FDEBUG(1) << "Begin: NetMonInputDetail::hasMoreData()\n";
    if (shutdownMsgReceived_) {
        FDEBUG(1) << "NetMonInputDetail::hasMoreData(): "
                     "returning false on shutdownMsgReceived_.\n";
        FDEBUG(1) << "End:   NetMonInputDetail::hasMoreData()\n";
        return false;
    }
    FDEBUG(1) << "NetMonInputDetail::hasMoreData(): "
                 "returning true on not shutdownMsgReceived_.\n";
    FDEBUG(1) << "End:   NetMonInputDetail::hasMoreData()\n";
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

    if (msg_type_code == 2) {
        // EndRun message.

        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::RunAuxiliary ...\n";
            run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, 
							   "art::RunAuxiliary"));
            FDEBUG(1) << "readAndConstructPrincipal: got art::RunAuxiliary.\n";
            if (art::debugit() >= 1) {
                std::ostringstream OS;
                run_aux->processHistoryID().print(OS);
                FDEBUG(1) << "readAndConstructPrincipal: ProcessHistoryID: '"
                          << OS.str() << "'\n";
            }
            if (art::debugit() >= 1) {
                if (run_aux->processHistoryID().isValid()) {
                    std::ostringstream OS;
                    run_aux->processHistoryID().print(OS);
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: '"
                              << OS.str() << "'\n";
                }
                else {
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: 'INVALID'\n";
                }
            }
        }
    }
    else if (msg_type_code == 3) {
        // EndSubRun message.

        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::SubRunAuxiliary ...\n";
            subrun_aux.reset(ReadObjectAny<art::SubRunAuxiliary>(msg,
								 "art::SubRunAuxiliary"));
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "got art::SubRunAuxiliary.\n";
            if (art::debugit() >= 1) {
                if (subrun_aux->processHistoryID().isValid()) {
                    std::ostringstream OS;
                    subrun_aux->processHistoryID().print(OS);
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: '"
                              << OS.str() << "'\n";
                }
                else {
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: 'INVALID'\n";
                }
            }
        }
    }
    else if (msg_type_code == 4) {
        // Event message.

        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::RunAuxiliary ...\n";
            run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, 
							   "art::RunAuxiliary"));

            FDEBUG(1) << "readAndConstructPrincipal: "
                         "got art::RunAuxiliary.\n";
            if (art::debugit() >= 1) {
                if (run_aux->processHistoryID().isValid()) {
                    std::ostringstream OS;
                    run_aux->processHistoryID().print(OS);
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: '"
                              << OS.str() << "'\n";
                }
                else {
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: 'INVALID'\n";
                }
            }
        }

        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::SubRunAuxiliary ...\n";
            
	    subrun_aux.reset(ReadObjectAny<art::SubRunAuxiliary>(msg,
								 "art::SubRunAuxiliary"));
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "got art::SubRunAuxiliary.\n";
            if (art::debugit() >= 1) {
                if (subrun_aux->processHistoryID().isValid()) {
                    std::ostringstream OS;
                    subrun_aux->processHistoryID().print(OS);
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: '"
                              << OS.str() << "'\n";
                }
                else {
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: 'INVALID'\n";
                }
            }
        }

        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::EventAuxiliary ...\n";
	    
	    event_aux.reset(ReadObjectAny<art::EventAuxiliary>(msg,
							       "art::EventAuxiliary"));

            FDEBUG(1) << "readAndConstructPrincipal: "
                         "got art::EventAuxiliary.\n";
        }

        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::History ...\n";

	    history.reset(ReadObjectAny<art::History>(msg,
						      "art::History"));

            FDEBUG(1) << "readAndConstructPrincipal: got art::History.\n";
            if (art::debugit() >= 1) {
                if (history->processHistoryID().isValid()) {
                    std::ostringstream OS;
                    history->processHistoryID().print(OS);
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: '"
                              << OS.str() << "'\n";
                }
                else {
                    FDEBUG(1) << "readAndConstructPrincipal: "
                              << "ProcessHistoryID: 'INVALID'\n";
                }
            }
        }
    }
    //
    //  Construct the principal.
    //
    if (msg_type_code == 2) {
        // EndRun message.
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "processing EndRun message ...\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "making flush RunPrincipal ...\n";
        outR = pm_.makeRunPrincipal(RunID::flushRun(), run_aux->beginTime());
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished making flush RunPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "making flush SubRunPrincipal ...\n";
        outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(),
                                        run_aux->beginTime());
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished making flush SubRunPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "making flush EventPrincipal ...\n";
        outE = pm_.makeEventPrincipal(EventID::flushEvent(),
                                      run_aux->endTime(), true,
                                      EventAuxiliary::Any);
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished making flush EventPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished processing EndRun message.\n";
    }
    else if (msg_type_code == 3) {
        // EndSubRun message.
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "processing EndSubRun message ...\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
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

        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished making flush RunPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "making flush SubRunPrincipal ...\n";
        outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(),
                                        subrun_aux->beginTime());
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished making flush SubRunPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "making flush EventPrincipal ...\n";
        outE = pm_.makeEventPrincipal(EventID::flushEvent(),
                                      subrun_aux->endTime(), true,
                                      EventAuxiliary::Any);
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished making flush EventPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "finished processing EndSubRun message.\n";
    }
    else if (msg_type_code == 4) {
        // Event message.
        FDEBUG(1) << "readAndConstructPrincipal: "
                     "processing Event message ...\n";
        // FIXME: This need to be an exception throw!
        assert(history->processHistoryID().isValid() &&
               "readAndConstructPrincipal: processHistoryID of history in "
               "Event message is invalid!");
        //const ProcessHistory& processHistory =
        //    ProcessHistoryRegistry::get(history->processHistoryID());
        if ((inR == nullptr) || !inR->id().isValid() ||
                (inR->run() != event_aux->run())) {
            // New run, either we have no input RunPrincipal, or the
            // input run number does not match the event run number.
            FDEBUG(1) << "readAndConstructPrincipal: making RunPrincipal ...\n";
            //outR = new RunPrincipal(*run_aux.get(),
            //                        processHistory.data().back());
	    outR = pm_.makeRunPrincipal(*run_aux.get());
            FDEBUG(1) << "readAndConstructPrincipal: made RunPrincipal.\n";
        }
        if ((inSR == nullptr) || !inSR->id().isValid() ||
                (inSR->subRun() != event_aux->subRun())) {
            // New SubRun, either we have no input SubRunPrincipal, or the
            // input subRun number does not match the event subRun number.
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "making SubRunPrincipal ...\n";
            //outSR = new SubRunPrincipal(*subrun_aux.get(),
            //                            processHistory.data().back());
	    outSR = pm_.makeSubRunPrincipal(*subrun_aux.get());
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "made SubRunPrincipal.\n";
        }
        FDEBUG(1) << "readAndConstructPrincipal: making EventPrincipal ...\n";
        //outE = new EventPrincipal(*event_aux.get(),
        //                          processHistory.data().back(),
        //                          history);
	outE = pm_.makeEventPrincipal(*event_aux.get(), std::move(history));
        FDEBUG(1) << "readAndConstructPrincipal: made EventPrincipal.\n";
        FDEBUG(1) << "readAndConstructPrincipal: "
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
        FDEBUG(1) << "readDataProducts: reading data product count ...\n";
        msg->ReadULong(prd_cnt);
        FDEBUG(1) << "readDataProducts: product count: " << prd_cnt << '\n';
    }
    //
    //  Read the data products.
    //
    const ProductList& productList = ProductMetaData::instance().productList();

    for (unsigned long I = 0; I < prd_cnt; ++I) {
        std::unique_ptr<BranchKey> bk;

	{
	FDEBUG(1) << "readDataProducts: Reading branch key.\n";
	bk.reset(ReadObjectAny<BranchKey>(msg,"art::BranchKey"));
	}
	  
        if (art::debugit() >= 1) {
            FDEBUG(1) << "readDataProducts: got product class: '"
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
            FDEBUG(1) << "readDataProducts: looking up product ...\n";
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
            FDEBUG(1) << "readDataProducts: Reading product.\n";

	    // JCF, May-25-2016
	    // Currently unclear why the templatized version of ReadObjectAny doesn't work here...

	    //	    prd.reset(ReadObjectAny<EDProduct>(msg, bd.wrappedName()));

	    void* p = msg->ReadObjectAny(TClass::GetClass(bd.wrappedName().c_str()));
	    FDEBUG(2) << "readDataProducts: p: 0x" << std::hex
		      << (unsigned long) p << std::dec << '\n';
	    prd.reset(reinterpret_cast<EDProduct*>(p));
	    p = nullptr;

        }
        std::unique_ptr<const ProductProvenance> prdprov;
        {
            FDEBUG(1) << "readDataProducts: Reading product provenance.\n";
	    prdprov.reset(ReadObjectAny<ProductProvenance>(msg,
							   "art::ProductProvenance"));
        }
        {
            FDEBUG(1) << "readDataProducts: inserting product: class: '"
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
    FDEBUG(1) << "Begin: NetMonInputDetail::readNext\n";
    if (outputFileCloseNeeded_) {
        outputFileCloseNeeded_ = false;
        // Signal that we need the output file closed by returning false,
        // but answering true to the hasMoreData() query.
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "returning false on outputFileCloseNeeded_\n";
        FDEBUG(1) << "End:   NetMonInputDetail::readNext\n";
        return false;
    }
    //
    //  Read the next message.
    //
    std::unique_ptr<TBufferFile> msg;
    {
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "Calling receiveMessage ...\n";
        ServiceHandle<NetMonTransportService> transport;
        TBufferFile* msg_ptr = 0;
        transport->receiveMessage(msg_ptr);
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "receiveMessage returned." << '\n';
        FDEBUG(2) << "NetMonInputDetail::readNext: ptr: 0x" << std::hex
                  << (unsigned long) msg_ptr << std::dec << '\n';
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
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "getting message type code ...\n";
        msg->ReadULong(msg_type_code);
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "message type: " <<  msg_type_code << '\n';
    }
    if (msg_type_code == 5) {
        // Shutdown message.
        shutdownMsgReceived_ = true;
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "returning false on Shutdown message.\n";
        FDEBUG(1) << "End:   NetMonInputDetail::readNext\n";
        return false;
    }
    FDEBUG(2) << "NetMonInputDetail::readNext: Dumping BranchIdList\n";
    if (art::debugit() >= 2) {
        BranchIDLists* bil = &BranchIDListRegistry::instance()->data();
        int max_bli = bil->size();
        FDEBUG(2) << "NetMonInputDetail::readNext: "
                     "max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
            int max_prdidx = (*bil)[i].size();
            FDEBUG(2) << "NetMonInputDetail::readNext: max_prdidx: "
                      << max_prdidx << '\n';
            for (int j = 0; j < max_prdidx; ++j) {
                FDEBUG(2) << "NetMonInputDetail::readNext:"
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
        FDEBUG(1) << "NetMonInputDetail::readNext: "
                     "returning false on EndRun message.\n";
        FDEBUG(1) << "End:   NetMonInputDetail::readNext\n";
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
        FDEBUG(1) << "readNext: returning true on EndSubRun message.\n";
        FDEBUG(1) << "End:   NetMonInputDetail::readNext\n";
        return true;
    }
    else if (msg_type_code == 4) {
        // Event message.
      readDataProducts(msg, outE);
        FDEBUG(1) << "readNext: returning true on Event message.\n";
        FDEBUG(1) << "End:   NetMonInputDetail::readNext\n";
        return true;
    }
    // Unknown message.
    // FIXME: What do we throw here for invalid message code?
    FDEBUG(1) << "readNext: returning false on unknown msg_type_code!\n";
    FDEBUG(1) << "End:   NetMonInputDetail::readNext\n";
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

