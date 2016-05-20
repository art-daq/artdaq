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
#include "cetlib/BasicPluginFactory.h"

#include "TClass.h"
#include "TMessage.h"
#include "TBufferFile.h"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"
#include "artdaq/ArtModules/TransferInterface.h"
//#include "artdaq/ArtModules/NetMonTransportService.h"

#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <sys/time.h>

namespace art {
class TransferInputDetail;
}

class art::TransferInputDetail {
public:
    TransferInputDetail(const TransferInputDetail&) = delete;
    TransferInputDetail& operator=(const TransferInputDetail&) = delete;

  ~TransferInputDetail() = default;

    TransferInputDetail(const fhicl::ParameterSet&, art::ProductRegistryHelper&,
                      const art::SourceHelper&);

    void closeCurrentFile();

    void readFile(const std::string&, art::FileBlock*&);

    bool hasMoreData() const;

    bool readNext(art::RunPrincipal* const inR,
                  art::SubRunPrincipal* const inSR, art::RunPrincipal*& outR,
                  art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE);

private:
    void
    readAndConstructPrincipal(TBufferFile&, unsigned long,
                              art::RunPrincipal* const,
                              art::SubRunPrincipal* const,
                              art::RunPrincipal*&,
                              art::SubRunPrincipal*&,
                              art::EventPrincipal*&);

    template <class T>
    void
    readDataProducts(TBufferFile&, T*&);

  void extractTBufferFile(const artdaq::Fragment&, TBufferFile*& );

private:
    bool shutdownMsgReceived_;
    bool outputFileCloseNeeded_;
    const art::SourceHelper& pm_;
  std::unique_ptr<TransferInterface> transfer_;

  // JCF, May-20-2016
  // Plan to make this a settable parameter...
  const std::size_t recvTimeout = 1000000;
};

art::TransferInputDetail::
TransferInputDetail(const fhicl::ParameterSet& ps,
                  art::ProductRegistryHelper& helper,
                  const art::SourceHelper& pm)
    : shutdownMsgReceived_(false), outputFileCloseNeeded_(false), pm_(pm)
{

  // JCF, May-20-2016
  // Will eventually move this to the initializer list, and make plugin type settable from FHiCL

  static cet::BasicPluginFactory bpf("transfer", "make");
  
  fhicl::ParameterSet dummyset;

  transfer_ =  bpf.makePlugin<std::unique_ptr<TransferInterface>,const fhicl::ParameterSet&>("RTIDDS",dummyset);

  // Strictly to avoid compiler warnings about unused variables
  (void) ps;
  (void) helper;

    //
    //  Get the root classes needed for reading.
    //
    static TClass* string_class = TClass::GetClass("std::string");
    if (string_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "TransferInputDetail: "
            "Could not get TClass for std::string!";
    }
    static TClass* product_list_class = TClass::GetClass("map<art::BranchKey,"
        "art::BranchDescription>");
    if (product_list_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "TransferInputDetail: "
            "Could not get TClass for "
            "map<art::BranchKey,art::BranchDescription>!";
    }
    //typedef std::map<const ProcessHistoryID,ProcessHistory> ProcessHistoryMap;
    //static TClass* phm_class = TClass::GetClass(
    //    "std::map<const art::ProcessHistoryID,art::ProcessHistory>");
    //FIXME: Instead of using the by-name version of GetClass here, we
    //       should use the type_info version so that we do not have
    //       to hard-code the enumerator value into the Hash<..> name.
    static TClass* phm_class = TClass::GetClass(
        "std::map<const art::Hash<2>,art::ProcessHistory>");
    if (phm_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "TransferInputDetail: "
            "Could not get TClass for "
            "std::map<const art::Hash<2>,art::ProcessHistory>!";
    }
    //typedef std::map<const ParentageID,Parentage> ParentageMap;
    //FIXME: Instead of using the by-name version of GetClass here, we
    //       should use the type_info version so that we do not have
    //       to hard-code the enumerator value into the Hash<..> name.
    static TClass* parentage_map_class = TClass::GetClass(
        "std::map<const art::Hash<5>,art::Parentage>");
    if (parentage_map_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "TransferInputDetail: "
            "Could not get TClass for "
            "std::map<const art::Hash<5>,art::Parentage>!";
    }
    // //
    // //  Start server and listen for a connection.
    // //
    // FDEBUG(1) << "TransferInputDetail: Starting server ...\n";
    // ServiceHandle<NetMonTransportService> transport;
    // transport->listen();
    // //
    // //  Got a connection, now receive the init message.
    // //
    // FDEBUG(1) << "TransferInputDetail: Connect request received.\n";
    // TBufferFile* msg_ptr = 0;
    // transport->receiveMessage(msg_ptr);
    // FDEBUG(1) << "TransferInputDetail: receiveMessage returned.  ptr: 0x"
    //           << std::hex << (unsigned long) msg_ptr << std::dec << '\n';

    artdaq::Fragment fragment;
    TBufferFile* msg_ptr( nullptr );

    transfer_->receiveFragmentFrom(fragment, recvTimeout);
    
    extractTBufferFile(fragment, msg_ptr);

    if (msg_ptr == nullptr) {
        throw art::Exception(art::errors::DataCorruption) <<
            "TransferInputDetail: Could not receive message!";
    }
    std::unique_ptr<TBufferFile> msg(msg_ptr);
    msg_ptr = 0;
    void* p = 0;
    //
    //  Read message type.
    //
    FDEBUG(1) << "TransferInputDetail: getting message type code ...\n";
    unsigned long msg_type_code = 0;
    msg->ReadULong(msg_type_code);
    FDEBUG(1) << "TransferInputDetail: message type: " << msg_type_code << '\n';
    //
    //  Read the ParameterSetRegistry.
    //
    unsigned long ps_cnt = 0;
    msg->ReadULong(ps_cnt);
    FDEBUG(1) << "TransferInputDetail: parameter set count: " << ps_cnt << '\n';
    FDEBUG(1) << "TransferInputDetail: reading parameter sets ...\n";
    for (unsigned long I = 0; I < ps_cnt; ++I) {
        p = msg->ReadObjectAny(string_class);
        std::string* pset_str = reinterpret_cast<std::string*>(p);
        p = 0;
        fhicl::ParameterSet pset;
        fhicl::make_ParameterSet(*pset_str, pset);
        // Force id calculation.
        pset.id();
        fhicl::ParameterSetRegistry::put(pset);
    }
    FDEBUG(1) << "TransferInputDetail: finished reading parameter sets.\n";
    //
    //  Read the MasterProductRegistry.
    //
    p = msg->ReadObjectAny(product_list_class);
    helper.productList(reinterpret_cast<art::ProductList*>(p));
    p = 0;
    FDEBUG(1) << "TransferInputDetail: got product list\n";
    if (art::debugit() >= 1) {
        FDEBUG(1) << "TransferInputDetail: before BranchIDLists\n";
        BranchIDLists* bil = &BranchIDListRegistry::instance()->data();
        int max_bli = bil->size();
        FDEBUG(1) << "TransferInputDetail: max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
            int max_prdidx = (*bil)[i].size();
            FDEBUG(1) << "TransferInputDetail: max_prdidx: "
                      << max_prdidx << '\n';
            for (int j = 0; j < max_prdidx; ++j) {
                FDEBUG(1) << "TransferInputDetail:"
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
    //
    //  Read the ProcessHistoryRegistry.
    //
    //typedef std::map<const ProcessHistoryID,ProcessHistory> ProcessHistoryMap;
    //static TClass* phm_class = TClass::GetClass(
    //    "std::map<const art::ProcessHistoryID,art::ProcessHistory>");
    p = msg->ReadObjectAny(phm_class);
    art::ProcessHistoryMap* phm = reinterpret_cast<art::ProcessHistoryMap*>(p);
    p = 0;
    FDEBUG(1) << "TransferInputDetail: got ProcessHistoryMap\n";
    if (art::debugit() >= 1) {
        FDEBUG(1) << "TransferInputDetail: dumping ProcessHistoryMap ...\n";
        FDEBUG(1) << "TransferInputDetail: phm: size: "
                  << (unsigned long) phm->size() << '\n';
        for (auto I = phm->begin(), E = phm->end(); I != E; ++I) {
            std::ostringstream OS;
            I->first.print(OS);
            FDEBUG(1) << "TransferInputDetail: phm: id: '" << OS.str() << "'\n";
        }
    }
    ProcessHistoryRegistry::put(*phm);
    if (art::debugit() >= 1) {
        FDEBUG(1) << "TransferInputDetail: dumping ProcessHistoryRegistry ...\n";
        //typedef std::map<const ProcessHistoryID,ProcessHistory>
        //    ProcessHistoryMap;
        ProcessHistoryMap const& phr = ProcessHistoryRegistry::get();
        FDEBUG(1) << "TransferInputDetail: phr: size: "
                  << (unsigned long) phr.size() << '\n';
        for (auto I = phr.begin(), E = phr.end(); I != E; ++I) {
            std::ostringstream OS;
            I->first.print(OS);
            FDEBUG(1) << "TransferInputDetail: phr: id: '" << OS.str() << "'\n";
            OS.str("");
            FDEBUG(1) << "TransferInputDetail: phr: data.size(): "
                      << I->second.data().size() << '\n';
            I->second.data().back().id().print(OS);
            FDEBUG(1) << "TransferInputDetail: phr: data.back().id(): '"
                      << OS.str() << "'\n";
        }
    }
    //
    //  Read the ParentageRegistry.
    //
    //typedef std::map<const ParentageID,Parentage> ParentageMap;
    p = msg->ReadObjectAny(parentage_map_class);
    art::ParentageMap* parentageMap = reinterpret_cast<art::ParentageMap*>(p);
    p = 0;
    FDEBUG(1) << "TransferInputDetail: got ParentageMap\n";
    ParentageRegistry::put(*parentageMap);
    //
    //  Finished with init message.
    //
    FDEBUG(1) << "End:   TransferInputDetail::TransferInputDetail("
                 "const fhicl::ParameterSet& ps, "
                 "art::ProductRegistryHelper& helper, "
                 "const art::SourceHelper& pm)\n";
}

void
art::TransferInputDetail::
closeCurrentFile()
{
 
}

void
art::TransferInputDetail::
readFile(const std::string& name, art::FileBlock*& fb)
{
    FDEBUG(1) << "Begin: TransferInputDetail::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
    (void) name;
    fb = new art::FileBlock(art::FileFormatVersion(1, "TransferInput2013"),
                            "nothing");
    FDEBUG(1) << "End:   TransferInputDetail::"
                 "readFile(const std::string& name, art::FileBlock*& fb)\n";
}

bool
art::TransferInputDetail::
hasMoreData() const
{
    FDEBUG(1) << "Begin: TransferInputDetail::hasMoreData()\n";
    if (shutdownMsgReceived_) {
        FDEBUG(1) << "TransferInputDetail::hasMoreData(): "
                     "returning false on shutdownMsgReceived_.\n";
        FDEBUG(1) << "End:   TransferInputDetail::hasMoreData()\n";
        return false;
    }
    FDEBUG(1) << "TransferInputDetail::hasMoreData(): "
                 "returning true on not shutdownMsgReceived_.\n";
    FDEBUG(1) << "End:   TransferInputDetail::hasMoreData()\n";
    return true;
}

void
art::TransferInputDetail::
readAndConstructPrincipal(TBufferFile& msg, unsigned long msg_type_code,
                          art::RunPrincipal* const inR,
                          art::SubRunPrincipal* const inSR,
                          art::RunPrincipal*& outR,
                          art::SubRunPrincipal*& outSR,
                          art::EventPrincipal*& outE)
{
    //
    //  Get root classes necessary for reading.
    //
    static TClass* run_aux_class = TClass::GetClass("art::RunAuxiliary");
    if (run_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "readAndConstructPrincipal: "
            "Could not get TClass for art::RunAuxiliary!";
    }
    static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
    if (subrun_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "readAndConstructPrincipal: "
            "Could not get TClass for art::SubRunAuxiliary!";
    }
    static TClass* event_aux_class = TClass::GetClass("art::EventAuxiliary");
    if (event_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "readAndConstructPrincipal: "
            "Could not get TClass for art::EventAuxiliary!";
    }
    static TClass* history_class = TClass::GetClass("art::History");
    if (history_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "readAndConstructPrincipal: "
            "Could not get TClass for art::History!";
    }
    //
    //  Now process the message.
    //
    std::unique_ptr<art::RunAuxiliary> run_aux;
    std::unique_ptr<art::SubRunAuxiliary> subrun_aux;
    std::unique_ptr<art::EventAuxiliary> event_aux;
    std::shared_ptr<History> history;
    void* p = 0;
    if (msg_type_code == 2) {
        // EndRun message.
        //
        //  Read the RunAuxiliary.
        //
        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::RunAuxiliary ...\n";
            p = msg.ReadObjectAny(run_aux_class);
            FDEBUG(2) << "readAndConstructPrincipal: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            if (p == nullptr) {
                throw art::Exception(art::errors::DataCorruption) <<
                    "readAndConstructPrincipal: "
                    "Could not read art::RunAuxiliary!";
            }
            run_aux.reset(reinterpret_cast<art::RunAuxiliary*>(p));
            p = 0;
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
        //
        //  Read the SubRunAuxiliary.
        //
        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::SubRunAuxiliary ...\n";
            p = msg.ReadObjectAny(subrun_aux_class);
            FDEBUG(2) << "readAndConstructPrincipal: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            if (p == nullptr) {
                throw art::Exception(art::errors::DataCorruption) <<
                    "readAndConstructPrincipal: "
                    "Could not read art::SubRunAuxiliary!";
            }
            subrun_aux.reset(reinterpret_cast<art::SubRunAuxiliary*>(p));
            p = 0;
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
        //
        //  Read the RunAuxiliary.
        //
        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::RunAuxiliary ...\n";
            p = msg.ReadObjectAny(run_aux_class);
            FDEBUG(2) << "readAndConstructPrincipal: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            if (p == nullptr) {
                throw art::Exception(art::errors::DataCorruption) <<
                    "readAndConstructPrincipal: "
                    "Could not read art::RunAuxiliary!";
            }
            run_aux.reset(reinterpret_cast<art::RunAuxiliary*>(p));
            p = 0;
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
        //
        //  Read the SubRunAuxiliary.
        //
        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::SubRunAuxiliary ...\n";
            p = msg.ReadObjectAny(subrun_aux_class);
            FDEBUG(2) << "readAndConstructPrincipal: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            if (p == nullptr) {
                throw art::Exception(art::errors::DataCorruption) <<
                    "readAndConstructPrincipal: "
                    "Could not read art::SubRunAuxiliary!";
            }
            subrun_aux.reset(reinterpret_cast<art::SubRunAuxiliary*>(p));
            p = 0;
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
        //
        //  Read the EventAuxiliary.
        //
        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::EventAuxiliary ...\n";
            p = msg.ReadObjectAny(event_aux_class);
            FDEBUG(2) << "readAndConstructPrincipal: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            if (p == nullptr) {
                throw art::Exception(art::errors::DataCorruption) <<
                    "readAndConstructPrincipal: "
                    "Could not read art::EventAuxiliary!";
            }
            event_aux.reset(reinterpret_cast<art::EventAuxiliary*>(p));
            p = 0;
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "got art::EventAuxiliary.\n";
        }
        //
        //  Read the History.
        //
        {
            FDEBUG(1) << "readAndConstructPrincipal: "
                         "getting art::History ...\n";
            p = msg.ReadObjectAny(history_class);
            FDEBUG(2) << "readAndConstructPrincipal: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            if (p == nullptr) {
                throw art::Exception(art::errors::DataCorruption) <<
                    "readAndConstructPrincipal: "
                    "Could not read art::History!";
            }

	    history.reset(reinterpret_cast<art::History*>(p));

            p = 0;
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
art::TransferInputDetail::
readDataProducts(TBufferFile& msg, T*& outPrincipal)
{
    //
    //  Get the root classes we need for reading.
    //
    static TClass* branch_key_class = TClass::GetClass("art::BranchKey");
    if (branch_key_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "readDataProducts: "
            "Could not get TClass for art::BranchKey!";
    }
    static TClass* prdprov_class = TClass::GetClass("art::ProductProvenance");
    if (prdprov_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "readDataProducts: "
            "Could not get TClass for art::ProductProvenance!";
    }
    //
    //  Read the data product count.
    //
    unsigned long prd_cnt = 0;
    {
        FDEBUG(1) << "readDataProducts: reading product count ...\n";
        msg.ReadULong(prd_cnt);
        FDEBUG(1) << "readDataProducts: product count: " << prd_cnt << '\n';
    }
    //
    //  Read the data products.
    //
    const ProductList& productList = ProductMetaData::instance().productList();
    void* p = 0;
    for (unsigned long I = 0; I < prd_cnt; ++I) {
        std::unique_ptr<BranchKey> bk;
        {
            FDEBUG(1) << "readDataProducts: Reading branch key.\n";
            p = msg.ReadObjectAny(branch_key_class);
            FDEBUG(2) << "readDataProducts: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            bk.reset(reinterpret_cast<BranchKey*>(p));
            p = 0;
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
            p = msg.ReadObjectAny(TClass::GetClass(bd.wrappedName().c_str()));
            FDEBUG(2) << "readDataProducts: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            prd.reset(reinterpret_cast<EDProduct*>(p));
            p = 0;
        }
        std::unique_ptr<const ProductProvenance> prdprov;
        {
            FDEBUG(1) << "readDataProducts: Reading product provenance.\n";
            p = msg.ReadObjectAny(prdprov_class);
            FDEBUG(2) << "readDataProducts: p: 0x" << std::hex
                      << (unsigned long) p << std::dec << '\n';
            prdprov.reset(reinterpret_cast<ProductProvenance*>(p));
            p = 0;
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
art::TransferInputDetail::
readNext(art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR,
         art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR,
         art::EventPrincipal*& outE)
{
    FDEBUG(1) << "Begin: TransferInputDetail::readNext\n";
    if (outputFileCloseNeeded_) {
        outputFileCloseNeeded_ = false;
        // Signal that we need the output file closed by returning false,
        // but answering true to the hasMoreData() query.
        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "returning false on outputFileCloseNeeded_\n";
        FDEBUG(1) << "End:   TransferInputDetail::readNext\n";
        return false;
    }
    //
    //  Read the next message.
    //
    std::unique_ptr<TBufferFile> msg;
    {
        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "Calling receiveMessage ...\n";
        //ServiceHandle<NetMonTransportService> transport;
	//        TBufferFile* msg_ptr = 0;
	//        transport->receiveMessage(msg_ptr);
	artdaq::Fragment fragment;
	transfer_->receiveFragmentFrom(fragment, recvTimeout);

	TBufferFile* msg_ptr( nullptr );    
	extractTBufferFile(fragment, msg_ptr);

        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "receiveMessage returned." << '\n';
        FDEBUG(2) << "TransferInputDetail::readNext: ptr: 0x" << std::hex
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
        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "getting message type code ...\n";
        msg->ReadULong(msg_type_code);
        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "message type: " <<  msg_type_code << '\n';
    }
    if (msg_type_code == 5) {
        // Shutdown message.
        shutdownMsgReceived_ = true;
        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "returning false on Shutdown message.\n";
        FDEBUG(1) << "End:   TransferInputDetail::readNext\n";
        return false;
    }
    FDEBUG(2) << "TransferInputDetail::readNext: Dumping BranchIdList\n";
    if (art::debugit() >= 2) {
        BranchIDLists* bil = &BranchIDListRegistry::instance()->data();
        int max_bli = bil->size();
        FDEBUG(2) << "TransferInputDetail::readNext: "
                     "max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
            int max_prdidx = (*bil)[i].size();
            FDEBUG(2) << "TransferInputDetail::readNext: max_prdidx: "
                      << max_prdidx << '\n';
            for (int j = 0; j < max_prdidx; ++j) {
                FDEBUG(2) << "TransferInputDetail::readNext:"
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
    readAndConstructPrincipal(*msg.get(), msg_type_code, inR, inSR, outR,
                              outSR, outE);
    //
    //  Read per-event metadata needed to construct principal.
    //
    if (msg_type_code == 2) {
        // EndRun message.
        // FIXME: We need to merge these into the input RunPrincipal.
        readDataProducts(*msg.get(), outR);
        // Signal that we should close the input and output file.
        FDEBUG(1) << "TransferInputDetail::readNext: "
                     "returning false on EndRun message.\n";
        FDEBUG(1) << "End:   TransferInputDetail::readNext\n";
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
        readDataProducts(*msg.get(), outSR);
        // Remember that we should ask for file close next time
        // we are called.
        outputFileCloseNeeded_ = true;
        FDEBUG(1) << "readNext: returning true on EndSubRun message.\n";
        FDEBUG(1) << "End:   TransferInputDetail::readNext\n";
        return true;
    }
    else if (msg_type_code == 4) {
        // Event message.
        readDataProducts(*msg.get(), outE);
        FDEBUG(1) << "readNext: returning true on Event message.\n";
        FDEBUG(1) << "End:   TransferInputDetail::readNext\n";
        return true;
    }
    // Unknown message.
    // FIXME: What do we throw here for invalid message code?
    FDEBUG(1) << "readNext: returning false on unknown msg_type_code!\n";
    FDEBUG(1) << "End:   TransferInputDetail::readNext\n";
    return false;
}

void 
art::TransferInputDetail::extractTBufferFile(const artdaq::Fragment& fragment, TBufferFile*& tbuffer) {

  const artdaq::NetMonHeader *header = fragment.metadata<artdaq::NetMonHeader>();
  char *buffer = (char *)malloc(header->data_length);
  memcpy(buffer, fragment.dataBeginBytes(), header->data_length);

  // TBufferFile takes ownership of the contents of memory passed to it
  tbuffer = new TBufferFile(TBuffer::kRead, header->data_length, buffer, kTRUE, 0);
}

namespace art {

// Trait definition (must precede source typedef).
template <>
struct Source_generator<art::TransferInputDetail> {
    static constexpr bool value = true;
};

// Source declaration.
typedef art::Source<TransferInputDetail> TransferInput;

} // namespace art

DEFINE_ART_INPUT_SOURCE(art::TransferInput)

