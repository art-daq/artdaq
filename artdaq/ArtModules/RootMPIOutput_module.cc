#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/OutputHandle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Persistency/Provenance/BranchDescription.h"
#include "art/Persistency/Provenance/BranchIDList.h"
#include "art/Persistency/Provenance/BranchIDListHelper.h"
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "art/Persistency/Provenance/BranchKey.h"
#include "art/Persistency/Provenance/History.h"
#include "art/Persistency/Provenance/ParentageRegistry.h"
#include "art/Persistency/Provenance/ProcessConfiguration.h"
#include "art/Persistency/Provenance/ProcessConfigurationID.h"
#include "art/Persistency/Provenance/ProcessHistoryID.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductList.h"
#include "art/Persistency/Provenance/ProductMetaData.h"
#include "art/Persistency/Provenance/ProductProvenance.h"
#include "art/Persistency/Provenance/RunAuxiliary.h"
#include "art/Persistency/Provenance/SubRunAuxiliary.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Utilities/DebugMacros.h"
#include "art/Utilities/Exception.h"
#include "cetlib/column_width.h"
#include "cetlib/lpad.h"
#include "cetlib/rpad.h"
#include <algorithm>
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"

#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq/DAQdata/NetMonHeader.hh"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "unistd.h"

#include "TClass.h"
#include "TMessage.h"

#if ART_MAJOR_VERSION >= 1 && ART_MINOR_VERSION >= 16
#  define CONST_WRITE
struct Config {

};
#else
#  define CONST_WRITE const
#endif

namespace art {
class RootMPIOutput;
}

using art::RootMPIOutput;
using fhicl::ParameterSet;

class art::RootMPIOutput : public OutputModule {
public:
    explicit RootMPIOutput(ParameterSet const&);
    ~RootMPIOutput();
private:
    virtual void openFile(FileBlock const&);
    virtual void closeFile();
    virtual void respondToCloseInputFile(FileBlock const&);
    virtual void respondToCloseOutputFiles(FileBlock const&);
    virtual void endJob();

    virtual void write(EventPrincipal CONST_WRITE&);
    virtual void writeRun(RunPrincipal CONST_WRITE&);
    virtual void writeSubRun(SubRunPrincipal CONST_WRITE&);
    void writeDataProducts(TBufferFile&, const Principal&,
                           std::vector<BranchKey*>&);
private:
    bool initMsgSent_;
};

art::RootMPIOutput::
RootMPIOutput(ParameterSet const& ps)
#if (ART_MAJOR_VERSION == 1 && ART_MINOR_VERSION >= 18) || ART_MAJOR_VERSION > 1
  : OutputModule(ps), initMsgSent_(false)
#elif ART_MAJOR_VERSION >= 1 && ART_MINOR_VERSION >= 16
  : OutputModule(OutputModule::Table<Config>(ps)), initMsgSent_(false)
#else
    : OutputModule(ps), initMsgSent_(false)
#endif
{
    FDEBUG(1) << "Begin: RootMPIOutput::RootMPIOutput(ParameterSet const& ps)\n";
    ServiceHandle<NetMonTransportService> transport;
    transport->connect();
    FDEBUG(1) << "End:   RootMPIOutput::RootMPIOutput(ParameterSet const& ps)\n";
}

art::RootMPIOutput::
~RootMPIOutput()
{
    FDEBUG(1) << "Begin: RootMPIOutput::~RootMPIOutput()\n";
    ServiceHandle<NetMonTransportService> transport;
    transport->disconnect();
    FDEBUG(1) << "End:   RootMPIOutput::~RootMPIOutput()\n";
}

void
art::RootMPIOutput::
openFile(FileBlock const&)
{
    FDEBUG(1) << "Begin/End: RootMPIOutput::openFile(const FileBlock&)\n";
}

void
art::RootMPIOutput::
closeFile()
{
    FDEBUG(1) << "Begin/End: RootMPIOutput::closeFile()\n";
}

void
art::RootMPIOutput::
respondToCloseInputFile(FileBlock const&)
{
    FDEBUG(1) << "Begin/End: RootMPIOutput::"
                 "respondToCloseOutputFiles(FileBlock const&)\n";
}

void
art::RootMPIOutput::
respondToCloseOutputFiles(FileBlock const&)
{
    FDEBUG(1) << "Begin/End: RootMPIOutput::"
                 "respondToCloseOutputFiles(FileBlock const&)\n";
}

static
void
send_shutdown_message()
{
    FDEBUG(1) << "Begin: RootMPIOutput static send_shutdown_message()\n";
    //
    //  Construct and send the shutdown message.
    //
    TBufferFile msg(TBuffer::kWrite);
    msg.SetWriteMode();
    //
    //  Stream the message type code.
    //
    {
        FDEBUG(1) << "RootMPIOutput static send_shutdown_message: "
                     "streaming shutdown message type code ...\n";
        msg.WriteULong(5);
        FDEBUG(1) << "RootMPIOutput static send_shutdown_message: "
                     "finished streaming shutdown message type code.\n";
    }
    //
    //
    //  Send the shutdown  message.
    //
    {
        art::ServiceHandle<NetMonTransportService> transport;
        FDEBUG(1) << "RootMPIOutput static send_shutdown_message: "
                     "sending the shutdown message ...\n";
	transport->sendMessage(0, artdaq::Fragment::ShutdownFragmentType, msg);
        FDEBUG(1) << "RootMPIOutput static send_shutdown_message: "
                     "sent the shutdown message.\n";
    }
    FDEBUG(1) << "End:   RootMPIOutput static send_shutdown_message()\n";
}

void
art::RootMPIOutput::
endJob()
{
    FDEBUG(1) << "Begin: RootMPIOutput::endJob()\n";
    send_shutdown_message();
    FDEBUG(1) << "End:   RootMPIOutput::endJob()\n";
}

#pragma GCC push_options
#pragma GCC optimize ("O0")
static
void
send_init_message()
{
    FDEBUG(1) << "Begin: RootMPIOutput static send_init_message()\n";
    //
    //  Get the classes we will need.
    //
    static TClass* string_class = TClass::GetClass("std::string");
    if (string_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput static send_init_message(): "
            "Could not get TClass for std::string!";
    }
    static TClass* product_list_class = TClass::GetClass(
        "map<art::BranchKey,art::BranchDescription>");
    if (product_list_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput static send_init_message(): "
            "Could not get TClass for "
            "map<art::BranchKey,art::BranchDescription>!";
    }
    //typedef std::map<const ProcessHistoryID,ProcessHistory> ProcessHistoryMap;
    //TClass* process_history_map_class = TClass::GetClass(
    //    "std::map<const art::ProcessHistoryID,art::ProcessHistory>");
    //FIXME: Replace the "2" here with a use of the proper enum value!
    static TClass* process_history_map_class = TClass::GetClass(
        "std::map<const art::Hash<2>,art::ProcessHistory>");
    if (process_history_map_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput static send_init_message(): "
            "Could not get class for "
            "std::map<const art::Hash<2>,art::ProcessHistory>!";
    }
    //static TClass* parentage_map_class = TClass::GetClass(
    //    "std::map<const art::ParentageID,art::Parentage>");
    //FIXME: Replace the "5" here with a use of the proper enum value!
    static TClass* parentage_map_class = TClass::GetClass(
        "std::map<const art::Hash<5>,art::Parentage>");
    if (parentage_map_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput static send_init_message(): "
            "Could not get class for "
            "std::map<const art::Hash<5>,art::Parentage>!";
    }
    //
    //  Construct and send the init message.
    //
    TBufferFile msg(TBuffer::kWrite);
    msg.SetWriteMode();
    //
    //  Stream the message type code.
    //
    {
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Streaming message type code ...\n";
        msg.WriteULong(1);
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Finished streaming message type code.\n";
    }
    //
    //  Stream the ParameterSetRegistry.
    //
    {
        unsigned long ps_cnt = fhicl::ParameterSetRegistry::size();
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "parameter set count: " << ps_cnt << '\n';
        msg.WriteULong(ps_cnt);
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Streaming parameter sets ...\n";
        for (auto I = fhicl::ParameterSetRegistry::begin(),
                E = fhicl::ParameterSetRegistry::end(); I != E; ++I) {
            std::string pset_str = I->second.to_string();
            msg.WriteObjectAny(&pset_str, string_class);
        }
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Finished streaming parameter sets.\n";
    }
    //
    //  Stream the MasterProductRegistry.
    //
    {
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Streaming MasterProductRegistry ...\n";
        art::ProductList productList(
            art::ProductMetaData::instance().productList());
        msg.WriteObjectAny(&productList, product_list_class);
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Finished streaming MasterProductRegistry.\n";
    }
    //
    //  Dump The BranchIDListRegistry
    //
    if (art::debugit() >= 2) {
        //typedef vector<BranchID::value_type> BranchIDList
        //typedef vector<BranchIDList> BranchIDLists
        //std::vector<std::vector<art::BranchID::value_type>>
        art::BranchIDLists* bilr =
            &art::BranchIDListRegistry::instance()->data();
        FDEBUG(2) << "RootMPIOutput static send_init_message(): "
                     "Content of BranchIDLists\n";
        int max_bli = bilr->size();
        FDEBUG(2) << "RootMPIOutput static send_init_message(): "
                     "max_bli: " << max_bli << '\n';
        for (int i = 0; i < max_bli; ++i) {
           int max_prdidx = (*bilr)[i].size();
           FDEBUG(2) << "RootMPIOutput static send_init_message(): "
                        "max_prdidx: " << max_prdidx << '\n';
           for (int j = 0; j < max_prdidx; ++j) {
               FDEBUG(2) << "RootMPIOutput static send_init_message(): "
                            "bli: " << i
                         << " prdidx: " << j
                         << " bid: 0x" << std::hex
                         << static_cast<unsigned long>((*bilr)[i][j])
                         << std::dec << '\n';
           }
        }
    }
    //
    //  Dump the ProcessHistoryRegistry.
    //
    if (art::debugit() >= 1) {
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Dumping ProcessHistoryRegistry ...\n";
        //typedef std::map<const ProcessHistoryID,ProcessHistory>
        //    ProcessHistoryMap;
        art::ProcessHistoryMap const& phr = art::ProcessHistoryRegistry::get();
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "phr: size: " << phr.size() << '\n';
        for (auto I = phr.begin(), E = phr.end(); I != E; ++I) {
            std::ostringstream OS;
            I->first.print(OS);
            FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                         "phr: id: '" << OS.str() << "'\n";
        }
    }
    //
    //  Stream the ProcessHistoryRegistry.
    //
    {
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Streaming ProcessHistoryRegistry ...\n";
        //typedef std::map<const ProcessHistoryID,ProcessHistory>
        //    ProcessHistoryMap;
        const art::ProcessHistoryMap& phm = art::ProcessHistoryRegistry::get();
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "phm: size: " << phm.size() << '\n';
        msg.WriteObjectAny(&phm, process_history_map_class);
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Finished streaming ProcessHistoryRegistry.\n";
    }
    //
    //  Stream the ParentageRegistry.
    //
    {
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Streaming ParentageRegistry ...\n";
        //typedef std::map<const ParentageID,Parentage> ParentageMap
        const art::ParentageMap& parentageMap = art::ParentageRegistry::get();
        msg.WriteObjectAny(&parentageMap, parentage_map_class);
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Finished streaming ParentageRegistry.\n";
    }
    //
    //
    //  Send init message.
    //
    {
        art::ServiceHandle<NetMonTransportService> transport;
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Sending the init message ...\n";
	transport->sendMessage(0, artdaq::Fragment::InitFragmentType, msg);
        FDEBUG(1) << "RootMPIOutput static send_init_message(): "
                     "Init message sent.\n";
    }
    FDEBUG(1) << "End:   RootMPIOutput static send_init_message()\n";
}
#pragma GCC pop_options

void
art::RootMPIOutput::
writeDataProducts(TBufferFile& msg, const Principal& principal,
                  std::vector<BranchKey*>& bkv)
{
    FDEBUG(1) << "Begin: RootMPIOutput::writeDataProducts(...)\n";
    //
    //  Fetch the class dictionaries we need for
    //  writing out the data products.
    //
    static TClass* branch_key_class = TClass::GetClass("art::BranchKey");
    if (branch_key_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput::writeDataProducts(...): "
            "Could not get TClass for art::BranchKey!";
    }
    static TClass* prdprov_class = TClass::GetClass("art::ProductProvenance");
    if (prdprov_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput::writeDataProducts(...): "
            "Could not get TClass for art::ProductProvenance!";
    }
    //
    //  Calculate the data product count.
    //
    unsigned long prd_cnt = 0;
    //std::map<art::BranchID, std::shared_ptr<art::Group>>::const_iterator
    for (auto I = principal.begin(), E = principal.end(); I != E; ++I) {
      if (I->second->productUnavailable() || ! selected(I->second->productDescription())) {
            continue;
        }
        ++prd_cnt;
    }
    //
    //  Write the data product count.
    //
    {
        FDEBUG(1) << "RootMPIOutput::writeDataProducts(...): "
                     "Streaming product count: " << prd_cnt << '\n';
        msg.WriteULong(prd_cnt);
        FDEBUG(1) << "RootMPIOutput::writeDataProducts(...): "
                     "Finished streaming product count.\n";
    }
    //
    //  Loop over the groups in the RunPrincipal and
    //  write out the data products.
    //
    // Note: We need this vector of keys because the ROOT I/O mechanism
    //       requires that each object inserted in the message has a
    //       unique address, so we force that by holding on to each
    //       branch key manufactured in the loop until after we are
    //       done constructing the message.
    //
    bkv.reserve(prd_cnt);
    //std::map<art::BranchID, std::shared_ptr<art::Group>>::const_iterator
    for (auto I = principal.begin(), E = principal.end(); I != E; ++I) {
        if (I->second->productUnavailable() || ! selected(I->second->productDescription())) {
            continue;
        }
        const BranchDescription& bd(I->second->productDescription());
        bkv.push_back(new BranchKey(bd));
        if (art::debugit() >= 2) {
            FDEBUG(2) << "RootMPIOutput::writeDataProducts(...): "
                         "Dumping branch key           of class: '"
                      << bkv.back()->friendlyClassName_
                      << "' modlbl: '"
                      << bkv.back()->moduleLabel_
                      << "' instnm: '"
                      << bkv.back()->productInstanceName_
                      << "' procnm: '"
                      << bkv.back()->processName_
                      << "'\n";
        }
        {
            FDEBUG(1) << "RootMPIOutput::writeDataProducts(...): "
                         "Streaming branch key         of class: '"
                      << bd.producedClassName()
                      << "' modlbl: '"
                      << bd.moduleLabel()
                      << "' instnm: '"
                      << bd.productInstanceName()
                      << "' procnm: '"
                      << bd.processName()
                      << "'\n";
            msg.WriteObjectAny(bkv.back(), branch_key_class);
        }
        {
            FDEBUG(1) << "RootMPIOutput::writeDataProducts(...): "
                         "Streaming product            of class: '"
                      << bd.producedClassName()
                      << "' modlbl: '"
                      << bd.moduleLabel()
                      << "' instnm: '"
                      << bd.productInstanceName()
                      << "' procnm: '"
                      << bd.processName()
                      << "'\n";
            OutputHandle oh = principal.getForOutput(bd.branchID(), true);
            const EDProduct* prd = oh.wrapper();
            msg.WriteObjectAny(prd, TClass::GetClass(bd.wrappedName().c_str()));
        }
        {
            FDEBUG(1) << "RootMPIOutput::writeDataProducts(...): "
                         "Streaming product provenance of class: '"
                      << bd.producedClassName()
                      << "' modlbl: '"
                      << bd.moduleLabel()
                      << "' instnm: '"
                      << bd.productInstanceName()
                      << "' procnm: '"
                      << bd.processName()
                      << "'\n";
            const ProductProvenance* prdprov =
                I->second->productProvenancePtr().get();
            msg.WriteObjectAny(prdprov, prdprov_class);
        }
    }
    FDEBUG(1) << "End:   RootMPIOutput::writeDataProducts(...)\n";
}

void
art::RootMPIOutput::
write(CONST_WRITE EventPrincipal& ep)
{
    //
    //  Write an Event message.
    //
    FDEBUG(1) << "Begin: RootMPIOutput::"
                 "write(const EventPrincipal& ep)\n";
    if (!initMsgSent_) {
        send_init_message();
        initMsgSent_ = true;
    }
    //
    //  Get root classes needed for I/O.
    //
    static TClass* run_aux_class = TClass::GetClass("art::RunAuxiliary");
    if (run_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput::write(const EventPrincipal& ep): "
            "Could not get TClass for art::RunAuxiliary!";
    }
    static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
    if (subrun_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput::write(const EventPrincipal& ep): "
            "Could not get TClass for art::SubRunAuxiliary!";
    }
    static TClass* event_aux_class = TClass::GetClass("art::EventAuxiliary");
    if (event_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
           "RootMPIOutput::write(const EventPrincipal& ep): "
           "Could not get TClass for art::EventAuxiliary!";
    }
    static TClass* history_class = TClass::GetClass("art::History");
    if (history_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput::write(const EventPrincipal& ep): "
            "Could not get TClass for art::History!";
    }
    //
    //  Setup message buffer.
    //
    TBufferFile msg(TBuffer::kWrite);
    msg.SetWriteMode();
    //
    //  Write message type code.
    //
    {
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Streaming message type code ...\n";
        msg.WriteULong(4);
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Finished streaming message type code.\n";
    }
    //
    //  Write RunAuxiliary.
    //
    {
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Streaming RunAuxiliary ...\n";
        msg.WriteObjectAny(&ep.subRunPrincipal().runPrincipal().aux(),
                           run_aux_class);
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Finished streaming RunAuxiliary.\n";
    }
    //
    //  Write SubRunAuxiliary.
    //
    {
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Streaming SubRunAuxiliary ...\n";
        msg.WriteObjectAny(&ep.subRunPrincipal().aux(),
                           subrun_aux_class);
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Finished streaming SubRunAuxiliary.\n";
    }
    //
    //  Write EventAuxiliary.
    //
    {
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Streaming EventAuxiliary ...\n";
        msg.WriteObjectAny(&ep.aux(), event_aux_class);
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Finished streaming EventAuxiliary.\n";
    }
    //
    //  Write History.
    //
    {
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Streaming History ...\n";
        msg.WriteObjectAny(&ep.history(), history_class);
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Finished streaming History.\n";
    }
    //
    //  Write data products.
    //
    std::vector<BranchKey*> bkv;
    writeDataProducts(msg, ep, bkv);
    //
    //  Send message.
    //
    {
        ServiceHandle<NetMonTransportService> transport;
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Sending a message ...\n";
	transport->sendMessage(ep.id().event(), artdaq::Fragment::DataFragmentType, msg);
        FDEBUG(1) << "RootMPIOutput::write(const EventPrincipal& ep): "
                     "Message sent.\n";
    }
    //
    //  Delete the branch keys we created for the message.
    //
    for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I) {
        delete *I;
        *I = 0;
    }
    FDEBUG(1) << "End:   RootMPIOutput::write(const EventPrincipal& ep)\n";
}

void
art::RootMPIOutput::
writeRun(CONST_WRITE RunPrincipal& rp)
{
    //
    //  Write an EndRun message.
    //
    FDEBUG(1) << "Begin: RootMPIOutput::writeRun(const RunPrincipal& rp)\n";
    (void) rp;
    if (!initMsgSent_) {
        send_init_message();
        initMsgSent_ = true;
    }
#if 0
    //
    //  Fetch the class dictionaries we need for
    //  writing out the auxiliary information.
    //
    static TClass* run_aux_class = TClass::GetClass("art::RunAuxiliary");
    assert(run_aux_class != nullptr && "writeRun: Could not get TClass for "
           "art::RunAuxiliary!");
    //
    //  Begin preparing message.
    //
    TBufferFile msg(TBuffer::kWrite);
    msg.SetWriteMode();
    //
    //  Write message type code.
    //
    {
        FDEBUG(1) << "writeRun: streaming message type code ...\n";
        msg.WriteULong(2);
        FDEBUG(1) << "writeRun: finished streaming message type code.\n";
    }
    //
    //  Write RunAuxiliary.
    //
    {
        FDEBUG(1) << "writeRun: streaming RunAuxiliary ...\n";
        if (art::debugit() >= 1) {
            FDEBUG(1) << "writeRun: dumping ProcessHistoryRegistry ...\n";
            //typedef std::map<const ProcessHistoryID,ProcessHistory>
            //    ProcessHistoryMap;
            art::ProcessHistoryMap const& phr =
                art::ProcessHistoryRegistry::get();
            FDEBUG(1) << "writeRun: phr: size: " << phr.size() << '\n';
            for (auto I = phr.begin(), E = phr.end(); I != E; ++I) {
                std::ostringstream OS;
                I->first.print(OS);
                FDEBUG(1) << "writeRun: phr: id: '" << OS.str() << "'\n";
                OS.str("");
                FDEBUG(1) << "writeRun: phr: data.size(): "
                          << I->second.data().size() << '\n';
                if (I->second.data().size()) {
                    I->second.data().back().id().print(OS);
                    FDEBUG(1) << "writeRun: phr: data.back().id(): '"
                              << OS.str() << "'\n";
                }
            }
            if (!rp.aux().processHistoryID().isValid()) {
                FDEBUG(1) << "writeRun: ProcessHistoryID: 'INVALID'\n";
            }
            else {
                std::ostringstream OS;
                rp.aux().processHistoryID().print(OS);
                FDEBUG(1) << "writeRun: ProcessHistoryID: '"
                          << OS.str() << "'\n";
                OS.str("");
                const ProcessHistory& processHistory =
                    ProcessHistoryRegistry::get(rp.aux().processHistoryID());
                if (processHistory.data().size()) {
                    // FIXME: Print something special on invalid id() here!
                    processHistory.data().back().id().print(OS);
                    FDEBUG(1) << "writeRun: ProcessConfigurationID: '"
                              << OS.str() << "'\n";
                    OS.str("");
                    FDEBUG(1) << "writeRun: ProcessConfiguration: '"
                              << processHistory.data().back() << '\n';
                }
            }
        }
        msg.WriteObjectAny(&rp.aux(), run_aux_class);
        FDEBUG(1) << "writeRun: streamed RunAuxiliary.\n";
    }
    //
    //  Write data products.
    //
    std::vector<BranchKey*> bkv;
    writeDataProducts(msg, rp, bkv);
    //
    //  Send message.
    //
    {
        ServiceHandle<NetMonTransportService> transport;
        FDEBUG(1) << "writeRun: sending a message ...\n";
	transport->sendMessage(0, artdaq::Fragment::EndOfRunFragmentType, msg);
        FDEBUG(1) << "writeRun: message sent.\n";
    }
    //
    //  Delete the branch keys we created for the message.
    //
    for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I) {
        delete *I;
        *I = 0;
    }
#endif // 0
    FDEBUG(1) << "End:   RootMPIOutput::writeRun(const RunPrincipal& rp)\n";
}

void
art::RootMPIOutput::writeSubRun(CONST_WRITE SubRunPrincipal& srp)
{
    //
    //  Write an EndSubRun message.
    //
    FDEBUG(1) << "Begin: RootMPIOutput::"
                 "writeSubRun(const SubRunPrincipal& srp)\n";
    if (!initMsgSent_) {
        send_init_message();
        initMsgSent_ = true;
    }
    //
    //  Fetch the class dictionaries we need for
    //  writing out the auxiliary information.
    //
    static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
    if (subrun_aux_class == nullptr) {
        throw art::Exception(art::errors::DictionaryNotFound) <<
            "RootMPIOutput::writeSubRun: "
            "Could not get TClass for art::SubRunAuxiliary!";
    }
    //
    //  Begin preparing message.
    //
    TBufferFile msg(TBuffer::kWrite);
    msg.SetWriteMode();
    //
    //  Write message type code.
    //
    {
        FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                     "streaming message type code ...\n";
        msg.WriteULong(3);
        FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                     "finished streaming message type code.\n";
    }
    //
    //  Write SubRunAuxiliary.
    //
    {
        FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                     "streaming SubRunAuxiliary ...\n";
        if (art::debugit() >= 1) {
            FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                         "dumping ProcessHistoryRegistry ...\n";
            //typedef std::map<const ProcessHistoryID,ProcessHistory>
            //    ProcessHistoryMap;
            art::ProcessHistoryMap const& phr =
                art::ProcessHistoryRegistry::get();
            FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                         "phr: size: " << phr.size() << '\n';
            for (auto I = phr.begin(), E = phr.end(); I != E; ++I) {
                std::ostringstream OS;
                I->first.print(OS);
                FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                             "phr: id: '" << OS.str() << "'\n";
                OS.str("");
                FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                             "phr: data.size(): "
                          << I->second.data().size() << '\n';
                if (I->second.data().size()) {
                    I->second.data().back().id().print(OS);
                    FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                                 "phr: data.back().id(): '"
                              << OS.str() << "'\n";
                }
            }
            if (!srp.aux().processHistoryID().isValid()) {
                FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                             "ProcessHistoryID: 'INVALID'\n";
            }
            else {
                std::ostringstream OS;
                srp.aux().processHistoryID().print(OS);
                FDEBUG(1) << "RootMPIOutput::writeSubRun: ProcessHistoryID: '"
                          << OS.str() << "'\n";
                OS.str("");
                const ProcessHistory& processHistory =
                    ProcessHistoryRegistry::get(srp.aux().processHistoryID());
                if (processHistory.data().size()) {
                    // FIXME: Print something special on invalid id() here!
                    processHistory.data().back().id().print(OS);
                    FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                                 "ProcessConfigurationID: '"
                              << OS.str() << "'\n";
                    OS.str("");
                    FDEBUG(1) << "RootMPIOutput::writeSubRun: "
                                 "ProcessConfiguration: '"
                              << processHistory.data().back() << '\n';
                }
            }
        }
        msg.WriteObjectAny(&srp.aux(), subrun_aux_class);
        FDEBUG(1) << "RootMPIOutput::writeSubRun: streamed SubRunAuxiliary.\n";
    }
    //
    //  Write data products.
    //
    std::vector<BranchKey*> bkv;
    writeDataProducts(msg, srp, bkv);
    //
    //  Send message.
    //
    {
        ServiceHandle<NetMonTransportService> transport;
        FDEBUG(1) << "RootMPIOutput::writeSubRun: sending a message ...\n";
	transport->sendMessage(0, artdaq::Fragment::EndOfSubrunFragmentType, msg);
        FDEBUG(1) << "RootMPIOutput::writeSubRun: message sent.\n";

	// Disconnecting will cause EOD fragments to be generated which will
	// allow components downstream to flush data and clean up.
	transport->disconnect();
    }
    //
    //  Delete the branch keys we created for the message.
    //
    for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I) {
        delete *I;
        *I = 0;
    }
    FDEBUG(1) << "End:   RootMPIOutput::"
                 "writeSubRun(const SubRunPrincipal& srp)\n";
}

DEFINE_ART_MODULE(art::RootMPIOutput)

