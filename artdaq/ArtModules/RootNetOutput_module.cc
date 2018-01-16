#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/OutputHandle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#if ART_HEX_VERSION >= 0x20703
# include <iterator>
#else
# include "art/Persistency/Provenance/BranchIDListHelper.h"
#endif
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductMetaData.h"

#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/BranchIDList.h"
#include "canvas/Persistency/Provenance/BranchKey.h"
#include "canvas/Persistency/Provenance/History.h"
#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ProcessConfiguration.h"
#include "canvas/Persistency/Provenance/ProcessConfigurationID.h"
#include "canvas/Persistency/Provenance/ProcessHistoryID.h"
#include "canvas/Persistency/Provenance/ProductList.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"
#include "canvas/Persistency/Provenance/RunAuxiliary.h"
#include "canvas/Persistency/Provenance/SubRunAuxiliary.h"
#include "canvas/Utilities/DebugMacros.h"
#include "canvas/Utilities/Exception.h"
#include "cetlib/column_width.h"
#include "cetlib/lpad.h"
#include "cetlib/rpad.h"
#include <algorithm>
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"

#define TRACE_NAME "RootNetOutput"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq-core/Data/detail/ParentageMap.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

#include <TClass.h>
#include <TMessage.h>

#  define CONST_WRITE

namespace art
{
        class RootNetOutput;
}


/**
 * \brief An art::OutputModule which sends events using DataSenderManager.
 * This module is designed for transporting Fragment-wrapped art::Events after
 * they have been read into art, for example between the EventBuilder and the Aggregator.
 */
class art::RootNetOutput : public OutputModule
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

        virtual void write(EventPrincipal CONST_WRITE&);

        virtual void writeRun(RunPrincipal CONST_WRITE&);

        virtual void writeSubRun(SubRunPrincipal CONST_WRITE&);

        void writeDataProducts(TBufferFile&, const Principal&,
                                                   std::vector<BranchKey*>&);

private:
        bool initMsgSent_;
};

art::RootNetOutput::
RootNetOutput(fhicl::ParameterSet const& ps)
        : OutputModule(ps)
        , initMsgSent_(false)
{
        TLOG_DEBUG("RootNetOutput") << "Begin: RootNetOutput::RootNetOutput(ParameterSet const& ps)" << TLOG_ENDL;
        ServiceHandle<NetMonTransportService> transport;
        transport->connect();
        TLOG_DEBUG("RootNetOutput") << "End:   RootNetOutput::RootNetOutput(ParameterSet const& ps)" << TLOG_ENDL;
}

art::RootNetOutput::
~RootNetOutput()
{
        TLOG_DEBUG("RootNetOutput") << "Begin: RootNetOutput::~RootNetOutput()" << TLOG_ENDL;
        ServiceHandle<NetMonTransportService> transport;
        transport->disconnect();
        TLOG_DEBUG("RootNetOutput") << "End:   RootNetOutput::~RootNetOutput()" << TLOG_ENDL;
}

void
art::RootNetOutput::
openFile(FileBlock const&)
{
        TRACE(5, "RootNetOutput: Begin/End: RootNetOutput::openFile(const FileBlock&)");
}

void
art::RootNetOutput::
closeFile()
{
        TRACE(5, "RootNetOutput: Begin/End: RootNetOutput::closeFile()");
}

void
art::RootNetOutput::
respondToCloseInputFile(FileBlock const&)
{
        TRACE(5, "RootNetOutput: Begin/End: RootNetOutput::"
                "respondToCloseOutputFiles(FileBlock const&)");
}

void
art::RootNetOutput::
respondToCloseOutputFiles(FileBlock const&)
{
        TRACE(5, "RootNetOutput: Begin/End: RootNetOutput::"
                "respondToCloseOutputFiles(FileBlock const&)");
}

static
void
send_shutdown_message()
{
        // 4/1/2016, ELF: Apparently, all this function does is make things not work.
        // At this point in the state machine, RHandles and SHandles have already been
        // destructed. Calling sendMessage will cause SHandles to reconnect itself,
        // but the other end will never recieve the message.
}

void
art::RootNetOutput::
endJob()
{
        TRACE(5, "RootNetOutput: Begin: RootNetOutput::endJob()");
        send_shutdown_message();
        TRACE(5, "RootNetOutput: End:   RootNetOutput::endJob()");
}

//#pragma GCC push_options
//#pragma GCC optimize ("O0")
static
void
send_init_message()
{
        TRACE(5, "RootNetOutput: Begin: RootNetOutput static send_init_message()");
        //
        //  Get the classes we will need.
        //
        //static TClass* string_class = TClass::GetClass("std::string");
        //if (string_class == nullptr) {
        //	throw art::Exception(art::errors::DictionaryNotFound) <<
        //		"RootNetOutput static send_init_message(): "
        //		"Could not get TClass for std::string!";
        //}
        static TClass* product_list_class = TClass::GetClass(
                "std::map<art::BranchKey,art::BranchDescription>");
        if (product_list_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput static send_init_message(): "
                          "Could not get TClass for "
                          "map<art::BranchKey,art::BranchDescription>!";
        }
        //typedef std::map<const ProcessHistoryID,ProcessHistory> ProcessHistoryMap;
        //TClass* process_history_map_class = TClass::GetClass(
        //    "std::map<const art::ProcessHistoryID,art::ProcessHistory>");
        //FIXME: Replace the "2" here with a use of the proper enum value!
        static TClass* process_history_map_class = TClass::GetClass(
                "std::map<const art::Hash<2>,art::ProcessHistory>");
        if (process_history_map_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput static send_init_message(): "
                          "Could not get class for "
                          "std::map<const art::Hash<2>,art::ProcessHistory>!";
        }
        //static TClass* parentage_map_class = TClass::GetClass(
        //    "std::map<const art::ParentageID,art::Parentage>");
        //FIXME: Replace the "5" here with a use of the proper enum value!
        static TClass* parentage_map_class = TClass::GetClass("art::ParentageMap");
        if (parentage_map_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput static send_init_message(): "
                          "Could not get class for ParentageMap.";
        }
        //
        //  Construct and send the init message.
        //
        TBufferFile msg(TBuffer::kWrite);
        msg.SetWriteMode();
        //
        //  Stream the message type code.
        //
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
		      "Streaming message type code ...");
		msg.WriteULong(1);
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
                        "Finished streaming message type code.");

        //
        //  Stream the ParameterSetRegistry.
        //
		unsigned long ps_cnt = fhicl::ParameterSetRegistry::size();
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): parameter set count: " + std::to_string(ps_cnt));
		msg.WriteULong(ps_cnt);
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): Streaming parameter sets ...");
		for (
#            if ART_HEX_VERSION >= 0x20703
                         auto I = std::begin(fhicl::ParameterSetRegistry::get()),
                                 E = std::end(fhicl::ParameterSetRegistry::get());
#            else
			 auto I = fhicl::ParameterSetRegistry::begin(),
				 E = fhicl::ParameterSetRegistry::end();
#            endif
                         I != E; ++I)
		{
			std::string pset_str = I->second.to_string();
			//msg.WriteObjectAny(&pset_str, string_class);
			msg.WriteStdString(pset_str);
		}
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): Finished streaming parameter sets.");

        //
        //  Stream the MasterProductRegistry.
        //
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): Streaming MasterProductRegistry ...");
		art::ProductList productList(
									 art::ProductMetaData::instance().productList());
		msg.WriteObjectAny(&productList, product_list_class);
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): Finished streaming MasterProductRegistry.");

        //
        //  Dump The BranchIDListRegistry
        //
        if (art::debugit() >= 2)
        {
                //typedef vector<BranchID::value_type> BranchIDList
                //typedef vector<BranchIDList> BranchIDLists
                //std::vector<std::vector<art::BranchID::value_type>>
#       if ART_HEX_VERSION >= 0x20703
                art::BranchIDLists const * bilr =
                        &art::BranchIDListRegistry::instance().data();
#       else
                art::BranchIDLists* bilr =
                        &art::BranchIDListRegistry::instance()->data();
#       endif
                FDEBUG(2) << "RootNetOutput static send_init_message(): "
                                 "Content of BranchIDLists\n";
                int max_bli = bilr->size();
                FDEBUG(2) << "RootNetOutput static send_init_message(): "
                                 "max_bli: " << max_bli << '\n';
                for (int i = 0; i < max_bli; ++i)
                {
                        int max_prdidx = (*bilr)[i].size();
                        FDEBUG(2) << "RootNetOutput static send_init_message(): "
                                         "max_prdidx: " << max_prdidx << '\n';
                        for (int j = 0; j < max_prdidx; ++j)
                        {
                                FDEBUG(2) << "RootNetOutput static send_init_message(): "
                                                 "bli: " << i
                                                 << " prdidx: " << j
                                                 << " bid: 0x" << std::hex
                                                 << static_cast<unsigned long>((*bilr)[i][j])
                                                 << std::dec << '\n';
                        }
                }
        }

#   if ART_HEX_VERSION >= 0x20703
        art::ProcessHistoryMap phr;
        for (auto const& pr : art::ProcessHistoryRegistry::get()) {
                phr.emplace(pr);
        }
#   endif
        //
        //  Dump the ProcessHistoryRegistry.
        //
        if (art::debugit() >= 1)
        {
                TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
                        "Dumping ProcessHistoryRegistry ...");
                //typedef std::map<const ProcessHistoryID,ProcessHistory>
                //    ProcessHistoryMap;
#       if ART_HEX_VERSION < 0x20703
                art::ProcessHistoryMap const& phr = art::ProcessHistoryRegistry::get();
#       endif
                TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
                        "phr: size: " + std::to_string(phr.size()));
                for (auto I = phr.begin(), E = phr.end(); I != E; ++I)
                {
                        std::ostringstream OS;
                        I->first.print(OS);
                        TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
                                "phr: id: '" + OS.str() + "'");
                }
        }
        //
        //  Stream the ProcessHistoryRegistry.
        //
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
		      "Streaming ProcessHistoryRegistry ...");
		//typedef std::map<const ProcessHistoryID,ProcessHistory>
		//    ProcessHistoryMap;
#       if ART_HEX_VERSION >= 0x20703
		const art::ProcessHistoryMap& phm = phr;
#       else
		const art::ProcessHistoryMap& phm = art::ProcessHistoryRegistry::get();
#       endif
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
		      "phm: size: " + std::to_string(phm.size()));
		msg.WriteObjectAny(&phm, process_history_map_class);
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
		      "Finished streaming ProcessHistoryRegistry.");

        //
        //  Stream the ParentageRegistry.
        //
		TRACE(5, "RootNetOutput: static send_init_message(): "
		      "Streaming ParentageRegistry ...");
#       if ART_HEX_VERSION >= 0x20703
		art::ParentageMap parentageMap{};
		for (auto const& pr : art::ParentageRegistry::get()) {
			parentageMap.emplace(pr.first, pr.second);
		}
#       else
		const art::ParentageMap& parentageMap = art::ParentageRegistry::get();
#       endif

		msg.WriteObjectAny(&parentageMap, parentage_map_class);

		TRACE(5, "RootNetOutput: static send_init_message(): Finished streaming ParentageRegistry.");

        //
        //
        //  Send init message.
        //
		art::ServiceHandle<NetMonTransportService> transport;
		if (!transport.get())
		{
			TLOG_ERROR("RootNetOutput") << "Could not get handle to NetMonTransportService!" << TLOG_ENDL;
			return;
		}
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
		      "Sending the init message to "
		      + std::to_string(transport->dataReceiverCount()) +
		      " data receivers ...");
		for (size_t idx = 0; idx < transport->dataReceiverCount(); ++idx)
		{
			transport->sendMessage(idx, artdaq::Fragment::InitFragmentType, msg);
		}
		TRACE(5, "RootNetOutput: RootNetOutput static send_init_message(): "
		      "Init message(s) sent.");

        TRACE(5, "RootNetOutput: End:   RootNetOutput static send_init_message()");
}

//#pragma GCC pop_options

void
art::RootNetOutput::
writeDataProducts(TBufferFile& msg, const Principal& principal,
                                  std::vector<BranchKey*>& bkv)
{
        TRACE(5, "RootNetOutput: Begin: RootNetOutput::writeDataProducts(...)");
        //
        //  Fetch the class dictionaries we need for
        //  writing out the data products.
        //
        static TClass* branch_key_class = TClass::GetClass("art::BranchKey");
        if (branch_key_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::writeDataProducts(...): "
                          "Could not get TClass for art::BranchKey!";
        }
        static TClass* prdprov_class = TClass::GetClass("art::ProductProvenance");
        if (prdprov_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::writeDataProducts(...): "
                          "Could not get TClass for art::ProductProvenance!";
        }
        //
        //  Calculate the data product count.
        //
        unsigned long prd_cnt = 0;
        //std::map<art::BranchID, std::shared_ptr<art::Group>>::const_iterator
        for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
        {
                if (I->second->productUnavailable() || !selected(I->second->productDescription()))
                {
                        continue;
                }
                ++prd_cnt;
        }
        //
        //  Write the data product count.
        //
		TRACE(5, "RootNetOutput: RootNetOutput::writeDataProducts(...): "
		      "Streaming product count: " + std::to_string(prd_cnt));
		msg.WriteULong(prd_cnt);
		TRACE(5, "RootNetOutput: RootNetOutput::writeDataProducts(...): "
		      "Finished streaming product count.");

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
        for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
        {
                if (I->second->productUnavailable() || !selected(I->second->productDescription()))
                {
                        continue;
                }
                const BranchDescription& bd(I->second->productDescription());
                bkv.push_back(new BranchKey(bd));
                if (art::debugit() >= 2)
                {
                        FDEBUG(2) << "RootNetOutput::writeDataProducts(...): "
                                         "Dumping branch key           of class: '"
                                         << bkv.back()->friendlyClassName_
                                         << "' modlbl: '"
                                         << bkv.back()->moduleLabel_
                                         << "' instnm: '"
                                         << bkv.back()->productInstanceName_
                                         << "' procnm: '"
                                         << bkv.back()->processName_
                                         << "'";
                }
				TRACE(5, "RootNetOutput: RootNetOutput::writeDataProducts(...): "
				      "Streaming branch key         of class: '"
				      + bd.producedClassName()
				      + "' modlbl: '"
				      + bd.moduleLabel()
				      + "' instnm: '"
				      + bd.productInstanceName()
				      + "' procnm: '"
				      + bd.processName()
				      + "'");
				msg.WriteObjectAny(bkv.back(), branch_key_class);
				TRACE(5, "RootNetOutput: RootNetOutput::writeDataProducts(...): "
				      "Streaming product            of class: '"
				      + bd.producedClassName()
				      + "' modlbl: '"
				      + bd.moduleLabel()
				      + "' instnm: '"
				      + bd.productInstanceName()
				      + "' procnm: '"
				      + bd.processName()
				      + "'");
				OutputHandle oh = principal.getForOutput(bd.branchID(), true);
				const EDProduct* prd = oh.wrapper();
				msg.WriteObjectAny(prd, TClass::GetClass(bd.wrappedName().c_str()));
				TRACE(5, "RootNetOutput: RootNetOutput::writeDataProducts(...): "
				      "Streaming product provenance of class: '"
				      + bd.producedClassName()
				      + "' modlbl: '"
				      + bd.moduleLabel()
				      + "' instnm: '"
				      + bd.productInstanceName()
				      + "' procnm: '"
				      + bd.processName()
				      + "'");
				const ProductProvenance* prdprov =
					I->second->productProvenancePtr().get();
				msg.WriteObjectAny(prdprov, prdprov_class);
        }
        TRACE(5, "RootNetOutput: End:   RootNetOutput::writeDataProducts(...)");
}

void
art::RootNetOutput::
write(CONST_WRITE EventPrincipal& ep)
{
        //
        //  Write an Event message.
        //
        TRACE(5, "RootNetOutput: Begin: RootNetOutput::"
                "write(const EventPrincipal& ep)");
        if (!initMsgSent_)
        {
                send_init_message();
                initMsgSent_ = true;
        }
        //
        //  Get root classes needed for I/O.
        //
        static TClass* run_aux_class = TClass::GetClass("art::RunAuxiliary");
        if (run_aux_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::write(const EventPrincipal& ep): "
                          "Could not get TClass for art::RunAuxiliary!";
        }
        static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
        if (subrun_aux_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::write(const EventPrincipal& ep): "
                          "Could not get TClass for art::SubRunAuxiliary!";
        }
        static TClass* event_aux_class = TClass::GetClass("art::EventAuxiliary");
        if (event_aux_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::write(const EventPrincipal& ep): "
                          "Could not get TClass for art::EventAuxiliary!";
        }
        static TClass* history_class = TClass::GetClass("art::History");
        if (history_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::write(const EventPrincipal& ep): "
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
		TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
		      "Streaming message type code ...");
		msg.WriteULong(4);
		TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
		      "Finished streaming message type code.");

        //
        //  Write RunAuxiliary.
        //
		TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
		      "Streaming RunAuxiliary ...");
		msg.WriteObjectAny(&ep.subRunPrincipal().runPrincipal().aux(),
		                   run_aux_class);
		TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
		      "Finished streaming RunAuxiliary.");

        //
        //  Write SubRunAuxiliary.
        //
		TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
		      "Streaming SubRunAuxiliary ...");
		msg.WriteObjectAny(&ep.subRunPrincipal().aux(),
		                   subrun_aux_class);
		TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
		      "Finished streaming SubRunAuxiliary.");

        //
        //  Write EventAuxiliary.
        //
        {
                TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
                        "Streaming EventAuxiliary ...");
                msg.WriteObjectAny(&ep.aux(), event_aux_class);
                TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
                        "Finished streaming EventAuxiliary.");
        }
        //
        //  Write History.
        //
        {
                TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
                        "Streaming History ...");
                msg.WriteObjectAny(&ep.history(), history_class);
                TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
                        "Finished streaming History.");
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
				if (!transport.get())
				{
					TLOG_ERROR("RootNetOutput") << "Could not get handle to NetMonTransportService!" << TLOG_ENDL;
					return;
				}
                TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
                        "Sending a message ...");
                transport->sendMessage(ep.id().event(), artdaq::Fragment::DataFragmentType, msg);
                TRACE(5, "RootNetOutput: RootNetOutput::write(const EventPrincipal& ep): "
                        "Message sent.");
        }
        //
        //  Delete the branch keys we created for the message.
        //
        for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
        {
                delete *I;
                *I = 0;
        }
        TRACE(5, "RootNetOutput: End:   RootNetOutput::write(const EventPrincipal& ep)");
}

void
art::RootNetOutput::
writeRun(CONST_WRITE RunPrincipal& rp)
{
        //
        //  Write an EndRun message.
        //
        TRACE(5, "RootNetOutput: Begin: RootNetOutput::writeRun(const RunPrincipal& rp)");
        (void)rp;
        if (!initMsgSent_)
        {
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
                TRACE(5, "RootNetOutput: writeRun: streaming message type code ...");
                msg.WriteULong(2);
                TRACE(5, "RootNetOutput: writeRun: finished streaming message type code.");
        }
        //
        //  Write RunAuxiliary.
        //
        {
                TRACE(5, "RootNetOutput: writeRun: streaming RunAuxiliary ...");
                if (art::debugit() >= 1) {
                        TRACE(5, "RootNetOutput: writeRun: dumping ProcessHistoryRegistry ...");
        //typedef std::map<const ProcessHistoryID,ProcessHistory>
        //    ProcessHistoryMap;
                        art::ProcessHistoryMap const& phr =
                                art::ProcessHistoryRegistry::get();
                        TRACE(5, "RootNetOutput: writeRun: phr: size: " << phr.size() << '\n';
                        for (auto I = phr.begin(), E = phr.end(); I != E; ++I) {
                                std::ostringstream OS;
                                I->first.print(OS);
                                TRACE(5, "RootNetOutput: writeRun: phr: id: '" << OS.str() << "'");
                                OS.str("");
                                TRACE(5, "RootNetOutput: writeRun: phr: data.size(): "
                                          << I->second.data().size() << '\n';
                                if (I->second.data().size()) {
                                        I->second.data().back().id().print(OS);
                                        TRACE(5, "RootNetOutput: writeRun: phr: data.back().id(): '"
                                                  << OS.str() << "'");
                                }
                        }
                        if (!rp.aux().processHistoryID().isValid()) {
                                TRACE(5, "RootNetOutput: writeRun: ProcessHistoryID: 'INVALID'");
                        }
                        else {
                                std::ostringstream OS;
                                rp.aux().processHistoryID().print(OS);
                                TRACE(5, "RootNetOutput: writeRun: ProcessHistoryID: '"
                                          << OS.str() << "'");
                                OS.str("");
                                const ProcessHistory& processHistory =
                                        ProcessHistoryRegistry::get(rp.aux().processHistoryID());
                                if (processHistory.data().size()) {
        // FIXME: Print something special on invalid id() here!
                                        processHistory.data().back().id().print(OS);
                                        TRACE(5, "RootNetOutput: writeRun: ProcessConfigurationID: '"
                                                  << OS.str() << "'");
                                        OS.str("");
                                        TRACE(5, "RootNetOutput: writeRun: ProcessConfiguration: '"
                                                  << processHistory.data().back() << '\n';
                                }
                        }
                }
                msg.WriteObjectAny(&rp.aux(), run_aux_class);
                TRACE(5, "RootNetOutput: writeRun: streamed RunAuxiliary.");
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
				if (!transport.get())
				{
					TLOG_ERROR("RootNetOutput") << "Could not get handle to NetMonTransportService!" << TLOG_ENDL;
					return;
}
                TRACE(5, "RootNetOutput: writeRun: sending a message ...");
                transport->sendMessage(0, artdaq::Fragment::EndOfRunFragmentType, msg);
                TRACE(5, "RootNetOutput: writeRun: message sent.");
        }
        //
        //  Delete the branch keys we created for the message.
        //
        for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I) {
                delete *I;
                *I = 0;
        }
#endif // 0
        TRACE(5, "RootNetOutput: End:   RootNetOutput::writeRun(const RunPrincipal& rp)");
}

void
art::RootNetOutput::writeSubRun(CONST_WRITE SubRunPrincipal& srp)
{
        //
        //  Write an EndSubRun message.
        //
        TRACE(5, "RootNetOutput: Begin: RootNetOutput::"
                "writeSubRun(const SubRunPrincipal& srp)");
        if (!initMsgSent_)
        {
                send_init_message();
                initMsgSent_ = true;
        }
        //
        //  Fetch the class dictionaries we need for
        //  writing out the auxiliary information.
        //
        static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
        if (subrun_aux_class == nullptr)
        {
                throw art::Exception(art::errors::DictionaryNotFound) <<
                          "RootNetOutput::writeSubRun: "
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
                TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                        "streaming message type code ...");
                msg.WriteULong(3);
                TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                        "finished streaming message type code.");
        }
        //
        //  Write SubRunAuxiliary.
        //
        {
                TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                        "streaming SubRunAuxiliary ...");
                if (art::debugit() >= 1)
                {
                        TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                                "dumping ProcessHistoryRegistry ...");
                        //typedef std::map<const ProcessHistoryID,ProcessHistory>
                        //    ProcessHistoryMap;
#          if ART_HEX_VERSION >= 0x20703
                        for (auto I = std::begin(art::ProcessHistoryRegistry::get())
                                         , E = std::end(art::ProcessHistoryRegistry::get()); I != E; ++I)
#          else
                        art::ProcessHistoryMap const& phr =
                                art::ProcessHistoryRegistry::get();
                        TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                                "phr: size: " + std::to_string(phr.size()));
                        for (auto I = phr.begin(), E = phr.end(); I != E; ++I)
#          endif
			{
				std::ostringstream OS;
				I->first.print(OS);
				TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
					"phr: id: '" + OS.str() + "'");
				OS.str("");
				TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
					  "phr: data.size(): %zu",I->second.data().size() );
				if (I->second.data().size())
				{
					I->second.data().back().id().print(OS);
					TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
						"phr: data.back().id(): '"
						+ OS.str() + "'");
				}
			}
			if (!srp.aux().processHistoryID().isValid())
			{
				TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
					"ProcessHistoryID: 'INVALID'");
			}
			else
			{
				std::ostringstream OS;
				srp.aux().processHistoryID().print(OS);
				TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: ProcessHistoryID: '"
					+ OS.str() + "'");
				OS.str("");
#              if ART_HEX_VERSION >= 0x20703
                                ProcessHistory processHistory;
                                ProcessHistoryRegistry::get(srp.aux().processHistoryID(),processHistory);
#              else
                                const ProcessHistory& processHistory =
                                        ProcessHistoryRegistry::get(srp.aux().processHistoryID());
#              endif
                                if (processHistory.data().size())
                                {
                                        // FIXME: Print something special on invalid id() here!
                                        processHistory.data().back().id().print(OS);
                                        TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                                                "ProcessConfigurationID: '"
                                                + OS.str() + "'");
                                        OS.str("");
                                        OS << processHistory.data().back();
                                        TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
                                                "ProcessConfiguration: '"
                                                + OS.str());
                                }
                        }
                }
                msg.WriteObjectAny(&srp.aux(), subrun_aux_class);
                TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: streamed SubRunAuxiliary.");
        }
        //
        //  Write data products.
        //
        std::vector<BranchKey*> bkv;
        writeDataProducts(msg, srp, bkv);
        //
        //  Send message.
        //
		ServiceHandle<NetMonTransportService> transport;
		if (!transport.get())
		{
			TLOG_ERROR("RootNetOutput") << "Could not get handle to NetMonTransportService!" << TLOG_ENDL;
			return;
		}
		TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
		      "Sending the EndOfSubrun message to "
		      + std::to_string(transport->dataReceiverCount())
		      + " data receivers ...");
		for (size_t idx = 0; idx < transport->dataReceiverCount(); ++idx)
		{
			transport->sendMessage(idx, artdaq::Fragment::EndOfSubrunFragmentType, msg);
		}
		TRACE(5, "RootNetOutput: RootNetOutput::writeSubRun: "
		      "EndOfSubrun message(s) sent.");
		
		// Disconnecting will cause EOD fragments to be generated which will
		// allow components downstream to flush data and clean up.
		transport->disconnect();
 
        //
        //  Delete the branch keys we created for the message.
        //
        for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
        {
                delete *I;
                *I = 0;
        }
        TRACE(5, "RootNetOutput: End:   RootNetOutput::writeSubRun(const SubRunPrincipal& srp)");
}

DEFINE_ART_MODULE(art::RootNetOutput)
