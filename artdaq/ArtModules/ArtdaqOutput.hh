
#include <TBufferFile.h>
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/OutputHandle.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#if ART_HEX_VERSION < 0x30000
#include "art/Persistency/Provenance/ProductMetaData.h"
#endif

#include <algorithm>
#include <iterator>
#include "canvas/Persistency/Provenance/BranchDescription.h"
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
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"

#include "artdaq/DAQdata/Globals.hh"

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/detail/ParentageMap.hh"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

#include <TBufferFile.h>
#include <TClass.h>
#include <TMessage.h>

#define TLVL_OPENFILE 5
#define TLVL_CLOSEFILE 6
#define TLVL_RESPONDTOCLOSEINPUTFILE 7
#define TLVL_RESPONDTOCLOSEOUTPUTFILE 8
#define TLVL_ENDJOB 9
#define TLVL_SENDINIT 10
#define TLVL_SENDINIT_VERBOSE1 32
#define TLVL_SENDINIT_VERBOSE2 33
#define TLVL_WRITEDATAPRODUCTS 11
#define TLVL_WRITEDATAPRODUCTS_VERBOSE 34
#define TLVL_WRITE 12
#define TLVL_WRITERUN 13
#define TLVL_WRITERUN_VERBOSE 37
#define TLVL_WRITESUBRUN 14
#define TLVL_WRITESUBRUN_VERBOSE 35
#define TLVL_EXTRACTPRODUCTS 15
#define TLVL_EXTRACTPRODUCTS_VERBOSE 36

#if ART_HEX_VERSION < 0x30000
#define RUN_AUX aux
#define SUBRUN_AUX aux
#define EVENT_AUX aux
#define RUN_ID id
#define SUBRUN_ID id
#define EVENT_ID id
#else
#define RUN_AUX runAux
#define SUBRUN_AUX subRunAux
#define EVENT_AUX eventAux
#define RUN_ID runID
#define SUBRUN_ID subRunID
#define EVENT_ID eventID
#endif

namespace art {
class ArtdaqOutput;
}

class art::ArtdaqOutput : public art::OutputModule
{
public:
	explicit ArtdaqOutput(fhicl::ParameterSet const& ps)
	    : OutputModule(ps), initMsgSent_(false), productList_() {}

	virtual ~ArtdaqOutput() = default;

protected:
	virtual void openFile(FileBlock const&)
	{
		TLOG(TLVL_OPENFILE) << "Begin/End: ArtdaqOutput::openFile(const FileBlock&)";
	}

	virtual void closeFile() { TLOG(TLVL_CLOSEFILE) << "Begin/End: ArtdaqOutput::closeFile()"; }

	virtual void respondToCloseInputFile(FileBlock const&)
	{
		TLOG(TLVL_RESPONDTOCLOSEINPUTFILE) << "Begin/End: ArtdaqOutput::"
		                                      "respondToCloseOutputFiles(FileBlock const&)";
	}

	virtual void respondToCloseOutputFiles(FileBlock const&)
	{
		TLOG(TLVL_RESPONDTOCLOSEOUTPUTFILE) << "Begin/End: ArtdaqOutput::"
		                                       "respondToCloseOutputFiles(FileBlock const&)";
	}

	virtual void endJob()
	{
		TLOG(TLVL_ENDJOB) << "Begin/End: ArtdaqOutput::endJob()";
	}

	virtual void beginRun(RunPrincipal const& rp) final { extractProducts_(rp); }

	virtual void beginSubRun(SubRunPrincipal const& srp) final { extractProducts_(srp); }

	virtual void event(EventPrincipal const& ep) final { extractProducts_(ep); }

	virtual void write(EventPrincipal&) final;

	virtual void writeRun(RunPrincipal&) final;

	virtual void writeSubRun(SubRunPrincipal&) final;

	void writeDataProducts(TBufferFile&, const Principal&, std::vector<BranchKey*>&);

	void extractProducts_(Principal const&);

	void send_init_message(History history);

	virtual void SendMessage(artdaq::Fragment::sequence_id_t sequenceId, artdaq::Fragment::type_t messageType, TBufferFile& msg) = 0;

private:
	bool initMsgSent_;
	ProductList productList_;
};

void art::ArtdaqOutput::send_init_message(History history)
{
	TLOG(TLVL_SENDINIT) << "Begin: ArtdaqOutput::send_init_message()";
	//
	//  Get the classes we will need.
	//
	// static TClass* string_class = TClass::GetClass("std::string");
	// if (string_class == nullptr) {
	//	throw art::Exception(art::errors::DictionaryNotFound) <<
	//		"ArtdaqOutput static send_init_message(): "
	//		"Could not get TClass for std::string!";
	//}
	static TClass* product_list_class = TClass::GetClass("std::map<art::BranchKey,art::BranchDescription>");
	if (product_list_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::send_init_message(): "
		                                                         "Could not get TClass for "
		                                                         "map<art::BranchKey,art::BranchDescription>!";
	}
	// typedef std::map<const ProcessHistoryID,ProcessHistory> ProcessHistoryMap;
	// TClass* process_history_map_class = TClass::GetClass(
	//    "std::map<const art::ProcessHistoryID,art::ProcessHistory>");
	// FIXME: Replace the "2" here with a use of the proper enum value!
	static TClass* process_history_map_class = TClass::GetClass("std::map<const art::Hash<2>,art::ProcessHistory>");
	if (process_history_map_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::send_init_message(): "
		                                                         "Could not get class for "
		                                                         "std::map<const art::Hash<2>,art::ProcessHistory>!";
	}
	// static TClass* parentage_map_class = TClass::GetClass(
	//    "std::map<const art::ParentageID,art::Parentage>");
	static TClass* parentage_map_class = TClass::GetClass("art::ParentageMap");
	if (parentage_map_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::send_init_message(): "
		                                                         "Could not get class for ParentageMap.";
	}
	TLOG(TLVL_SENDINIT) << "parentage_map_class: " << (void*)parentage_map_class;

	static TClass* history_class = TClass::GetClass("art::History");
	if (history_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::send_init_message(): "
		                                                         "Could not get TClass for art::History!";
	}

	//
	//  Construct and send the init message.
	//
	TBufferFile msg(TBuffer::kWrite);
	msg.SetWriteMode();
	//
	//  Stream the message type code.
	//
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Streaming message type code ...";
	msg.WriteULong(1);
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Finished streaming message type code.";

	//
	//  Stream the ParameterSetRegistry.
	//
	unsigned long ps_cnt = fhicl::ParameterSetRegistry::size();
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): parameter set count: " << ps_cnt;
	msg.WriteULong(ps_cnt);
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Streaming parameter sets ...";
	for (auto I = std::begin(fhicl::ParameterSetRegistry::get()), E = std::end(fhicl::ParameterSetRegistry::get());
	     I != E; ++I)
	{
		TLOG(TLVL_SENDINIT) << "Pset ID " << I->first << ": " << I->second.to_string();
		std::string pset_str = I->second.to_string();
		// msg.WriteObjectAny(&pset_str, string_class);
		msg.WriteStdString(pset_str);
	}
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Finished streaming parameter sets.";

	//
	//  Stream the MasterProductRegistry.
	//
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Streaming Product List ...";
	msg.WriteObjectAny(&productList_, product_list_class);
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Finished streaming Product List.";

	art::ProcessHistoryMap phr;
	for (auto const& pr : art::ProcessHistoryRegistry::get())
	{
		phr.emplace(pr);
	}
	//
	//  Dump the ProcessHistoryRegistry.
	//
	TLOG(TLVL_SENDINIT_VERBOSE2) << "ArtdaqOutput::send_init_message(): Dumping ProcessHistoryRegistry ...";
	// typedef std::map<const ProcessHistoryID,ProcessHistory>
	//    ProcessHistoryMap;
	TLOG(TLVL_SENDINIT_VERBOSE2) << "ArtdaqOutput::send_init_message(): phr: size: " << phr.size();
	for (auto I = phr.begin(), E = phr.end(); I != E; ++I)
	{
		std::ostringstream OS;
		I->first.print(OS);
		TLOG(TLVL_SENDINIT_VERBOSE2) << "ArtdaqOutput::send_init_message(): phr: id: '" << OS.str() << "'";
	}
	//
	//  Stream the ProcessHistoryRegistry.
	//
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Streaming ProcessHistoryRegistry ...";
	// typedef std::map<const ProcessHistoryID,ProcessHistory>
	//    ProcessHistoryMap;
	const art::ProcessHistoryMap& phm = phr;
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): phm: size: " << phm.size();
	msg.WriteObjectAny(&phm, process_history_map_class);
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Finished streaming ProcessHistoryRegistry.";

	//
	//  Stream the ParentageRegistry.
	//
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Streaming ParentageRegistry ..." << (void*)parentage_map_class;
	art::ParentageMap parentageMap{};
	for (auto const& pr : art::ParentageRegistry::get())
	{
		parentageMap.emplace(pr.first, pr.second);
	}

	msg.WriteObjectAny(&parentageMap, parentage_map_class);

	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Finished streaming ParentageRegistry.";

	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Streaming History";
	msg.WriteObjectAny(&history, history_class);
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Done streaming History";

	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Sending init message";
	SendMessage(0, artdaq::Fragment::InitFragmentType, msg);
	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): Done sending init message";

	TLOG(TLVL_SENDINIT) << "ArtdaqOutput::send_init_message(): END";
}

void art::ArtdaqOutput::writeDataProducts(TBufferFile& msg, const Principal& principal, std::vector<BranchKey*>& bkv)
{
	TLOG(TLVL_WRITEDATAPRODUCTS) << "Begin: ArtdaqOutput::writeDataProducts(...)";
	//
	//  Fetch the class dictionaries we need for
	//  writing out the data products.
	//
	static TClass* branch_key_class = TClass::GetClass("art::BranchKey");
	if (branch_key_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::writeDataProducts(...): "
		                                                         "Could not get TClass for art::BranchKey!";
	}
	static TClass* prdprov_class = TClass::GetClass("art::ProductProvenance");
	if (prdprov_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::writeDataProducts(...): "
		                                                         "Could not get TClass for art::ProductProvenance!";
	}

	//
	//  Calculate the data product count.
	//
	unsigned long prd_cnt = 0;
	// std::map<art::BranchID, std::shared_ptr<art::Group>>::const_iterator
	for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
	{
		auto const& productDescription = I->second->productDescription();
		auto const& refs = keptProducts()[productDescription.branchType()];
		bool found = false;
		for (auto const& ref : refs)
		{
#if ART_HEX_VERSION < 0x30000
			if (*ref == productDescription)
			{
#else
			if (ref.second == productDescription)
			{
#endif
				found = true;
				break;
			}
		}
#if ART_HEX_VERSION < 0x30000
		if (I->second->productUnavailable() || !found)
#else
		if (!I->second->productAvailable() || !found)
#endif
		{
			continue;
		}
		++prd_cnt;
	}
	//
	//  Write the data product count.
	//
	TLOG(TLVL_WRITEDATAPRODUCTS) << "ArtdaqOutput::writeDataProducts(...): Streaming product count: " +
	                                    std::to_string(prd_cnt);
	msg.WriteULong(prd_cnt);
	TLOG(TLVL_WRITEDATAPRODUCTS) << "ArtdaqOutput::writeDataProducts(...): Finished streaming product count.";

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
	// std::map<art::BranchID, std::shared_ptr<art::Group>>::const_iterator
	for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
	{
		auto const& productDescription = I->second->productDescription();
		auto const& refs = keptProducts()[productDescription.branchType()];
		bool found = false;
		for (auto const& ref : refs)
		{
#if ART_HEX_VERSION < 0x30000
			if (*ref == productDescription)
			{
#else
			if (ref.second == productDescription)
			{
#endif
				found = true;
				break;
			}
		}
#if ART_HEX_VERSION < 0x30000
		if (I->second->productUnavailable() || !found)
#else
		if (!I->second->productAvailable() || !found)
#endif
		{
			continue;
		}
		const BranchDescription& bd(I->second->productDescription());
		bkv.push_back(new BranchKey(bd));
		TLOG(TLVL_WRITEDATAPRODUCTS_VERBOSE)
		    << "ArtdaqOutput::writeDataProducts(...): Dumping branch key           of class: '"
		    << bkv.back()->friendlyClassName_ << "' modlbl: '" << bkv.back()->moduleLabel_ << "' instnm: '"
		    << bkv.back()->productInstanceName_ << "' procnm: '" << bkv.back()->processName_ << "'";
		TLOG(TLVL_WRITEDATAPRODUCTS) << "ArtdaqOutput::writeDataProducts(...): "
		                                "Streaming branch key         of class: '"
		                             << bd.producedClassName() << "' modlbl: '" << bd.moduleLabel() << "' instnm: '"
		                             << bd.productInstanceName() << "' procnm: '" << bd.processName() << "'";
		msg.WriteObjectAny(bkv.back(), branch_key_class);

		TLOG(TLVL_WRITEDATAPRODUCTS) << "ArtdaqOutput::writeDataProducts(...): "
		                                "Streaming product            of class: '"
		                             << bd.producedClassName() << "' modlbl: '" << bd.moduleLabel() << "' instnm: '"
		                             << bd.productInstanceName() << "' procnm: '" << bd.processName() << "'";

		OutputHandle oh = principal.getForOutput(bd.productID(), true);
		const EDProduct* prd = oh.wrapper();
		TLOG(TLVL_WRITEDATAPRODUCTS) << "Class for branch " << bd.wrappedName() << " is "
		                             << (void*)TClass::GetClass(bd.wrappedName().c_str());
		msg.WriteObjectAny(prd, TClass::GetClass(bd.wrappedName().c_str()));
		TLOG(TLVL_WRITEDATAPRODUCTS) << "ArtdaqOutput::writeDataProducts(...): "
		                                "Streaming product provenance of class: '"
		                             << bd.producedClassName() << "' modlbl: '" << bd.moduleLabel() << "' instnm: '"
		                             << bd.productInstanceName() << "' procnm: '" << bd.processName() << "'";
#if ART_HEX_VERSION < 0x30000
		const ProductProvenance* prdprov = I->second->productProvenancePtr().get();
#else
		const ProductProvenance* prdprov = I->second->productProvenance().get();
#endif
		msg.WriteObjectAny(prdprov, prdprov_class);
	}
	TLOG(TLVL_WRITEDATAPRODUCTS) << "End:   ArtdaqOutput::writeDataProducts(...)";
}

void art::ArtdaqOutput::write(EventPrincipal& ep)
{
	//
	//  Write an Event message.
	//
	TLOG(TLVL_WRITE) << "Begin: ArtdaqOutput::write(const EventPrincipal& ep)";
	if (!initMsgSent_)
	{
		send_init_message(ep.history());
		initMsgSent_ = true;
	}
	//
	//  Get root classes needed for I/O.
	//
	static TClass* run_aux_class = TClass::GetClass("art::RunAuxiliary");
	if (run_aux_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::write(const EventPrincipal& ep): "
		                                                         "Could not get TClass for art::RunAuxiliary!";
	}
	static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
	if (subrun_aux_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::write(const EventPrincipal& ep): "
		                                                         "Could not get TClass for art::SubRunAuxiliary!";
	}
	static TClass* event_aux_class = TClass::GetClass("art::EventAuxiliary");
	if (event_aux_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::write(const EventPrincipal& ep): "
		                                                         "Could not get TClass for art::EventAuxiliary!";
	}
	static TClass* history_class = TClass::GetClass("art::History");
	if (history_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::write(const EventPrincipal& ep): "
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
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Streaming message type code ...";
	msg.WriteULong(4);
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Finished streaming message type code.";

	//
	//  Write RunAuxiliary.
	//
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Streaming RunAuxiliary ...";
	msg.WriteObjectAny(&ep.subRunPrincipal().runPrincipal().RUN_AUX(), run_aux_class);
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Finished streaming RunAuxiliary.";

	//
	//  Write SubRunAuxiliary.
	//
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Streaming SubRunAuxiliary ...";
	msg.WriteObjectAny(&ep.subRunPrincipal().SUBRUN_AUX(), subrun_aux_class);
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Finished streaming SubRunAuxiliary.";

	//
	//  Write EventAuxiliary.
	//
	{
		TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Streaming EventAuxiliary ...";
		msg.WriteObjectAny(&ep.EVENT_AUX(), event_aux_class);
		TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Finished streaming EventAuxiliary.";
	}
	//
	//  Write History.
	//
	{
		TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Streaming History ...";
		msg.WriteObjectAny(&ep.history(), history_class);
		TLOG(TLVL_WRITE) << "ArtdaqOutput::write(const EventPrincipal& ep): Finished streaming History.";
	}
	//
	//  Write data products.
	//
	std::vector<BranchKey*> bkv;
	writeDataProducts(msg, ep, bkv);

	TLOG(TLVL_WRITE) << "ArtdaqOutput::write: Sending message";
	SendMessage(ep.EVENT_ID().event(), artdaq::Fragment::DataFragmentType, msg);
	TLOG(TLVL_WRITE) << "ArtdaqOutput::write: Done sending message";

	//
	//  Delete the branch keys we created for the message.
	//
	for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
	{
		delete *I;
		*I = 0;
	}
	TLOG(TLVL_WRITE) << "End: ArtdaqOutput::write(const EventPrincipal& ep)";
}

void art::ArtdaqOutput::writeRun(RunPrincipal& rp)
{  //
	//  Write an EndRun message.
	//
	TLOG(TLVL_WRITERUN) << "Begin: ArtdaqOutput::writeRun(const RunPrincipal& rp)";
	(void)rp;
	if (!initMsgSent_)
	{
		send_init_message(rp.history());
		initMsgSent_ = true;
	}
#if 0
	//
	//  Fetch the class dictionaries we need for
	//  writing out the auxiliary information.
	//
	static TClass* run_aux_class = TClass::GetClass("art::RunAuxiliary");
	assert(run_aux_class != nullptr && "writeRun: Could not get TClass for art::RunAuxiliary!");
	//
	//  Begin preparing message.
	//
	TBufferFile msg(TBuffer::kWrite);
	msg.SetWriteMode();
	//
	//  Write message type code.
	//
	{
		TLOG(TLVL_WRITERUN) << "ArtdaqOutput::writeRun: streaming message type code ...";
		msg.WriteULong(2);
		TLOG(TLVL_WRITERUN) << "ArtdaqOutput::writeRun: finished streaming message type code.";
	}
	//
	//  Write RunAuxiliary.
	//
	{
		TLOG(TLVL_WRITERUN) << "ArtdaqOutput::writeRun: streaming RunAuxiliary ...";
		msg.WriteObjectAny(&rp.RUN_AUX(), run_aux_class);
		TLOG(TLVL_WRITERUN) << "ArtdaqOutput::writeRun: streamed RunAuxiliary.";
	}
	//
	//  Write data products.
	//
	std::vector<BranchKey*> bkv;
	writeDataProducts(msg, rp, bkv);

	TLOG(TLVL_WRITERUN) << "ArtdaqOutput::writeRun: Sending message";
	SendMessage(0, artdaq::Fragment::EndOfRunFragmentType, msg);
	TLOG(TLVL_WRITERUN) << "ArtdaqOutput::writeRun: Done sending message";

	for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
	{
		delete *I;
		*I = 0;
	}
#endif
	TLOG(TLVL_WRITERUN) << "End: ArtdaqOutput::writeRun(RunPrincipal& rp)";
}
void art::ArtdaqOutput::writeSubRun(SubRunPrincipal& srp)
{  //
	//  Write an EndSubRun message.
	//
	TLOG(TLVL_WRITESUBRUN) << "Begin: ArtdaqOutput::writeSubRun(const SubRunPrincipal& srp)";
	if (!initMsgSent_)
	{
		send_init_message(srp.history());
		initMsgSent_ = true;
	}
	//
	//  Fetch the class dictionaries we need for
	//  writing out the auxiliary information.
	//
	static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
	if (subrun_aux_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) << "ArtdaqOutput::writeSubRun: "
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
		TLOG(TLVL_WRITESUBRUN) << "ArtdaqOutput::writeSubRun: streaming message type code ...";
		msg.WriteULong(3);
		TLOG(TLVL_WRITESUBRUN) << "ArtdaqOutput::writeSubRun: finished streaming message type code.";
	}
	//
	//  Write SubRunAuxiliary.
	//
	{
		TLOG(TLVL_WRITESUBRUN) << "ArtdaqOutput::writeSubRun: streaming SubRunAuxiliary ...";

		TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: dumping ProcessHistoryRegistry ...";
		// typedef std::map<const ProcessHistoryID,ProcessHistory>
		//    ProcessHistoryMap;
		for (auto I = std::begin(art::ProcessHistoryRegistry::get()), E = std::end(art::ProcessHistoryRegistry::get());
		     I != E; ++I)
		{
			std::ostringstream OS;
			I->first.print(OS);
			TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: phr: id: '" << OS.str() << "'";
			OS.str("");
			TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: phr: data.size():  " << I->second.data().size();
			if (I->second.data().size())
			{
				I->second.data().back().id().print(OS);
				TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: phr: data.back().id(): '" << OS.str() << "'";
			}
		}
		if (!srp.SUBRUN_AUX().processHistoryID().isValid())
		{
			TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: ProcessHistoryID: 'INVALID'";
		}
		else
		{
			std::ostringstream OS;
			srp.SUBRUN_AUX().processHistoryID().print(OS);
			TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: ProcessHistoryID: '" << OS.str() << "'";
			OS.str("");
			ProcessHistory processHistory;
			ProcessHistoryRegistry::get(srp.SUBRUN_AUX().processHistoryID(), processHistory);
			if (processHistory.data().size())
			{
				// FIXME: Print something special on invalid id() here!
				processHistory.data().back().id().print(OS);
				TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: ProcessConfigurationID: '" << OS.str() << "'";
				OS.str("");
				OS << processHistory.data().back();
				TLOG(TLVL_WRITESUBRUN_VERBOSE) << "ArtdaqOutput::writeSubRun: ProcessConfiguration: '" << OS.str();
			}
		}
		msg.WriteObjectAny(&srp.SUBRUN_AUX(), subrun_aux_class);
		TLOG(TLVL_WRITESUBRUN) << "ArtdaqOutput::writeSubRun: streamed SubRunAuxiliary.";
	}
	//
	//  Write data products.
	//
	std::vector<BranchKey*> bkv;
	writeDataProducts(msg, srp, bkv);

	TLOG(TLVL_WRITESUBRUN) << "ArtdaqOutput::writeSubRun: Sending message";
	SendMessage(0, artdaq::Fragment::EndOfSubrunFragmentType, msg);
	TLOG(TLVL_WRITESUBRUN) << "ArtdaqOutput::writeSubRun: Done sending message";

	//
	//  Delete the branch keys we created for the message.
	//
	for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
	{
		delete *I;
		*I = 0;
	}
	TLOG(TLVL_WRITESUBRUN) << "End: ArtdaqOutput::writeSubRun(const SubRunPrincipal& srp)";
}

void art::ArtdaqOutput::extractProducts_(Principal const& principal[[gnu::unused]])
{
	TLOG(TLVL_EXTRACTPRODUCTS) << "Begin: ArtdaqOutput::extractProducts_(Principal const& principal) sz=" << principal.size();
#if ART_HEX_VERSION < 0x30000
	productList_ = art::ProductMetaData::instance().productList();

#else
	for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
	{
		auto const& productDescription = I->second->productDescription();
		auto const& branchKey = BranchKey(productDescription);

		if (!productList_.count(branchKey))
		{
			TLOG(TLVL_EXTRACTPRODUCTS_VERBOSE) << "ArtdaqOutput::extractProducts_:"
			                                   << "Adding branch key to productList of class: '"
			                                   << branchKey.friendlyClassName_ << "' modlbl: '" << branchKey.moduleLabel_ << "' instnm: '"
			                                   << branchKey.productInstanceName_ << "' procnm: '" << branchKey.processName_ << "'";
			productList_[branchKey] = productDescription;
		}
	}
#endif

	TLOG(TLVL_EXTRACTPRODUCTS) << "End: ArtdaqOutput::extractProducts_(Principal const& principal) Product list sz=" << productList_.size();
}
