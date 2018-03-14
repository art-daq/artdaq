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
#if ART_HEX_VERSION < 0x20900
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "canvas/Persistency/Provenance/BranchIDList.h"
#endif
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductMetaData.h"

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
#include <algorithm>
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"

#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq/TransferPlugins/MakeTransferPlugin.hh"
#include "artdaq-core/Data/detail/ParentageMap.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

#include <TClass.h>
#include <TMessage.h>

namespace art
{
	class TransferOutput;
}


/**
* \brief An art::OutputModule which sends events using DataSenderManager.
* This module is designed for transporting Fragment-wrapped art::Events after
* they have been read into art, for example between the EventBuilder and the Aggregator.
*/
class art::TransferOutput : public OutputModule
{
public:
	/**
	* \brief TransferOutput Constructor
	* \param ps ParameterSet used to configure TransferOutput
	*
	* TransferOutput accepts no Parameters beyond those which art::OutputModule takes.
	* See the art::OutputModule documentation for more details on those Parameters.
	*/
	explicit TransferOutput(fhicl::ParameterSet const& ps);

	/**
	* \brief TransferOutput Destructor
	*/
	~TransferOutput();

private:
	virtual void openFile(FileBlock const&);

	virtual void closeFile();

	virtual void respondToCloseInputFile(FileBlock const&);

	virtual void respondToCloseOutputFiles(FileBlock const&);

	virtual void endJob();

	virtual void write(EventPrincipal&);

	virtual void writeRun(RunPrincipal&);

	virtual void writeSubRun(SubRunPrincipal&);

	void writeDataProducts(TBufferFile&, const Principal&,
		std::vector<BranchKey*>&);

private:
	bool initMsgSent_;
	size_t send_timeout_us_;
	size_t send_retry_count_;
	std::unique_ptr<artdaq::TransferInterface> transfer_;

	void sendMessage_(uint64_t sequenceId, uint8_t messageType, TBufferFile& msg);
	void send_init_message();

};

art::TransferOutput::
TransferOutput(fhicl::ParameterSet const& ps)
	: OutputModule(ps)
	, initMsgSent_(false)
	, send_timeout_us_(ps.get<size_t>("send_timeout_us", 5000000))
	, send_retry_count_(ps.get<size_t>("send_retry_count", 5))
{
	TLOG_DEBUG("TransferOutput") << "Begin: TransferOutput::TransferOutput(ParameterSet const& ps)" << TLOG_ENDL;
	transfer_ = artdaq::MakeTransferPlugin(ps, "transfer_plugin", artdaq::TransferInterface::Role::kSend);
	TLOG_DEBUG("TransferOutput") << "END: TransferOutput::TransferOutput" << TLOG_ENDL;
}

art::TransferOutput::
~TransferOutput()
{
	TLOG_DEBUG("TransferOutput") << "Begin: TransferOutput::~TransferOutput()" << TLOG_ENDL;

	auto sts = transfer_->moveFragment(std::move(*artdaq::Fragment::eodFrag(0)));
	if (sts != artdaq::TransferInterface::CopyStatus::kSuccess) TLOG_ERROR("TransferOutput") << "Error sending EOD Fragment!" << TLOG_ENDL;
	transfer_.reset(nullptr);
}

void
art::TransferOutput::
openFile(FileBlock const&)
{
	TLOG_TRACE("TransferOutput") << "Begin/End: TransferOutput::openFile(const FileBlock&)" << TLOG_ENDL;
}

void
art::TransferOutput::
closeFile()
{
	TLOG_TRACE("TransferOutput") << "Begin/End: TransferOutput::closeFile()" << TLOG_ENDL;
}

void
art::TransferOutput::
respondToCloseInputFile(FileBlock const&)
{
	TLOG_TRACE("TransferOutput") << "Begin/End: TransferOutput::" <<
		"respondToCloseOutputFiles(FileBlock const&)" << TLOG_ENDL;
}

void
art::TransferOutput::
respondToCloseOutputFiles(FileBlock const&)
{
	TLOG_TRACE("TransferOutput") << "Begin/End: TransferOutput::" <<
		"respondToCloseOutputFiles(FileBlock const&)" << TLOG_ENDL;
}

void
art::TransferOutput::
endJob()
{
	TLOG_TRACE("TransferOutput") << "Begin: TransferOutput::endJob()" << TLOG_ENDL;
}

//#pragma GCC push_options
//#pragma GCC optimize ("O0")
void
art::TransferOutput::
send_init_message()
{
	TLOG_TRACE("TransferOutput") << "Begin: TransferOutput static send_init_message()" << TLOG_ENDL;
	//
	//  Get the classes we will need.
	//
	//static TClass* string_class = TClass::GetClass("std::string");
	//if (string_class == nullptr) {
	//	throw art::Exception(art::errors::DictionaryNotFound) <<
	//		"TransferOutput static send_init_message(): "
	//		"Could not get TClass for std::string!";
	//}
	static TClass* product_list_class = TClass::GetClass(
		"std::map<art::BranchKey,art::BranchDescription>");
	if (product_list_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) <<
			"TransferOutput static send_init_message(): "
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
			"TransferOutput static send_init_message(): "
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
			"TransferOutput static send_init_message(): "
			"Could not get class for ParentageMap";
	}
	//
	//  Construct and send the init message.
	//
	TBufferFile msg(TBuffer::kWrite);
	msg.SetWriteMode();
	//
	//  Stream the message type code.
	//
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): "
		"Streaming message type code ..." << TLOG_ENDL;
	msg.WriteULong(1);
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): "
		"Finished streaming message type code." << TLOG_ENDL;

	//
	//  Stream the ParameterSetRegistry.
	//
	unsigned long ps_cnt = fhicl::ParameterSetRegistry::size();
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): parameter set count: " << std::to_string(ps_cnt) << TLOG_ENDL;
	msg.WriteULong(ps_cnt);
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): Streaming parameter sets ..." << TLOG_ENDL;
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
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): Finished streaming parameter sets." << TLOG_ENDL;

	//
	//  Stream the MasterProductRegistry.
	//
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): Streaming MasterProductRegistry ..." << TLOG_ENDL;;
	art::ProductList productList(art::ProductMetaData::instance().productList());
	msg.WriteObjectAny(&productList, product_list_class);
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): Finished streaming MasterProductRegistry." << TLOG_ENDL;

#if ART_HEX_VERSION < 0x20900
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
		TLOG_TRACE("TransferOutput") << "TransferOutput static send_init_message(): " << "Content of BranchIDLists" << TLOG_ENDL;
		int max_bli = bilr->size();
		TLOG_TRACE("TransferOutput") << "TransferOutput static send_init_message(): " <<
			"max_bli: " << max_bli << TLOG_ENDL;
		for (int i = 0; i < max_bli; ++i)
		{
			int max_prdidx = (*bilr)[i].size();
			TLOG_TRACE("TransferOutput") << "TransferOutput static send_init_message(): " <<
				"max_prdidx: " << max_prdidx << TLOG_ENDL;
			for (int j = 0; j < max_prdidx; ++j)
			{
				TLOG_TRACE("TransferOutput") << "TransferOutput static send_init_message(): " <<
					"bli: " << i
					<< " prdidx: " << j
					<< " bid: 0x" << std::hex
					<< static_cast<unsigned long>((*bilr)[i][j])
					<< std::dec << TLOG_ENDL;
			}
		}
}
#endif

#   if ART_HEX_VERSION >= 0x20703
	art::ProcessHistoryMap phr;
	for (auto const& pr : art::ProcessHistoryRegistry::get())
	{
		phr.emplace(pr);
	}
#   endif
	//
	//  Dump the ProcessHistoryRegistry.
	//
	if (art::debugit() >= 1)
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): "
			"Dumping ProcessHistoryRegistry ..." << TLOG_ENDL;
		//typedef std::map<const ProcessHistoryID,ProcessHistory>
		//    ProcessHistoryMap;
#       if ART_HEX_VERSION < 0x20703
		art::ProcessHistoryMap const& phr = art::ProcessHistoryRegistry::get();
#       endif
		TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): " <<
			"phr: size: " + std::to_string(phr.size()) << TLOG_ENDL;
		for (auto I = phr.begin(), E = phr.end(); I != E; ++I)
		{
			std::ostringstream OS;
			I->first.print(OS);
			TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): " <<
				"phr: id: '" + OS.str() + "'" << TLOG_ENDL;
		}
	}
	//
	//  Stream the ProcessHistoryRegistry.
	//

	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): " <<
		"Streaming ProcessHistoryRegistry ..." << TLOG_ENDL;
	//typedef std::map<const ProcessHistoryID,ProcessHistory>
	//    ProcessHistoryMap;
#       if ART_HEX_VERSION >= 0x20703
	const art::ProcessHistoryMap& phm = phr;
#       else
	const art::ProcessHistoryMap& phm = art::ProcessHistoryRegistry::get();
#       endif
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): " <<
		"phm: size: " << std::to_string(phm.size()) << TLOG_ENDL;
	msg.WriteObjectAny(&phm, process_history_map_class);
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): " <<
		"Finished streaming ProcessHistoryRegistry." << TLOG_ENDL;

	//
	//  Stream the ParentageRegistry.
	//
#if 0
	TLOG_DEBUG("TransferOutput") << "TransferOutput static send_init_message(): " <<
		"Streaming ParentageRegistry ... sz=" << std::to_string(msg.Length()) << TLOG_ENDL;
#endif
#       if ART_HEX_VERSION >= 0x20703
	art::ParentageMap parentageMap{};
	for (auto const& pr : art::ParentageRegistry::get())
	{
		parentageMap.emplace(pr.first, pr.second);
	}
#       else
	const art::ParentageMap& parentageMap = art::ParentageRegistry::get();
#       endif

	TLOG_TRACE("TransferOutput") << "Before WriteObjectAny ParentageMap" << TLOG_ENDL;
#if 0
	auto sts =
#endif
		msg.WriteObjectAny(&parentageMap, parentage_map_class);
	TLOG_TRACE("RootMPIOuptut") << "After WriteObjectAny ParentageMap" << TLOG_ENDL;
#if 0
	TLOG_DEBUG("TransferOutput") << "TransferOutput: TransferOutput static send_init_message(): " <<
		"Finished streaming ParentageRegistry." << " sts=" << sts << ", sz=" << std::to_string(msg.Length()) << TLOG_ENDL;
#endif

	//
	//
	//  Send init message.
	//
	TLOG_DEBUG("TransferOutput") << "TransferOutput: TransferOutput static send_init_message(): " << "Sending the init message." << TLOG_ENDL;
	sendMessage_(artdaq::Fragment::InvalidSequenceID - 1, artdaq::Fragment::InitFragmentType, msg);
	TLOG_TRACE("TransferOutput") << " TransferOutput static send_init_message(): "
		"Init message(s) sent." << TLOG_ENDL;

	TLOG_TRACE("TransferOutput") << " End:   TransferOutput static send_init_message()" << TLOG_ENDL;
	}

#//pragma GCC pop_options

void
art::TransferOutput::
writeDataProducts(TBufferFile& msg, const Principal& principal,
	std::vector<BranchKey*>& bkv)
{
	TLOG_TRACE("TransferOutput") << " Begin: TransferOutput::writeDataProducts(...)" << TLOG_ENDL;
	//
	//  Fetch the class dictionaries we need for
	//  writing out the data products.
	//
	static TClass* branch_key_class = TClass::GetClass("art::BranchKey");
	if (branch_key_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) <<
			"TransferOutput::writeDataProducts(...): "
			"Could not get TClass for art::BranchKey!";
	}
	static TClass* prdprov_class = TClass::GetClass("art::ProductProvenance");
	if (prdprov_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) <<
			"TransferOutput::writeDataProducts(...): "
			"Could not get TClass for art::ProductProvenance!";
	}
	//
	//  Calculate the data product count.
	//
	unsigned long prd_cnt = 0;
	//std::map<art::BranchID, std::shared_ptr<art::Group>>::const_iterator
	for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
	{
#if ART_HEX_VERSION > 0x20800
		auto const& productDescription = I->second->productDescription();
		auto const& refs = keptProducts()[productDescription.branchType()];
		bool found = false;
		for (auto const& ref : refs)
		{
			if (*ref == productDescription) {
				found = true;
				break;
			}
		}
		if (I->second->productUnavailable() || !found)
		{
			continue;
		}
#else
		if (I->second->productUnavailable() || !selected(I->second->productDescription()))
		{
			continue;
		}

#endif
		++prd_cnt;
		}
	//
	//  Write the data product count.
	//
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeDataProducts(...): "
			"Streaming product count: " << std::to_string(prd_cnt) << TLOG_ENDL;
		msg.WriteULong(prd_cnt);
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeDataProducts(...): "
			"Finished streaming product count." << TLOG_ENDL;
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
	for (auto I = principal.begin(), E = principal.end(); I != E; ++I)
	{
#if ART_HEX_VERSION > 0x20800
		auto const& productDescription = I->second->productDescription();
		auto const& refs = keptProducts()[productDescription.branchType()];
		bool found = false;
		for (auto const& ref : refs)
		{
			if (*ref == productDescription) {
				found = true;
				break;
			}
		}
		if (I->second->productUnavailable() || !found)
		{
			continue;
		}
#else
		if (I->second->productUnavailable() || !selected(I->second->productDescription()))
		{
			continue;
		}

#endif
		const BranchDescription& bd(I->second->productDescription());
		bkv.push_back(new BranchKey(bd));
		if (art::debugit() >= 2)
		{
			TLOG_TRACE("TransferOutput") << "TransferOutput::writeDataProducts(...): "
				"Dumping branch key           of class: '"
				<< bkv.back()->friendlyClassName_
				<< "' modlbl: '"
				<< bkv.back()->moduleLabel_
				<< "' instnm: '"
				<< bkv.back()->productInstanceName_
				<< "' procnm: '"
				<< bkv.back()->processName_
				<< "'" << TLOG_ENDL;
		}
		{
			TLOG_TRACE("TransferOutput") << " TransferOutput::writeDataProducts(...): "
				"Streaming branch key         of class: '"
				+ bd.producedClassName()
				+ "' modlbl: '"
				+ bd.moduleLabel()
				+ "' instnm: '"
				+ bd.productInstanceName()
				+ "' procnm: '"
				+ bd.processName()
				+ "'" << TLOG_ENDL;
			msg.WriteObjectAny(bkv.back(), branch_key_class);
		}
		{
			TLOG_TRACE("TransferOutput") << " TransferOutput::writeDataProducts(...): "
				"Streaming product            of class: '"
				+ bd.producedClassName()
				+ "' modlbl: '"
				+ bd.moduleLabel()
				+ "' instnm: '"
				+ bd.productInstanceName()
				+ "' procnm: '"
				+ bd.processName()
				+ "'" << TLOG_ENDL;
#if ART_HEX_VERSION > 0x20800
			OutputHandle oh = principal.getForOutput(bd.productID(), true);
#else
			OutputHandle oh = principal.getForOutput(bd.branchID(), true);
#endif
			const EDProduct* prd = oh.wrapper();
			msg.WriteObjectAny(prd, TClass::GetClass(bd.wrappedName().c_str()));
		}
		{
			TLOG_TRACE("TransferOutput") << " TransferOutput::writeDataProducts(...): "
				"Streaming product provenance of class: '"
				+ bd.producedClassName()
				+ "' modlbl: '"
				+ bd.moduleLabel()
				+ "' instnm: '"
				+ bd.productInstanceName()
				+ "' procnm: '"
				+ bd.processName()
				+ "'" << TLOG_ENDL;
			const ProductProvenance* prdprov =
				I->second->productProvenancePtr().get();
			msg.WriteObjectAny(prdprov, prdprov_class);
		}
		}
	TLOG_TRACE("TransferOutput") << " End:   TransferOutput::writeDataProducts(...)" << TLOG_ENDL;
	}

void
art::TransferOutput::
write(EventPrincipal& ep)
{
	//
	//  Write an Event message.
	//
	TLOG_TRACE("TransferOutput") << " Begin: TransferOutput::"
		"write(const EventPrincipal& ep)" << TLOG_ENDL;
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
			"TransferOutput::write(const EventPrincipal& ep): "
			"Could not get TClass for art::RunAuxiliary!";
	}
	static TClass* subrun_aux_class = TClass::GetClass("art::SubRunAuxiliary");
	if (subrun_aux_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) <<
			"TransferOutput::write(const EventPrincipal& ep): "
			"Could not get TClass for art::SubRunAuxiliary!";
	}
	static TClass* event_aux_class = TClass::GetClass("art::EventAuxiliary");
	if (event_aux_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) <<
			"TransferOutput::write(const EventPrincipal& ep): "
			"Could not get TClass for art::EventAuxiliary!";
	}
	static TClass* history_class = TClass::GetClass("art::History");
	if (history_class == nullptr)
	{
		throw art::Exception(art::errors::DictionaryNotFound) <<
			"TransferOutput::write(const EventPrincipal& ep): "
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
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Streaming message type code ..." << TLOG_ENDL;
		msg.WriteULong(4);
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Finished streaming message type code." << TLOG_ENDL;
	}
	//
	//  Write RunAuxiliary.
	//
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Streaming RunAuxiliary ..." << TLOG_ENDL;
		msg.WriteObjectAny(&ep.subRunPrincipal().runPrincipal().aux(),
			run_aux_class);
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Finished streaming RunAuxiliary." << TLOG_ENDL;
	}
	//
	//  Write SubRunAuxiliary.
	//
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Streaming SubRunAuxiliary ..." << TLOG_ENDL;
		msg.WriteObjectAny(&ep.subRunPrincipal().aux(),
			subrun_aux_class);
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Finished streaming SubRunAuxiliary." << TLOG_ENDL;
	}
	//
	//  Write EventAuxiliary.
	//
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Streaming EventAuxiliary ..." << TLOG_ENDL;
		msg.WriteObjectAny(&ep.aux(), event_aux_class);
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Finished streaming EventAuxiliary." << TLOG_ENDL;
	}
	//
	//  Write History.
	//
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Streaming History ..." << TLOG_ENDL;
		msg.WriteObjectAny(&ep.history(), history_class);
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Finished streaming History." << TLOG_ENDL;
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
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Sending a message ..." << TLOG_ENDL;
		sendMessage_(ep.id().event(), artdaq::Fragment::DataFragmentType, msg);
		TLOG_TRACE("TransferOutput") << " TransferOutput::write(const EventPrincipal& ep): "
			"Message sent." << TLOG_ENDL;
	}
	//
	//  Delete the branch keys we created for the message.
	//
	for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
	{
		delete *I;
		*I = 0;
	}
	TLOG_TRACE("TransferOutput") << " End:   TransferOutput::write(const EventPrincipal& ep)" << TLOG_ENDL;
}

void
art::TransferOutput::
writeRun(RunPrincipal& rp)
{
	//
	//  Write an EndRun message.
	//
	TLOG_TRACE("TransferOutput") << " Begin: TransferOutput::writeRun(const RunPrincipal& rp)" << TLOG_ENDL;
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
		TLOG_TRACE("TransferOutput") << " writeRun: streaming message type code ...");
		msg.WriteULong(2);
		TLOG_TRACE("TransferOutput") << " writeRun: finished streaming message type code.");
	}
	//
	//  Write RunAuxiliary.
	//
	{
		TLOG_TRACE("TransferOutput") << " writeRun: streaming RunAuxiliary ...");
		if (art::debugit() >= 1)
		{
			TLOG_TRACE("TransferOutput") << " writeRun: dumping ProcessHistoryRegistry ...");
			//typedef std::map<const ProcessHistoryID,ProcessHistory>
			//    ProcessHistoryMap;
			art::ProcessHistoryMap const& phr =
				art::ProcessHistoryRegistry::get();
			TLOG_TRACE("TransferOutput") << " writeRun: phr: size: " << phr.size() << '\n';
			for (auto I = phr.begin(), E = phr.end(); I != E; ++I)
			{
				std::ostringstream OS;
				I->first.print(OS);
				TLOG_TRACE("TransferOutput") << " writeRun: phr: id: '" << OS.str() << "'");
				OS.str("");
				TLOG_TRACE("TransferOutput") << " writeRun: phr: data.size(): "
					<< I->second.data().size() << '\n';
				if (I->second.data().size())
				{
					I->second.data().back().id().print(OS);
					TLOG_TRACE("TransferOutput") << " writeRun: phr: data.back().id(): '"
						<< OS.str() << "'");
				}
			}
			if (!rp.aux().processHistoryID().isValid())
			{
				TLOG_TRACE("TransferOutput") << " writeRun: ProcessHistoryID: 'INVALID'");
			}
			else
			{
				std::ostringstream OS;
				rp.aux().processHistoryID().print(OS);
				TLOG_TRACE("TransferOutput") << " writeRun: ProcessHistoryID: '"
					<< OS.str() << "'");
					OS.str("");
					const ProcessHistory& processHistory =
						ProcessHistoryRegistry::get(rp.aux().processHistoryID());
					if (processHistory.data().size())
					{
						// FIXME: Print something special on invalid id() here!
						processHistory.data().back().id().print(OS);
						TLOG_TRACE("TransferOutput") << " writeRun: ProcessConfigurationID: '"
							<< OS.str() << "'");
							OS.str("");
							TLOG_TRACE("TransferOutput") << " writeRun: ProcessConfiguration: '"
								<< processHistory.data().back() << '\n';
					}
			}
		}
		msg.WriteObjectAny(&rp.aux(), run_aux_class);
		TLOG_TRACE("TransferOutput") << " writeRun: streamed RunAuxiliary.");
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
		TLOG_TRACE("TransferOutput") << " writeRun: sending a message ...");
		sendMessage_(0, artdaq::Fragment::EndOfRunFragmentType, msg);
		TLOG_TRACE("TransferOutput") << " writeRun: message sent.");
	}
	//
	//  Delete the branch keys we created for the message.
	//
	for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
	{
		delete *I;
		*I = 0;
	}
#endif // 0
	TLOG_TRACE("TransferOutput") << " End:   TransferOutput::writeRun(const RunPrincipal& rp)" << TLOG_ENDL;
}

void
art::TransferOutput::writeSubRun(SubRunPrincipal& srp)
{
	//
	//  Write an EndSubRun message.
	//
	TLOG_TRACE("TransferOutput") << " Begin: TransferOutput::"
		"writeSubRun(const SubRunPrincipal& srp)" << TLOG_ENDL;
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
			"TransferOutput::writeSubRun: "
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
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
			"streaming message type code ..." << TLOG_ENDL;
		msg.WriteULong(3);
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
			"finished streaming message type code." << TLOG_ENDL;
	}
	//
	//  Write SubRunAuxiliary.
	//
	{
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
			"streaming SubRunAuxiliary ..." << TLOG_ENDL;
		if (art::debugit() >= 1)
		{
			TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
				"dumping ProcessHistoryRegistry ..." << TLOG_ENDL;
			//typedef std::map<const ProcessHistoryID,ProcessHistory>
			//    ProcessHistoryMap;
#          if ART_HEX_VERSION >= 0x20703
			for (auto I = std::begin(art::ProcessHistoryRegistry::get())
				, E = std::end(art::ProcessHistoryRegistry::get()); I != E; ++I)
#          else
			art::ProcessHistoryMap const& phr =
				art::ProcessHistoryRegistry::get();
			TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
				"phr: size: " << std::to_string(phr.size()) << TLOG_ENDL;
			for (auto I = phr.begin(), E = phr.end(); I != E; ++I)
#          endif
			{
				std::ostringstream OS;
					I->first.print(OS);
				TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
					"phr: id: '" + OS.str() + "'" << TLOG_ENDL;
				OS.str("");
				TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
					"phr: data.size(): " << std::to_string(I->second.data().size()) << TLOG_ENDL;
				if (I->second.data().size())
				{
					I->second.data().back().id().print(OS);
					TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
						"phr: data.back().id(): '"
						+ OS.str() + "'" << TLOG_ENDL;
				}
			}
			if (!srp.aux().processHistoryID().isValid())
			{
				TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
					"ProcessHistoryID: 'INVALID'" << TLOG_ENDL;;
			}
			else
			{
				std::ostringstream OS;
				srp.aux().processHistoryID().print(OS);
				TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: ProcessHistoryID: '"
					+ OS.str() + "'" << TLOG_ENDL;
				OS.str("");
#              if ART_HEX_VERSION >= 0x20703
				ProcessHistory processHistory;
				ProcessHistoryRegistry::get(srp.aux().processHistoryID(), processHistory);
#              else
				const ProcessHistory& processHistory =
					ProcessHistoryRegistry::get(srp.aux().processHistoryID());
#              endif
				if (processHistory.data().size())
				{
					// FIXME: Print something special on invalid id() here!
					processHistory.data().back().id().print(OS);
					TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
						"ProcessConfigurationID: '"
						+ OS.str() + "'" << TLOG_ENDL;
					OS.str("");
					OS << processHistory.data().back();
					TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
						"ProcessConfiguration: '"
						+ OS.str() << TLOG_ENDL;
				}
			}
		}
		msg.WriteObjectAny(&srp.aux(), subrun_aux_class);
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: streamed SubRunAuxiliary." << TLOG_ENDL;
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
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
			"Sending the EndOfSubrun message." << TLOG_ENDL;
		sendMessage_(0, artdaq::Fragment::EndOfSubrunFragmentType, msg);
		TLOG_TRACE("TransferOutput") << " TransferOutput::writeSubRun: "
			"EndOfSubrun message(s) sent." << TLOG_ENDL;

	}
	//
	//  Delete the branch keys we created for the message.
	//
	for (auto I = bkv.begin(), E = bkv.end(); I != E; ++I)
	{
		delete *I;
		*I = 0;
	}
	TLOG_TRACE("TransferOutput") << " End:   TransferOutput::writeSubRun(const SubRunPrincipal& srp)" << TLOG_ENDL;
}


void
art::TransferOutput::sendMessage_(uint64_t sequenceId, uint8_t messageType, TBufferFile& msg)
{
	TLOG_DEBUG("TransferOutput") << "Sending message with sequenceID=" << std::to_string(sequenceId) << ", type=" << std::to_string(messageType) << ", length=" << std::to_string(msg.Length()) << TLOG_ENDL;
	artdaq::NetMonHeader header;
	header.data_length = static_cast<uint64_t>(msg.Length());
	artdaq::Fragment
		fragment(std::ceil(msg.Length() /
			static_cast<double>(sizeof(artdaq::RawDataType))),
			sequenceId, 0, messageType, header);

	memcpy(&*fragment.dataBegin(), msg.Buffer(), msg.Length());
	auto sts = artdaq::TransferInterface::CopyStatus::kErrorNotRequiringException;
	size_t retries = 0;
	while (sts != artdaq::TransferInterface::CopyStatus::kSuccess && retries <= send_retry_count_)
	{
		sts = transfer_->copyFragment(fragment, send_timeout_us_);
		retries++;
	}

#if 0
	if (messageType == artdaq::Fragment::InitFragmentType)
	{
		std::fstream ostream("sendInitMessage_TransferOutput.bin", std::ios::out | std::ios::binary);
		ostream.write(msg.Buffer(), msg.Length());
		ostream.close();
}
#endif

	}

DEFINE_ART_MODULE(art::TransferOutput)
