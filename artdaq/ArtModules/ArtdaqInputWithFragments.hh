#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/Source.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"

#if ART_HEX_VERSION < 0x30000
#include "art/Persistency/Provenance/MasterProductRegistry.h"
#include "art/Persistency/Provenance/ProductMetaData.h"
#endif
#if ART_HEX_VERSION >= 0x30200
#include "art_root_io/setup.h"
#endif
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"

#include "canvas/Persistency/Common/EDProduct.h"
#include "canvas/Persistency/Common/Wrapper.h"
#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/BranchKey.h"
#include "canvas/Persistency/Provenance/History.h"
#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ProcessConfiguration.h"
#include "canvas/Persistency/Provenance/ProcessHistory.h"
#include "canvas/Persistency/Provenance/ProcessHistoryID.h"
#include "canvas/Persistency/Provenance/ProductList.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"
#include "canvas/Persistency/Provenance/ProductTables.h"
#include "canvas/Utilities/DebugMacros.h"

#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"
#include "fhiclcpp/make_ParameterSet.h"

#include <TBufferFile.h>
#include <TClass.h>
#include <TList.h>
#include <TStreamerInfo.h>

#include "artdaq-core/Data/detail/ParentageMap.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"
#include "artdaq/ArtModules/InputUtilities.hh"
#include "artdaq/ArtModules/detail/SharedMemoryReader.hh"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"

#include <sys/time.h>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#if ART_HEX_VERSION < 0x30000
#define EVENT_ID id
#define SUBRUN_ID id
#define RUN_ID id
#define HISTORY_PTR_T std::shared_ptr<art::History>
#else
#define EVENT_ID eventID
#define SUBRUN_ID subRunID
#define RUN_ID runID
#define HISTORY_PTR_T std::unique_ptr<art::History>
#endif

namespace art {
template<typename U>
class ArtdaqInputWithFragments;
}

/**
 * \brief This template class provides a unified interface for reading data into art
 * \tparam U The class responsible for delivering data
 *
 * JCF, May-27-2016
 * ArtdaqInputWithFragments is a template class which takes, as a parameter, a
 * class which it uses to receive data; the instance of this class is
 * called "communicationWrapper_". As of this writing, this wrapper
 * class is implemented by NetMonWrapper (for reading data into the
 * aggregator from the eventbuilder) and TransferWrapper (for reading
 * data into an art process). This class presents a unified approach
 * to handling art provenance, regardless of the communication
 * protocol used to read data in.
 */
template<typename U>
class art::ArtdaqInputWithFragments
{
public:
	/**
   * \brief Copy Constructor is deleted
   */
	ArtdaqInputWithFragments(const ArtdaqInputWithFragments&) = delete;

	/**
   * \brief Copy Assignment operator is deleted
   * \return ArtdaqInputWithFragments copy
   */
	ArtdaqInputWithFragments& operator=(const ArtdaqInputWithFragments&) = delete;

	/**
   * \brief ArtdaqInputWithFragments Destructor
   */
	~ArtdaqInputWithFragments();

	/**
   * \brief ArtdaqInputWithFragments Constructor
   * \param ps ParameterSet used to confiugre communication wrapper class
   * \param helper An art::ProductRegistryHelper for registering products
   * \param pm An art::SourceHelper for handling provenance
   */
	ArtdaqInputWithFragments(const fhicl::ParameterSet& ps, art::ProductRegistryHelper& helper, art::SourceHelper const& pm);

	/**
   * \brief Called by art to close the input source. No-Op
   */
	void closeCurrentFile();

	/**
   * \brief Emulate reading a file
   * \param fb Output art::FileBlock object
   */
	void readFile(const std::string&, art::FileBlock*& fb);

	/**
   * \brief Whether additional events are expected from the source
   * \return True if ArtdaqInputWithFragments has not been shut down
   */
	bool hasMoreData() const;

	/**
   * \brief Read the next event from the communication wrapper
   * \param inR RunPrincipal input pointer
   * \param inSR SubRunPrincipal input pointer
   * \param outR RunPrincipal output pointer
   * \param outSR SubRunPrincipal output pointer
   * \param outE EventPrincipal output pointer
   * \return Whether an event was successfully read from the communication wrapper
   */
	bool readNext(art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR, art::RunPrincipal*& outR,
	              art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE);

private:
	void readAndConstructPrincipal(std::unique_ptr<TBufferFile>&, unsigned long, art::RunPrincipal* const,
	                               art::SubRunPrincipal* const, art::RunPrincipal*&, art::SubRunPrincipal*&,
	                               art::EventPrincipal*&);

	template<class T>
	void readDataProducts(std::unique_ptr<TBufferFile>&, T*&);

	void putInPrincipal(RunPrincipal*&, std::unique_ptr<EDProduct>&&, const BranchDescription&,
	                    std::unique_ptr<const ProductProvenance>&&);

	void putInPrincipal(SubRunPrincipal*&, std::unique_ptr<EDProduct>&&, const BranchDescription&,
	                    std::unique_ptr<const ProductProvenance>&&);

	void putInPrincipal(EventPrincipal*&, std::unique_ptr<EDProduct>&&, const BranchDescription&,
	                    std::unique_ptr<const ProductProvenance>&&);

private:
	bool shutdownMsgReceived_;
	bool outputFileCloseNeeded_;
	art::SourceHelper const& pm_;
	U communicationWrapper_;
	ProductList* productList_;
	HISTORY_PTR_T history_to_use_;
	std::string pretend_module_name;                       ///< The module name to store data under
	std::string unidentified_instance_name;                ///< The name to use for unknown Fragment typesstd::chrono::steady_clock::time_point last_read_time;  ///< Time last read was completed
	size_t bytesRead;                                      ///< running total of number of bytes received
	std::chrono::steady_clock::time_point last_read_time;  ///< Time last read was completed
};

template<typename U>
art::ArtdaqInputWithFragments<U>::ArtdaqInputWithFragments(const fhicl::ParameterSet& ps, art::ProductRegistryHelper& helper,
                                                           art::SourceHelper const& pm)
    : shutdownMsgReceived_(false)
    , outputFileCloseNeeded_(false)
    , pm_(pm)
    , communicationWrapper_(ps)
    , productList_()
    , pretend_module_name(ps.get<std::string>("raw_data_label", "daq"))
    , unidentified_instance_name("unidentified")
    , bytesRead(0)
    , last_read_time(std::chrono::steady_clock::now())
{
	artdaq::configureMessageFacility("artdaqart");

#if ART_HEX_VERSION >= 0x30200
	root::setup();
#endif

#if 0
	volatile bool loop = true;
	while (loop)
	{
		usleep(1000);
	}
#endif

	// JCF, May-27-2016

	// Something will have to be done about the labeling of this class,
	// since it's just a template class- the user will care about the
	// specific instantiation when it comes to messages

	TLOG_ARB(5, "ArtdaqInputWithFragments") << "Begin: ArtdaqInputWithFragments::ArtdaqInputWithFragments("
	                                        << "const fhicl::ParameterSet& ps, "
	                                        << "art::ProductRegistryHelper& helper, "
	                                        << "const art::SourceHelper& pm)";

	TLOG_ARB(5, "ArtdaqInputWithFragments") << "Going to receive init message";
	artdaq::FragmentPtr initFrag = communicationWrapper_.receiveInitMessage();
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "Init message received";

	if (!initFrag)
	{
		throw art::Exception(art::errors::DataCorruption) << "ArtdaqInputWithFragments: Could not receive init message!";
	}
	auto header = initFrag->metadata<artdaq::NetMonHeader>();
	std::unique_ptr<TBufferFile> msg(new TBufferFile(TBuffer::kRead, header->data_length, initFrag->dataBegin(), kFALSE, 0));

	// This first unsigned long is the message type code, ignored here in the constructor
	unsigned long dummy = 0;
	msg->ReadULong(dummy);

	// ELF: 6/11/2019: This code is taken from TSocket::RecvStreamerInfos
	TList* list = (TList*)msg->ReadObject(TList::Class());

	TIter next(list);
	TStreamerInfo* info;
	TObjLink* lnk = list->FirstLink();
	// First call BuildCheck for regular class
	while (lnk)
	{
		info = (TStreamerInfo*)lnk->GetObject();
		TObject* element = info->GetElements()->UncheckedAt(0);
		Bool_t isstl = element && strcmp("This", element->GetName()) == 0;
		if (!isstl)
		{
			info->BuildCheck();
			TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: importing TStreamerInfo: " << info->GetName() << ", version = " << info->GetClassVersion();
		}
		lnk = lnk->Next();
	}
	// Then call BuildCheck for stl class
	lnk = list->FirstLink();
	while (lnk)
	{
		info = (TStreamerInfo*)lnk->GetObject();
		TObject* element = info->GetElements()->UncheckedAt(0);
		Bool_t isstl = element && strcmp("This", element->GetName()) == 0;
		if (isstl)
		{
			info->BuildCheck();
			TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: importing TStreamerInfo: " << info->GetName() << ", version = " << info->GetClassVersion();
		}
		lnk = lnk->Next();
	}
	// ELF: 6/11/2019: End TSocket snippet

	//
	//  Read the ParameterSetRegistry.
	//
	unsigned long ps_cnt = 0;
	msg->ReadULong(ps_cnt);
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: parameter set count: " << ps_cnt;
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: reading parameter sets ...";
	for (unsigned long I = 0; I < ps_cnt; ++I)
	{
		std::string pset_str = "";  // = ReadObjectAny<std::string>(msg, "std::string", "ArtdaqInputWithFragments::ArtdaqInputWithFragments");
		msg->ReadStdString(pset_str);

		TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: parameter set: " << pset_str;

		fhicl::ParameterSet pset;
		fhicl::make_ParameterSet(pset_str, pset);
		// Force id calculation.
		pset.id();
		fhicl::ParameterSetRegistry::put(pset);
	}
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: finished reading parameter sets.";

	//
	//  Read the MasterProductRegistry.
	//
	productList_ = ReadObjectAny<art::ProductList>(
	    msg, "std::map<art::BranchKey,art::BranchDescription>", "ArtdaqInputWithFragments::ArtdaqInputWithFragments");
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: Product list sz=" << productList_->size();

#ifndef __OPTIMIZE__
	for (auto I = productList_->begin(), E = productList_->end(); I != E; ++I)
	{
		TLOG_ARB(50, "ArtdaqInputWithFragments") << "Branch key: class: '" << I->first.friendlyClassName_ << "' modlbl: '"
		                                         << I->first.moduleLabel_ << "' instnm: '" << I->first.productInstanceName_ << "' procnm: '"
		                                         << I->first.processName_ << "', branch description name: " << I->second.wrappedName()
		                                         << ", TClass = " << (void*)TClass::GetClass(I->second.wrappedName().c_str());
	}
#endif

	// helper now owns productList_!
#if ART_HEX_VERSION < 0x30000
	helper.productList(productList_);
#else
	helper.productList(std::unique_ptr<art::ProductList>(productList_));
#endif
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: got product list";

	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: Reading ProcessHistory";
	art::ProcessHistoryMap* phm = ReadObjectAny<art::ProcessHistoryMap>(
	    msg, "std::map<const art::Hash<2>,art::ProcessHistory>", "ArtdaqInputWithFragments::ArtdaqInputWithFragments");
	printProcessMap(*phm, "ArtdaqInputWithFragments's ProcessHistoryMap");

	ProcessHistoryRegistry::put(*phm);
	printProcessMap(ProcessHistoryRegistry::get(), "ArtdaqInputWithFragments's ProcessHistoryRegistry");

	//
	//  Read the ParentageRegistry.
	//
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: Reading ParentageMap";
	ParentageMap* parentageMap = ReadObjectAny<ParentageMap>(msg, "art::ParentageMap", "ArtdaqInputWithFragments::ArtdaqInputWithFragments");
	ParentageRegistry::put(*parentageMap);

	//
	// Read the History
	//
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: Reading History";
	history_to_use_.reset(ReadObjectAny<History>(msg, "art::History", "ArtdaqInputWithFragments::ArtdaqInputWithFragments"));
	if (!history_to_use_->processHistoryID().isValid())
	{
		TLOG_ARB(5, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments: History from init message is INVALID!";
	}

	helper.reconstitutes<artdaq::Fragments, art::InEvent>(pretend_module_name, unidentified_instance_name);

	// Workaround for #22979
	helper.reconstitutes<artdaq::Fragments, art::InRun>(pretend_module_name, unidentified_instance_name);
	helper.reconstitutes<artdaq::Fragments, art::InSubRun>(pretend_module_name, unidentified_instance_name);

	art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> translator;

	std::set<std::string> instance_names = translator->GetAllProductInstanceNames();
	for (const auto& set_iter : instance_names)
	{
		helper.reconstitutes<artdaq::Fragments, art::InEvent>(pretend_module_name, set_iter);
	}

	//
	//  Finished with init message.
	//
	TLOG_ARB(5, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::ArtdaqInputWithFragments("
	                                        << "const fhicl::ParameterSet& ps, "
	                                        << "art::ProductRegistryHelper& helper, "
	                                        << "const art::SourceHelper& pm)";
}

template<typename U>
art::ArtdaqInputWithFragments<U>::~ArtdaqInputWithFragments()
{}

template<typename U>
void art::ArtdaqInputWithFragments<U>::closeCurrentFile()
{
	TLOG_ARB(6, "ArtdaqInputWithFragments") << "Begin/End: ArtdaqInputWithFragments::closeCurrentFile()";
}

template<typename U>
void art::ArtdaqInputWithFragments<U>::readFile(const std::string&, art::FileBlock*& fb)
{
	TLOG_ARB(7, "ArtdaqInputWithFragments") << "Begin: ArtdaqInputWithFragments::"
	                                           "readFile(const std::string& name, art::FileBlock*& fb)";
	fb = new art::FileBlock(art::FileFormatVersion(1, "ArtdaqInputWithFragments2013"), "nothing");
	TLOG_ARB(7, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::"
	                                           "readFile(const std::string& name, art::FileBlock*& fb)";
}

template<typename U>
bool art::ArtdaqInputWithFragments<U>::hasMoreData() const
{
	TLOG_ARB(8, "ArtdaqInputWithFragments") << "Begin: ArtdaqInputWithFragments::hasMoreData()";
	if (shutdownMsgReceived_)
	{
		TLOG_ARB(8, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::hasMoreData(): "
		                                           "returning false on shutdownMsgReceived_.";
		TLOG_ARB(8, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::hasMoreData()";
		return false;
	}
	TLOG_ARB(8, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::hasMoreData(): "
	                                           "returning true on not shutdownMsgReceived_.";
	TLOG_ARB(8, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::hasMoreData()";
	return true;
}

template<typename U>
void art::ArtdaqInputWithFragments<U>::readAndConstructPrincipal(std::unique_ptr<TBufferFile>& msg, unsigned long msg_type_code,
                                                                 art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR,
                                                                 art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR,
                                                                 art::EventPrincipal*& outE)
{
	//
	//  Process the message.
	//
	std::unique_ptr<art::RunAuxiliary> run_aux;
	std::unique_ptr<art::SubRunAuxiliary> subrun_aux;
	std::unique_ptr<art::EventAuxiliary> event_aux;

	HISTORY_PTR_T history_from_event;

	// Establish default 'results'
	outR = 0;
	outSR = 0;
	outE = 0;

	if (msg_type_code == 2)
	{  // EndRun message.

		TLOG_ARB(9, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                        << "processing EndRun message ...";

		run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary", "ArtdaqInputWithFragments::readAndConstructPrincipal"));
		printProcessHistoryID("readAndConstructPrincipal", run_aux.get());

		TLOG_ARB(9, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                        << "making flush RunPrincipal ...";
		outR = pm_.makeRunPrincipal(RunID::flushRun(), run_aux->beginTime());

		TLOG_ARB(9, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                        << "making flush SubRunPrincipal ...";
		outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(), run_aux->beginTime());

		TLOG_ARB(9, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                        << "making flush EventPrincipal ...";
		outE = pm_.makeEventPrincipal(EventID::flushEvent(), run_aux->endTime(), true, EventAuxiliary::Any);

		TLOG_ARB(9, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                        << "finished processing EndRun message.";
	}
	else if (msg_type_code == 3)
	{  // EndSubRun message.

		TLOG_ARB(10, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "processing EndSubRun message ...";

		subrun_aux.reset(
		    ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary", "ArtdaqInputWithFragments::readAndConstructPrincipal"));
		printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());

		TLOG_ARB(10, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "making flush RunPrincipal ...";
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
		if (inR != nullptr)
		{
#if ART_HEX_VERSION < 0x30000
			inR->setEndTime(currentTime);
#else
			inR->endTime(currentTime);
#endif
		}
		if (inSR != nullptr)
		{
#if ART_HEX_VERSION < 0x30000
			inSR->setEndTime(currentTime);
#else
			inSR->endTime(currentTime);
#endif
		}

		TLOG_ARB(10, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "making flush SubRunPrincipal ...";
		outSR = pm_.makeSubRunPrincipal(SubRunID::flushSubRun(), subrun_aux->beginTime());

		TLOG_ARB(10, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "making flush EventPrincipal ...";
		outE = pm_.makeEventPrincipal(EventID::flushEvent(), subrun_aux->endTime(), true, EventAuxiliary::Any);

		TLOG_ARB(10, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "finished processing EndSubRun message.";
	}
	else if (msg_type_code == 4)
	{  // Event message.

		TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "processing Event message ...";

		run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary", "ArtdaqInputWithFragments::readAndConstructPrincipal"));
		printProcessHistoryID("readAndConstructPrincipal", run_aux.get());

		subrun_aux.reset(
		    ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary", "ArtdaqInputWithFragments::readAndConstructPrincipal"));
		printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());

		event_aux.reset(
		    ReadObjectAny<art::EventAuxiliary>(msg, "art::EventAuxiliary", "ArtdaqInputWithFragments::readAndConstructPrincipal"));

		history_from_event.reset(ReadObjectAny<art::History>(msg, "art::History", "ArtdaqInputWithFragments::readAndConstructPrincipal"));
		printProcessHistoryID("readAndConstructPrincipal", history_from_event.get());

		// Every event should have a valid history
		if (!history_from_event->processHistoryID().isValid())
		{
			throw art::Exception(art::errors::Unknown)
			    << "readAndConstructPrincipal: processHistoryID of history in Event message is invalid!";
		}
		// If our stored history is invalid, use this Event's history
		else if (!history_to_use_->processHistoryID().isValid())
		{
			history_to_use_.swap(history_from_event);
		}
		// If our stored history doesn't match our ProcessHistoryRegistry, then used this Event's history
		else if (!art::ProcessHistoryRegistry::get().count(history_to_use_->processHistoryID()) &&
		         art::ProcessHistoryRegistry::get().count(history_from_event->processHistoryID()))
		{
			history_to_use_.swap(history_from_event);
		}

		TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "inR: " << (void*)inR << " run/expected "
		                                         << (inR ? std::to_string(inR->run()) : "invalid") << "/" << event_aux->run();
		TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "inSR: " << (void*)inSR << " run/expected "
		                                         << (inSR ? std::to_string(inSR->run()) : "invalid") << "/" << event_aux->run()
		                                         << ", subrun/expected " << (inSR ? std::to_string(inSR->subRun()) : "invalid") << "/"
		                                         << event_aux->subRun();
		if ((inR == nullptr) || !inR->RUN_ID().isValid() || (inR->run() != event_aux->run()))
		{
			// New run, either we have no input RunPrincipal, or the
			// input run number does not match the event run number.
			TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: making RunPrincipal ...";
			outR = pm_.makeRunPrincipal(*run_aux.get());
		}
		art::SubRunID subrun_check(event_aux->run(), event_aux->subRun());
		if (inSR == 0 || subrun_check != inSR->SUBRUN_ID())
		{
			// New SubRun, either we have no input SubRunPrincipal, or the
			// input subRun number does not match the event subRun number.
			TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
			                                         << "making SubRunPrincipal ...";
			outSR = pm_.makeSubRunPrincipal(*subrun_aux.get());
		}
		TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: making EventPrincipal ...";
		auto historyPtr = HISTORY_PTR_T(new History(*(history_to_use_.get())));
		if (!art::ProcessHistoryRegistry::get().count(history_to_use_->processHistoryID()))
		{
			TLOG_WARNING("ArtdaqInputWithFragments") << "Stored history is not in ProcessHistoryRegistry, this event may have issues!";
		}
		outE = pm_.makeEventPrincipal(*event_aux.get(), std::move(historyPtr));

		TLOG_ARB(11, "ArtdaqInputWithFragments") << "readAndConstructPrincipal: "
		                                         << "finished processing Event message.";
	}
}

template<typename U>
template<class T>
void art::ArtdaqInputWithFragments<U>::readDataProducts(std::unique_ptr<TBufferFile>& msg, T*& outPrincipal)
{
	unsigned long prd_cnt = 0;
	{
		TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: reading data product count ...";
		msg->ReadULong(prd_cnt);
		TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: product count: " << prd_cnt;
	}
	//
	//  Read the data products.
	//
	for (unsigned long I = 0; I < prd_cnt; ++I)
	{
		std::unique_ptr<BranchKey> bk;
		{
			TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: Reading branch key.";
			bk.reset(ReadObjectAny<BranchKey>(msg, "art::BranchKey", "ArtdaqInputWithFragments::readDataProducts"));
		}

#ifndef __OPTIMIZE__
		TLOG_ARB(13, "ArtdaqInputWithFragments") << "readDataProducts: got product class: '" << bk->friendlyClassName_ << "' modlbl: '"
		                                         << bk->moduleLabel_ << "' instnm: '" << bk->productInstanceName_ << "' procnm: '"
		                                         << bk->processName_;
#endif
		ProductList::const_iterator iter;
		{
			TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: looking up product ...";
			iter = productList_->find(*bk);
			if (iter == productList_->end())
			{
				throw art::Exception(art::errors::ProductNotFound)
				    << "No product is registered for\n"
				    << "  process name:                '" << bk->processName_ << "'\n"
				    << "  module label:                '" << bk->moduleLabel_ << "'\n"
				    << "  product friendly class name: '" << bk->friendlyClassName_ << "'\n"
				    << "  product instance name:       '" << bk->productInstanceName_ << "'\n";
			}
		}
		// Note: This must be a reference to the unique copy in
		//       the master product registry!
		const BranchDescription& bd = iter->second;
		std::unique_ptr<EDProduct> prd;
		{
			TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: Reading product with wrapped name: " << bd.wrappedName()
			                                         << ", TClass = " << (void*)TClass::GetClass(bd.wrappedName().c_str());

			// JCF, May-25-2016
			// Currently unclear why the templatized version of ReadObjectAny doesn't work here...

			//	    prd.reset(ReadObjectAny<EDProduct>(msg, bd.wrappedName()));

			void* p = msg->ReadObjectAny(TClass::GetClass(bd.wrappedName().c_str()));
			auto pp = reinterpret_cast<EDProduct*>(p);

			TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: After ReadObjectAny(prd): p=" << p << ", EDProduct::isPresent: " << pp->isPresent();
			prd.reset(pp);
			p = nullptr;
		}
		std::unique_ptr<const ProductProvenance> prdprov;
		{
			TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: Reading product provenance.";
			prdprov.reset(ReadObjectAny<ProductProvenance>(msg, "art::ProductProvenance", "ArtdaqInputWithFragments::readDataProducts"));
		}

		{
			TLOG_ARB(12, "ArtdaqInputWithFragments") << "readDataProducts: inserting product: class: '" << bd.friendlyClassName()
			                                         << "' modlbl: '" << bd.moduleLabel() << "' instnm: '" << bd.productInstanceName()
			                                         << "' procnm: '" << bd.processName() << "' id: '" << bd.productID() << "'";
			putInPrincipal(outPrincipal, std::move(prd), bd, std::move(prdprov));
		}
	}
}

template<typename U>
void art::ArtdaqInputWithFragments<U>::putInPrincipal(RunPrincipal*& rp, std::unique_ptr<EDProduct>&& prd,
                                                      const BranchDescription& bd,
                                                      std::unique_ptr<const ProductProvenance>&& prdprov)
{
#if ART_HEX_VERSION < 0x30000
	rp->put(std::move(prd), bd, std::move(prdprov), RangeSet::forRun(rp->RUN_ID()));
#else
	rp->put(bd, std::move(prdprov), std::move(prd), std::make_unique<RangeSet>(RangeSet::forRun(rp->RUN_ID())));
#endif
}

template<typename U>
void art::ArtdaqInputWithFragments<U>::putInPrincipal(SubRunPrincipal*& srp, std::unique_ptr<EDProduct>&& prd,
                                                      const BranchDescription& bd,
                                                      std::unique_ptr<const ProductProvenance>&& prdprov)
{
#if ART_HEX_VERSION < 0x30000
	srp->put(std::move(prd), bd, std::move(prdprov), RangeSet::forSubRun(srp->SUBRUN_ID()));
#else
	srp->put(bd, std::move(prdprov), std::move(prd), std::make_unique<RangeSet>(RangeSet::forSubRun(srp->SUBRUN_ID())));
#endif
}

template<typename U>
void art::ArtdaqInputWithFragments<U>::putInPrincipal(EventPrincipal*& ep, std::unique_ptr<EDProduct>&& prd,
                                                      const BranchDescription& bd,
                                                      std::unique_ptr<const ProductProvenance>&& prdprov)
{
	TLOG_ARB(14, "ArtdaqInputWithFragments") << "EventPrincipal size before put: " << ep->size();
#if ART_HEX_VERSION < 0x30000
	ep->put(std::move(prd), bd, std::move(prdprov));
#else
	ep->put(bd, std::move(prdprov), std::move(prd), std::make_unique<RangeSet>(RangeSet::invalid()));
#endif
	TLOG_ARB(14, "ArtdaqInputWithFragments") << "EventPrincipal size after put: " << ep->size();
}

template<typename U>
bool art::ArtdaqInputWithFragments<U>::readNext(art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR,
                                                art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE)
{
	TLOG_ARB(15, "ArtdaqInputWithFragments") << "Begin: ArtdaqInputWithFragments::readNext";
	if (outputFileCloseNeeded_)
	{
		outputFileCloseNeeded_ = false;
		// Signal that we need the output file closed by returning false,
		// but answering true to the hasMoreData() query.
		TLOG_ARB(15, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::readNext: "
		                                         << "returning false on outputFileCloseNeeded_";
		TLOG_ARB(15, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::readNext";
		return false;
	}
	auto read_start_time = std::chrono::steady_clock::now();

	std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> eventMap = communicationWrapper_.receiveMessages();
	if (!eventMap.count(artdaq::Fragment::DataFragmentType))
	{
		TLOG_ARB(15, "ArtdaqInput") << "ArtdaqInputWithFragments::readNext got a message without a DataFragment";
		shutdownMsgReceived_ = true;
		return false;
	}
	auto dataFrag = eventMap[artdaq::Fragment::DataFragmentType]->front();
	auto header = dataFrag.metadata<artdaq::NetMonHeader>();
	std::unique_ptr<TBufferFile> msg(new TBufferFile(TBuffer::kRead, header->data_length, dataFrag.dataBegin(), kFALSE, 0));

	auto got_event_time = std::chrono::steady_clock::now();

	//
	//  Read message type code.
	//
	unsigned long msg_type_code = 0;
	{
		TLOG_ARB(15, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::readNext: "
		                                         << "getting message type code ...";
		msg->ReadULong(msg_type_code);
		TLOG_ARB(15, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::readNext: "
		                                         << "message type: " << msg_type_code;
	}
	if (msg_type_code == 5)
	{
		// Shutdown message.
		shutdownMsgReceived_ = true;
		TLOG_ARB(16, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::readNext: "
		                                         << "returning false on Shutdown message.";
		TLOG_ARB(16, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::readNext";
		return false;
	}

	readAndConstructPrincipal(msg, msg_type_code, inR, inSR, outR, outSR, outE);
	//
	//  Read per-event metadata needed to construct principal.
	//
	if (msg_type_code == 2)
	{
		// EndRun message.
		// FIXME: We need to merge these into the input RunPrincipal.
		readDataProducts(msg, outR);
		// Signal that we should close the input and output file.
		TLOG_ARB(17, "ArtdaqInputWithFragments") << "ArtdaqInputWithFragments::readNext: "
		                                         << "returning false on EndRun message.";
		TLOG_ARB(17, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::readNext";
		return false;
	}
	else if (msg_type_code == 3)
	{
		// EndSubRun message.
		// From the code above, EndRun and EndSubRun messages cause
		// the construction of principals that have:
		//    Run:Subrun:Event=flush:flush:flush.
		// This is a problem when you have two neighboring EndSubRuns
		// which are both associated with empty subruns because art will
		// complain that you a new subrun with a subrun number identical
		// to that of the previous subrun.  So the solution is to not
		// return new principals.
		if (inR != 0 && inSR != 0 && outR != 0 && outSR != 0)
		{
			if (inR->RUN_ID().isFlush() && inSR->SUBRUN_ID().isFlush() && outR->RUN_ID().isFlush() &&
			    outSR->SUBRUN_ID().isFlush())
			{
				outR = 0;
				outSR = 0;
				outputFileCloseNeeded_ = true;
				return true;
			}
		}
		// FIXME: We need to merge these into the input SubRunPrincipal.
		readDataProducts(msg, outSR);
		// Remember that we should ask for file close next time
		// we are called.
		outputFileCloseNeeded_ = true;
		TLOG_ARB(18, "ArtdaqInputWithFragments") << "readNext: returning true on EndSubRun message.";
		TLOG_ARB(18, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::readNext";
		return true;
	}
	else if (msg_type_code == 4)
	{
		// Event message.
		readDataProducts(msg, outE);

		// Now read in Fragments
		double fragmentLatency = 0;
		double fragmentLatencyMax = 0.0;
		size_t fragmentCount = 0;

		art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> translator;

		// insert the Fragments of each type into the EventPrincipal
		for (auto& fragmentTypePair : eventMap)
		{
			auto type_code = fragmentTypePair.first;
			if (type_code == artdaq::Fragment::DataFragmentType) continue;
			TLOG_TRACE("ArtdaqInputWithFragments") << "Before GetFragmentsByType call, type is " << (int)type_code;
			TLOG_TRACE("ArtdaqInputWithFragments") << "After GetFragmentsByType call, number of fragments is " << fragmentTypePair.second->size();

			std::unordered_map<std::string, std::unique_ptr<artdaq::Fragments>> derived_fragments;
			for (auto& frag : *fragmentTypePair.second)
			{
				bytesRead += frag.sizeBytes();
				auto latency_s = frag.getLatency(true);
				double latency = latency_s.tv_sec + (latency_s.tv_nsec / 1000000000.0);

				fragmentLatency += latency;
				fragmentCount++;
				if (latency > fragmentLatencyMax) fragmentLatencyMax = latency;

				std::pair<bool, std::string> instance_name_result =
				    translator->GetInstanceNameForFragment(frag, unidentified_instance_name);
				std::string label = instance_name_result.second;
				if (!instance_name_result.first)
				{
					TLOG_WARNING("ArtdaqInputWithFragments")
					    << "UnknownFragmentType: The product instance name mapping for fragment type \"" << ((int)type_code)
					    << "\" is not known. Fragments of this "
					    << "type will be stored in the event with an instance name of \"" << unidentified_instance_name << "\".";
				}
				if (!derived_fragments.count(label))
				{
					derived_fragments[label] = std::make_unique<artdaq::Fragments>();
				}
				derived_fragments[label]->emplace_back(std::move(frag));
			}
			for (auto& type : derived_fragments)
			{
				put_product_in_principal(std::move(type.second),
				                         *outE,
				                         pretend_module_name,
				                         type.first);
			}
		}
		auto read_finish_time = std::chrono::steady_clock::now();
		TLOG_ARB(10, "ArtdaqInputWithFragments") << "readNext: bytesRead=" << bytesRead
		                                         << " metricMan=" << (void*)metricMan.get();
		if (metricMan)
		{
			metricMan->sendMetric("Avg Processing Time", artdaq::TimeUtils::GetElapsedTime(last_read_time, read_start_time),
			                      "s", 2, artdaq::MetricMode::Average);
			metricMan->sendMetric("Avg Input Wait Time", artdaq::TimeUtils::GetElapsedTime(read_start_time, got_event_time),
			                      "s", 3, artdaq::MetricMode::Average);
			metricMan->sendMetric("Avg Read Time", artdaq::TimeUtils::GetElapsedTime(got_event_time, read_finish_time), "s",
			                      3, artdaq::MetricMode::Average);
			metricMan->sendMetric("bytesRead", bytesRead, "B", 3, artdaq::MetricMode::LastPoint);

			metricMan->sendMetric("ArtdaqInputWithFragments Latency", fragmentLatency / fragmentCount, "s", 4, artdaq::MetricMode::Average);
			metricMan->sendMetric("ArtdaqInputWithFragments Maximum Latency", fragmentLatencyMax, "s", 4, artdaq::MetricMode::Maximum);
		}

		TLOG_ARB(19, "ArtdaqInputWithFragments") << "readNext: returning true on Event message.";
		last_read_time = std::chrono::steady_clock::now();
		TLOG_ARB(19, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::readNext";
		return true;
	}
	// Unknown message.
	// FIXME: What do we throw here for invalid message code?
	TLOG_ARB(20, "ArtdaqInputWithFragments") << "readNext: returning false on unknown msg_type_code!";
	TLOG_ARB(20, "ArtdaqInputWithFragments") << "End:   ArtdaqInputWithFragments::readNext";
	return false;
}
