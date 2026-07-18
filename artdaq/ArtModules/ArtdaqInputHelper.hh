#ifndef ARTDAQ_ARTDAQ_ARTMODULES_ARTDAQINPUTHELPER_HH_
#define ARTDAQ_ARTDAQ_ARTMODULES_ARTDAQINPUTHELPER_HH_

#include "TRACE/tracemf.h"  // Pre-empt TRACE/trace.h from Fragment.hh.

#include "artdaq-core/Data/Fragment.hh"
#include "artdaq/ArtModules/InputUtilities.hh"

#include "artdaq-core/Data/MetadataFragment.hh"
#include "artdaq-core/Data/detail/ParentageMap.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "artdaq/ArtModules/ArtdaqFragmentNamingService.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"
#include "artdaq/DAQdata/Globals.hh"
#include "artdaq/DAQdata/NetMonHeader.hh"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/put_product_in_principal.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"

#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art_root_io/setup.h"

#include "canvas/Persistency/Common/EDProduct.h"
#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/BranchKey.h"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#if ART_HEX_VERSION < 0x31100
#include "canvas/Persistency/Provenance/History.h"
#else
#include "canvas/Persistency/Provenance/Compatibility/History.h"
#endif
#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ProcessHistory.h"
#include "canvas/Persistency/Provenance/ProcessHistoryID.h"
#include "canvas/Persistency/Provenance/ProductList.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"

#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/ParameterSetID.h"
#include "fhiclcpp/ParameterSetRegistry.h"

#include <TBufferFile.h>
#include <TClass.h>
#include <TList.h>
#include <TStreamerInfo.h>

#include <sys/time.h>
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#define CAN_REINIT 0

namespace art {
template<typename U>
class ArtdaqInputHelper;
}  // namespace art

/**
 * \brief This template class provides a unified interface for reading data into art
 * \tparam U The class responsible for delivering data
 *
 * JCF, May-27-2016
 * ArtdaqInputHelper is a template class which takes, as a parameter, a
 * class which it uses to receive data; the instance of this class is
 * called "communicationWrapper_". As of this writing, this wrapper
 * class is implemented by NetMonWrapper (for reading data into the
 * aggregator from the eventbuilder) and TransferWrapper (for reading
 * data into an art process). This class presents a unified approach
 * to handling art provenance, regardless of the communication
 * protocol used to read data in.
 */
template<typename U>
class art::ArtdaqInputHelper
{
public:
	/**
	 * \brief Copy Constructor is deleted
	 */
	ArtdaqInputHelper(const ArtdaqInputHelper&) = delete;

	/**
	 * \brief Copy Assignment operator is deleted
	 * \return ArtdaqInputHelper copy
	 */
	ArtdaqInputHelper& operator=(const ArtdaqInputHelper&) = delete;

	/**
	 * \brief ArtdaqInputHelper Destructor
	 */
	~ArtdaqInputHelper();

	/**
	 * \brief ArtdaqInputHelper Constructor
	 * \param ps ParameterSet used to confiugre communication wrapper class
	 * \param helper An art::ProductRegistryHelper for registering products
	 * \param pm An art::SourceHelper for handling provenance
	 */
	ArtdaqInputHelper(const fhicl::ParameterSet& ps, art::ProductRegistryHelper& helper, art::SourceHelper const& pm);

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
	 * \return True if ArtdaqInputHelper has not been shut down
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
	ArtdaqInputHelper(ArtdaqInputHelper&&) = delete;
	ArtdaqInputHelper& operator=(ArtdaqInputHelper&&) = delete;

	void readAndConstructPrincipal(std::unique_ptr<TBufferFile>&, artdaq::NetMonHeader::MessageType, art::RunPrincipal* const,
	                               art::SubRunPrincipal* const, art::RunPrincipal*&, art::SubRunPrincipal*&,
	                               art::EventPrincipal*&);

	bool constructPrincipal(std::shared_ptr<ArtdaqEvent>, art::RunPrincipal* const,
	                        art::SubRunPrincipal* const, art::RunPrincipal*&, art::SubRunPrincipal*&,
	                        art::EventPrincipal*&);

	template<class T>
	void readDataProducts(std::list<std::unique_ptr<TBufferFile>>&, T* const&);

	void putInPrincipal(RunPrincipal* const&, std::unique_ptr<EDProduct>&&, const BranchDescription&,
	                    std::unique_ptr<const ProductProvenance>&&);

	void putInPrincipal(SubRunPrincipal* const&, std::unique_ptr<EDProduct>&&, const BranchDescription&,
	                    std::unique_ptr<const ProductProvenance>&&);

	void putInPrincipal(EventPrincipal* const&, std::unique_ptr<EDProduct>&&, const BranchDescription&,
	                    std::unique_ptr<const ProductProvenance>&&);

	std::pair<bool, bool> readFragments(std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> const& eventMap, art::RunPrincipal* const theRun, art::SubRunPrincipal* const theSubRun, art::EventPrincipal* const theEvent);

	void readInitMessage();

	bool shutdownMsgReceived_;
	art::SourceHelper const& pm_;
	art::ProductRegistryHelper& helper_;
	U communicationWrapper_;
	ProductList* productList_;
	std::unique_ptr<art::History> history_to_use_;
	bool fragmentsOnlyMode_;
	std::string pretend_module_name;                       ///< The module name to store data under
	size_t bytesRead;                                      ///< running total of number of bytes received
	std::chrono::steady_clock::time_point last_read_time;  ///< Time last read was completed
};

template<typename U>
art::ArtdaqInputHelper<U>::ArtdaqInputHelper(const fhicl::ParameterSet& ps, art::ProductRegistryHelper& helper,
                                             art::SourceHelper const& pm)
    : shutdownMsgReceived_(false)
    , pm_(pm)
    , helper_(helper)
    , communicationWrapper_(ps)
    , productList_()
    , fragmentsOnlyMode_(false)
    , pretend_module_name(ps.get<std::string>("raw_data_label", "daq"))
    , bytesRead(0)
    , last_read_time(std::chrono::steady_clock::now())
{
	root::setup();
	// Instantiate ArtdaqSharedMemoryService to set up artdaq Globals and MetricManager
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;

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

	TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "Begin: ArtdaqInputHelper::ArtdaqInputHelper("
	                                           << "const fhicl::ParameterSet& ps, "
	                                           << "art::ProductRegistryHelper& helper, "
	                                           << "const art::SourceHelper& pm)";
	readInitMessage();

	helper.reconstitutes<artdaq::detail::RawEventHeader, art::InEvent>(pretend_module_name, "RawEventHeader");

	if (ps.get<bool>("register_fragment_types", true))
	{
		TLOG(TLVL_DEBUG + 32, "ArtdaqInputHelper") << "Registering known Fragment labels from ArtdaqFragmentNamingServiceInterface";

		art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> translator;
		helper.reconstitutes<artdaq::Fragments, art::InEvent>(pretend_module_name, translator->GetUnidentifiedInstanceName());
		// Workaround for #22979
		helper.reconstitutes<artdaq::Fragments, art::InRun>(pretend_module_name, translator->GetUnidentifiedInstanceName());
		helper.reconstitutes<artdaq::Fragments, art::InSubRun>(pretend_module_name, translator->GetUnidentifiedInstanceName());

		helper.reconstitutes<std::vector<artdaq::ArtdaqMetadata>, art::InRun>(pretend_module_name, translator->GetUnidentifiedInstanceName());
		helper.reconstitutes<std::vector<artdaq::ArtdaqMetadata>, art::InSubRun>(pretend_module_name, translator->GetUnidentifiedInstanceName());
		helper.reconstitutes<std::vector<artdaq::ArtdaqMetadata>, art::InRun>(pretend_module_name, "StartOfRun");
		helper.reconstitutes<std::vector<artdaq::ArtdaqMetadata>, art::InRun>(pretend_module_name, "EndOfRun");
		helper.reconstitutes<std::vector<artdaq::ArtdaqMetadata>, art::InSubRun>(pretend_module_name, "StartOfSubrun");
		helper.reconstitutes<std::vector<artdaq::ArtdaqMetadata>, art::InSubRun>(pretend_module_name, "EndOfSubrun");

		std::set<std::string> instance_names = translator->GetAllProductInstanceNames();
		for (const auto& set_iter : instance_names)
		{
			helper.reconstitutes<artdaq::Fragments, art::InEvent>(pretend_module_name, set_iter);
		}
	}
	//
	//  Finished with init message.
	//
	TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::ArtdaqInputHelper("
	                                           << "const fhicl::ParameterSet& ps, "
	                                           << "art::ProductRegistryHelper& helper, "
	                                           << "const art::SourceHelper& pm)";
}

template<typename U>
void art::ArtdaqInputHelper<U>::readInitMessage()
{
	TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "Going to receive init message";
	artdaq::FragmentPtrs initFrags = communicationWrapper_.receiveInitMessage();
	TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "Init message received";

	if (!initFrags.empty() && initFrags.front().get()->type() == artdaq::Fragment::EndOfDataFragmentType)
	{
		TLOG_ERROR("ArtdaqInputHelper") << "Received EndOfData as first broadcast! This process never received any data!";
		shutdownMsgReceived_ = true;
	}
	else
	{
		if (initFrags.empty() || initFrags.back().get()->dataSize() == 0)
		{
			TLOG(TLVL_DEBUG + 32, "ArtdaqInputHelper") << "No init message received or zero-size init message: Fragments-only mode activated! This is an EventBuilder!";
			fragmentsOnlyMode_ = true;
		}
		else
		{
			std::list<std::unique_ptr<TBufferFile>> msgs;
			for (auto& initFrag : initFrags)
			{
				auto header = initFrag->metadata<artdaq::NetMonHeader>();
				msgs.emplace_back(new TBufferFile(TBuffer::kRead, header->data_length, initFrag->dataBegin(), kFALSE, nullptr));
			}

			std::set<art::ProcessHistoryID> history_ids;

			for (auto& msg : msgs)
			{
				// This first unsigned long is the message type code, ignored here in the constructor
				ULong_t dummy = 0;
				msg->ReadULong(dummy);

				// ELF: 6/11/2019: This code is taken from TSocket::RecvStreamerInfos
				auto list = dynamic_cast<TList*>(msg->ReadObject(TList::Class()));

				TIter next(list);
				TStreamerInfo* info;
				TObjLink* lnk = list->FirstLink();
				// First call BuildCheck for regular class
				while (lnk)
				{
					info = dynamic_cast<TStreamerInfo*>(lnk->GetObject());
					TObject* element = info->GetElements()->UncheckedAt(0);
					Bool_t isstl = element && strcmp("This", element->GetName()) == 0;
					if (!isstl)
					{
						info->BuildCheck();
						TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: importing TStreamerInfo: " << info->GetName() << ", version = " << info->GetClassVersion();
					}
					lnk = lnk->Next();
				}
				// Then call BuildCheck for stl class
				lnk = list->FirstLink();
				while (lnk)
				{
					info = dynamic_cast<TStreamerInfo*>(lnk->GetObject());
					TObject* element = info->GetElements()->UncheckedAt(0);
					Bool_t isstl = element && strcmp("This", element->GetName()) == 0;
					if (isstl)
					{
						info->BuildCheck();
						TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: importing TStreamerInfo: " << info->GetName() << ", version = " << info->GetClassVersion();
					}
					lnk = lnk->Next();
				}
				// ELF: 6/11/2019: End TSocket snippet

				//
				//  Read the ParameterSetRegistry.
				//
				ULong_t ps_cnt = 0;
				msg->ReadULong(ps_cnt);
				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: parameter set count: " << ps_cnt;
				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: reading parameter sets ...";
				for (ULong_t I = 0; I < ps_cnt; ++I)
				{
					std::string pset_str = "";  // = ReadObjectAny<std::string>(msg, "std::string", "ArtdaqInputHelper::ArtdaqInputHelper");
					msg->ReadStdString(pset_str);

					TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: parameter set: " << pset_str;

					fhicl::ParameterSet pset;
					pset = fhicl::ParameterSet::make(pset_str);
					// Force id calculation.
					pset.id();
					fhicl::ParameterSetRegistry::put(pset);
				}
				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: finished reading parameter sets.";

				//
				//  Read the MasterProductRegistry.
				//
				auto thisProductList = ReadObjectAny<art::ProductList>(
				    msg, "std::map<art::BranchKey,art::BranchDescription>", "ArtdaqInputHelper::ArtdaqInputHelper");
				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: Input Product list sz=" << thisProductList->size();

				bool productListInitialized = productList_ != nullptr;
				if (!productListInitialized) productList_ = thisProductList;
				for (auto I = thisProductList->begin(), E = thisProductList->end(); I != E; ++I)
				{
#ifndef __OPTIMIZE__
					TLOG(TLVL_DEBUG + 50, "ArtdaqInputHelper") << "Branch key: class: '" << I->first.friendlyClassName_ << "' modlbl: '"
					                                           << I->first.moduleLabel_ << "' instnm: '" << I->first.productInstanceName_ << "' procnm: '"
					                                           << I->first.processName_ << "', branch description name: " << I->second.wrappedName();
#endif
					if (productListInitialized)
					{
						productList_->emplace(*I);
					}
				}

				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: Reading ProcessHistory";
				auto phm = ReadObjectAny<art::ProcessHistoryMap>(
				    msg, "std::map<const art::Hash<2>,art::ProcessHistory>", "ArtdaqInputHelper::ArtdaqInputHelper");
				printProcessMap(*phm, "ArtdaqInputHelper's ProcessHistoryMap");

				for (auto& proc_hist : *phm)
				{
					history_ids.insert(proc_hist.second.id());
				}

				ProcessHistoryRegistry::put(*phm);
				printProcessMap(ProcessHistoryRegistry::get(), "ArtdaqInputHelper's ProcessHistoryRegistry");

				//
				//  Read the ParentageRegistry.
				//
				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: Reading ParentageMap";
				auto parentageMap = ReadObjectAny<ParentageMap>(msg, "art::ParentageMap", "ArtdaqInputHelper::ArtdaqInputHelper");
				ParentageRegistry::put(*parentageMap);
			}

			// We're going to make a fake History using the collected process histories!
			art::ProcessHistory fake_process_history;
			for (auto& hist : history_ids)
			{
				if (!hist.isValid())
				{
					TLOG(TLVL_WARNING, "ArtdaqInputHelper") << "Encountered invalid history ID!";
					continue;
				}

				ProcessHistory thisProcessHistory;
				if (ProcessHistoryRegistry::get(hist, thisProcessHistory))
				{
					for (auto& conf : thisProcessHistory)
					{
						if (auto e = fake_process_history.end();
						    std::find(fake_process_history.begin(), e, conf) == e)
						{
							fake_process_history.push_back(conf);
						}
					}
				}
			}
			art::ProcessHistoryMap fake_process_history_map;
			fake_process_history_map[fake_process_history.id()] = fake_process_history;
			ProcessHistoryRegistry::put(fake_process_history_map);
			printProcessMap(ProcessHistoryRegistry::get(), "ArtdaqInputHelper's ProcessHistoryRegistry w/fake history");

			history_to_use_.reset(new History());
			history_to_use_->setProcessHistoryID(fake_process_history.id());

			TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper")
			    << "ArtdaqInputHelper: Product list sz=" << productList_->size();
#if 0
			for (auto I = productList_->begin(), E = productList_->end(); I != E; ++I)
			{
				TLOG(TLVL_DEBUG + 50, "ArtdaqInputHelper") << "Branch key: class: '" << I->first.friendlyClassName_ << "' modlbl: '"
				                                           << I->first.moduleLabel_ << "' instnm: '" << I->first.productInstanceName_ << "' procnm: '"
				                                           << I->first.processName_ << "', branch description name: " << I->second.wrappedName();
			}
#endif
			// helper now owns productList_!

			helper_.productList(std::unique_ptr<art::ProductList>(productList_));

			TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "ArtdaqInputHelper: got product list";
		}
	}
}

template<typename U>
art::ArtdaqInputHelper<U>::~ArtdaqInputHelper()
{}

template<typename U>
void art::ArtdaqInputHelper<U>::closeCurrentFile()
{
	TLOG(TLVL_DEBUG + 34, "ArtdaqInputHelper") << "Begin/End: ArtdaqInputHelper::closeCurrentFile()";
}

template<typename U>
void art::ArtdaqInputHelper<U>::readFile(const std::string&, art::FileBlock*& fb)
{
	TLOG(TLVL_DEBUG + 35, "ArtdaqInputHelper") << "Begin: ArtdaqInputHelper::"
	                                              "readFile(const std::string& name, art::FileBlock*& fb)";
	fb = new art::FileBlock(art::FileFormatVersion(1, "ArtdaqInputHelper2013"), "");
	TLOG(TLVL_DEBUG + 35, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::"
	                                              "readFile(const std::string& name, art::FileBlock*& fb)";
}

template<typename U>
bool art::ArtdaqInputHelper<U>::hasMoreData() const
{
	TLOG(TLVL_DEBUG + 36, "ArtdaqInputHelper") << "Begin: ArtdaqInputHelper::hasMoreData()";
	if (shutdownMsgReceived_)
	{
		TLOG(TLVL_DEBUG + 36, "ArtdaqInputHelper") << "ArtdaqInputHelper::hasMoreData(): "
		                                              "returning false on shutdownMsgReceived_.";
		TLOG(TLVL_DEBUG + 36, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::hasMoreData()";
		return false;
	}
	TLOG(TLVL_DEBUG + 32, "ArtdaqInputHelper") << "ArtdaqInputHelper::hasMoreData(): "
	                                              "returning true on not shutdownMsgReceived_.";
	TLOG(TLVL_DEBUG + 36, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::hasMoreData()";
	return true;
}

template<typename U>
void art::ArtdaqInputHelper<U>::readAndConstructPrincipal(std::unique_ptr<TBufferFile>& msg, artdaq::NetMonHeader::MessageType msg_type_code,
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

	// Establish default 'results'
	outR = nullptr;
	outSR = nullptr;
	outE = nullptr;

	art::Timestamp currentTime = 0;
	timespec hi_res_time;
	int retcode = clock_gettime(CLOCK_REALTIME, &hi_res_time);
	TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "hi_res_time tv_sec = " << hi_res_time.tv_sec
	                                           << " tv_nsec = " << hi_res_time.tv_nsec << " (retcode = " << retcode << ")";
	if (retcode == 0)
	{
		currentTime = ((hi_res_time.tv_sec & 0xffffffff) << 32) | (hi_res_time.tv_nsec & 0xffffffff);
	}
	else
	{
		TLOG_ERROR("ArtdaqInputHelper")
		    << "Unable to fetch a high-resolution time with clock_gettime for art::SubRun Timestamp. ";
	}

	TLOG(TLVL_DEBUG + 37, "ArtdaqInputHelper") << "inR: " << static_cast<void*>(inR) << " run " << (inR ? std::to_string(inR->run()) : "invalid")
	                                           << ", inSR: " << static_cast<void*>(inSR) << " run " << (inSR ? std::to_string(inSR->run()) : "invalid")
	                                           << ", subrun " << (inSR ? std::to_string(inSR->subRun()) : "invalid");

	// Process Run Aux
	TLOG(TLVL_DEBUG + 37, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
	                                           << "processing Run auxiliary ...";

	run_aux.reset(ReadObjectAny<art::RunAuxiliary>(msg, "art::RunAuxiliary", "ArtdaqInputHelper::readAndConstructPrincipal"));
	run_aux->setProcessHistoryID(history_to_use_->processHistoryID());
	printProcessHistoryID("readAndConstructPrincipal", run_aux.get());

	TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
	                                           << "inR: " << static_cast<void*>(inR) << " run/expected "
	                                           << (inR ? std::to_string(inR->run()) : "invalid") << "/" << run_aux->run();

	if ((inR == nullptr) || !inR->runID().isValid() || (inR->run() != run_aux->run()))
	{
		// New run, either we have no input RunPrincipal, or the
		// input run number does not match the run number.
		TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: making RunPrincipal ...";
		outR = pm_.makeRunPrincipal(*run_aux);
	}

	TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
	                                           << "finished processing Run auxiliary.";

	if (msg_type_code != artdaq::NetMonHeader::MessageType::Run)  // SubRun or Event
	{
		TLOG(TLVL_DEBUG + 38, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
		                                           << "processing SubRun auxiliary ...";

		subrun_aux.reset(
		    ReadObjectAny<art::SubRunAuxiliary>(msg, "art::SubRunAuxiliary", "ArtdaqInputHelper::readAndConstructPrincipal"));
		printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());

		// HACK! Make the SR PHID match!
		printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());
		subrun_aux->setProcessHistoryID(run_aux->processHistoryID());
		printProcessHistoryID("readAndConstructPrincipal", subrun_aux.get());

		TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
		                                           << "inSR: " << static_cast<void*>(inSR) << " run/expected "
		                                           << (inSR ? std::to_string(inSR->run()) : "invalid") << "/" << subrun_aux->run()
		                                           << ", subrun/expected " << (inSR ? std::to_string(inSR->subRun()) : "invalid") << "/"
		                                           << subrun_aux->subRun();

		art::SubRunID subrun_check(subrun_aux->run(), subrun_aux->subRun());
		if (inSR == nullptr || !inSR->subRunID().isValid() || subrun_check != inSR->subRunID())
		{
			// New SubRun, either we have no input SubRunPrincipal, or the
			// input subRun number does not match the subRun number.
			TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
			                                           << "making SubRunPrincipal ...";
			outSR = pm_.makeSubRunPrincipal(*subrun_aux);
		}

		TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
		                                           << "finished processing SubRun auxiliary.";
	}

	if (msg_type_code == artdaq::NetMonHeader::MessageType::Event)
	{  // Event message.

		TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
		                                           << "processing Event auxiliary ...";

		event_aux.reset(
		    ReadObjectAny<art::EventAuxiliary>(msg, "art::EventAuxiliary", "ArtdaqInputHelper::readAndConstructPrincipal"));

		TLOG(TLVL_DEBUG + 34, "ArtdaqInputHelper") << "readAndConstructPrincipal: making EventPrincipal ...";
#if ART_HEX_VERSION < 0x31100
		auto historyPtr = std::unique_ptr<art::History>(new History(*(history_to_use_.get())));
		if (!art::ProcessHistoryRegistry::get().count(history_to_use_->processHistoryID()))
		{
			TLOG_WARNING("ArtdaqInputHelper") << "Stored history is not in ProcessHistoryRegistry, this event may have issues!";
		}
		outE = pm_.makeEventPrincipal(*event_aux, std::move(historyPtr));
#else
		if (!art::ProcessHistoryRegistry::get().count(history_to_use_->processHistoryID()))
		{
			TLOG_WARNING("ArtdaqInputHelper") << "Stored history is not in ProcessHistoryRegistry, this event may have issues!";
		}
		event_aux->setProcessHistoryID(history_to_use_->processHistoryID());
		outE = pm_.makeEventPrincipal(*event_aux);
#endif
		TLOG(TLVL_DEBUG + 39, "ArtdaqInputHelper") << "readAndConstructPrincipal: "
		                                           << "finished processing Event auxiliary.";
	}
	else if (msg_type_code == artdaq::NetMonHeader::MessageType::Subrun)
	{
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "SubrunDataFragment received, returning Flush event";
		art::SubRunID srID = outSR ? outSR->subRunID() : inSR->subRunID();
		art::EventID const evid(art::EventID::flushEvent(srID));
		outE = pm_.makeEventPrincipal(evid, currentTime);
	}
	else if (msg_type_code == artdaq::NetMonHeader::MessageType::Run)
	{
		art::RunID rID = outR ? outR->runID() : inR->runID();
		art::EventID const evid(art::EventID::flushEvent(rID));
		if (inSR == nullptr || inSR->subRunID() != evid.subRunID())
		{
			outSR = pm_.makeSubRunPrincipal(evid.subRunID(), currentTime);
		}
		outE = pm_.makeEventPrincipal(evid, currentTime);
	}
}

template<typename U>
bool art::ArtdaqInputHelper<U>::constructPrincipal(std::shared_ptr<ArtdaqEvent> eventPtr, art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR, art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE)
{
	// We return false, indicating we're done reading, if:
	//   1) we did not obtain an event, because we timed out and were
	//      configured NOT to keep trying after a timeout, or
	//   2) the event we read was the end-of-data marker: a null
	//      pointer
	if (eventPtr->FirstFragmentType() == artdaq::Fragment::EndOfDataFragmentType)
	{
		TLOG(TLVL_DEBUG + 32, "ArtdaqInputHelper") << "Received shutdown message, returning false";
		shutdownMsgReceived_ = true;
		return false;
	}

	if (!eventPtr->header)
	{
		TLOG_ERROR("ArtdaqInputHelper") << "No RawEventHeader received, cannot construct principals!";
		shutdownMsgReceived_ = true;
		return false;
	}

	// Check the number of fragments in the RawEvent.  If we have a single
	// fragment and that fragment is marked as EndRun or EndSubrun we'll create
	// the special principals for that.
	art::Timestamp currentTime = 0;
	timespec hi_res_time;
	int retcode = clock_gettime(CLOCK_REALTIME, &hi_res_time);
	TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "hi_res_time tv_sec = " << hi_res_time.tv_sec
	                                           << " tv_nsec = " << hi_res_time.tv_nsec << " (retcode = " << retcode << ")";
	if (retcode == 0)
	{
		currentTime = ((hi_res_time.tv_sec & 0xffffffff) << 32) | (hi_res_time.tv_nsec & 0xffffffff);
	}
	else
	{
		TLOG_ERROR("ArtdaqInputHelper")
		    << "Unable to fetch a high-resolution time with clock_gettime for art::Event Timestamp. "
		    << "The art::Event Timestamp will be zero for event " << eventPtr->header->event_id;
	}

	// make new run if inR is 0 or if the run has changed
	if (inR == nullptr || !inR->runID().isValid() || inR->run() != eventPtr->header->run_id)
	{
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "Making run principal with run_id " << eventPtr->header->run_id;
		outR = pm_.makeRunPrincipal(eventPtr->header->run_id, currentTime);
	}

	if (eventPtr->FirstFragmentType() == artdaq::Fragment::EndOfRunFragmentType)
	{
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "EndOfRunFragment received, returning Flush subrun/event";
		art::EventID const evid(art::EventID::flushEvent(outR != nullptr ? outR->runID() : inR->runID()));
		if (inSR == nullptr || inSR->subRunID() != evid.subRunID())
		{
			outSR = pm_.makeSubRunPrincipal(evid.subRunID(), currentTime);
		}
		outE = pm_.makeEventPrincipal(evid, currentTime);
		return true;
	}

	// make new subrun if inSR is 0 or if the subrun has changed
	art::SubRunID subrun_check(eventPtr->header->run_id, eventPtr->header->subrun_id);
	if (inSR == nullptr || !inSR->subRunID().isValid() || subrun_check != inSR->subRunID())
	{
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "Making subrun principal with subrun_id " << eventPtr->header->subrun_id;
		outSR = pm_.makeSubRunPrincipal(eventPtr->header->run_id, eventPtr->header->subrun_id, currentTime);
	}

	if (eventPtr->FirstFragmentType() == artdaq::Fragment::EndOfSubrunFragmentType)
	{
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "EndOfSubrunFragment received, returning Flush event";
		art::EventID const evid(art::EventID::flushEvent(outSR != nullptr ? outSR->subRunID() : inSR->subRunID()));
		outE = pm_.makeEventPrincipal(evid, currentTime);
		return true;
	}

	TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "Making event principal with event_id " << eventPtr->header->event_id;
	outE = pm_.makeEventPrincipal(eventPtr->header->run_id, eventPtr->header->subrun_id, eventPtr->header->event_id, currentTime);
	return true;
}

template<typename U>
template<class T>
void art::ArtdaqInputHelper<U>::readDataProducts(std::list<std::unique_ptr<TBufferFile>>& msgs, T* const& outPrincipal)
{
	for (auto& msg : msgs)
	{
		ULong_t prd_cnt = 0;
		{
			TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: reading data product count ...";
			msg->ReadULong(prd_cnt);
			TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: product count: " << prd_cnt;
		}
		//
		//  Read the data products.
		//
		for (ULong_t I = 0; I < prd_cnt; ++I)
		{
			std::unique_ptr<BranchKey> bk;
			{
				TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: Reading branch key.";
				bk.reset(ReadObjectAny<BranchKey>(msg, "art::BranchKey", "ArtdaqInputHelper::readDataProducts"));
			}

#ifndef __OPTIMIZE__
			TLOG(TLVL_DEBUG + 41, "ArtdaqInputHelper") << "readDataProducts: got product class: '" << bk->friendlyClassName_ << "' modlbl: '"
			                                           << bk->moduleLabel_ << "' instnm: '" << bk->productInstanceName_ << "' procnm: '"
			                                           << bk->processName_;
#endif
			ProductList::const_iterator iter;
			{
				TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: looking up product ...";
				iter = productList_->find(*bk);
				if (iter == productList_->end())
				{
					throw art::Exception(art::errors::ProductNotFound)  // NOLINT(cert-err60-cpp)
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
				TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: Reading product with wrapped name: " << bd.wrappedName()
				                                           << ", TClass = " << static_cast<void*>(TClass::GetClass(bd.wrappedName().c_str()));

				// JCF, May-25-2016
				// Currently unclear why the templatized version of ReadObjectAny doesn't work here...

				//	    prd.reset(ReadObjectAny<EDProduct>(msg, bd.wrappedName()));

				void* p = msg->ReadObjectAny(TClass::GetClass(bd.wrappedName().c_str()));
				auto pp = reinterpret_cast<EDProduct*>(p);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

				TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: After ReadObjectAny(prd): p=" << p << ", EDProduct::isPresent: " << pp->isPresent();
				prd.reset(pp);
				p = nullptr;
			}
			std::unique_ptr<const ProductProvenance> prdprov;
			{
				TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: Reading product provenance.";
				prdprov.reset(ReadObjectAny<ProductProvenance>(msg, "art::ProductProvenance", "ArtdaqInputHelper::readDataProducts"));
			}

			{
				TLOG(TLVL_DEBUG + 40, "ArtdaqInputHelper") << "readDataProducts: inserting product: class: '" << bd.friendlyClassName()
				                                           << "' modlbl: '" << bd.moduleLabel() << "' instnm: '" << bd.productInstanceName()
				                                           << "' procnm: '" << bd.processName() << "' id: '" << bd.productID() << "'";
				putInPrincipal(outPrincipal, std::move(prd), bd, std::move(prdprov));
			}
		}
	}
}

template<typename U>
void art::ArtdaqInputHelper<U>::putInPrincipal(RunPrincipal* const& rp, std::unique_ptr<EDProduct>&& prd,
                                               const BranchDescription& bd,
                                               std::unique_ptr<const ProductProvenance>&& prdprov)
{
	rp->put(bd, std::move(prdprov), std::move(prd), std::make_unique<RangeSet>(RangeSet::forRun(rp->runID())));
}

template<typename U>
void art::ArtdaqInputHelper<U>::putInPrincipal(SubRunPrincipal* const& srp, std::unique_ptr<EDProduct>&& prd,
                                               const BranchDescription& bd,
                                               std::unique_ptr<const ProductProvenance>&& prdprov)
{
	srp->put(bd, std::move(prdprov), std::move(prd), std::make_unique<RangeSet>(RangeSet::forSubRun(srp->subRunID())));
}

template<typename U>
void art::ArtdaqInputHelper<U>::putInPrincipal(EventPrincipal* const& ep, std::unique_ptr<EDProduct>&& prd,
                                               const BranchDescription& bd,
                                               std::unique_ptr<const ProductProvenance>&& prdprov)
{
	TLOG(TLVL_DEBUG + 42, "ArtdaqInputHelper") << "EventPrincipal size before put: " << ep->size();

	ep->put(bd, std::move(prdprov), std::move(prd), std::make_unique<RangeSet>(RangeSet::invalid()));

	TLOG(TLVL_DEBUG + 42, "ArtdaqInputHelper") << "EventPrincipal size after put: " << ep->size();
}

template<typename U>
std::pair<bool, bool> art::ArtdaqInputHelper<U>::readFragments(std::unordered_map<artdaq::Fragment::type_t, std::unique_ptr<artdaq::Fragments>> const& eventMap, art::RunPrincipal* const theRun, art::SubRunPrincipal* const theSubRun, art::EventPrincipal* const theEvent)
{
	// Now read in Fragments
	double fragmentLatency = 0;
	double fragmentLatencyMax = 0.0;
	size_t fragmentCount = 0;

	bool eventProductsRead = false;
	bool subrunProductsRead = false;

	art::ServiceHandle<ArtdaqFragmentNamingServiceInterface> translator;

	// insert the Fragments of each type into the EventPrincipal
	for (auto& fragmentTypePair : eventMap)
	{
		auto type_code = fragmentTypePair.first;
		if (artdaq::Fragment::isSystemFragmentType(type_code) && type_code != artdaq::Fragment::ContainerFragmentType && type_code != artdaq::Fragment::EmptyFragmentType)
		{
			if (type_code == artdaq::Fragment::EndOfRunFragmentType)
			{
				std::unordered_map<std::string, std::unique_ptr<std::vector<artdaq::ArtdaqMetadata>>> metadata_coll;
				for (auto& frag : *fragmentTypePair.second)
				{
					artdaq::MetadataFragment mf(frag);
					auto md = mf.get_metadata();

					std::pair<bool, std::string> instance_name_result =
					    translator->GetInstanceNameForFragment(frag);
					std::string label = instance_name_result.second;
					if (!instance_name_result.first)
					{
						TLOG_WARNING("ArtdaqInputHelper")
						    << "UnknownFragmentType: The product instance name mapping for fragment type \"" << static_cast<int>(type_code)
						    << "\" is not known. Fragments of this "
						    << "type will be stored in the event with an instance name of \"" << label << "\".";
					}
					if (!metadata_coll.count(label))
					{
						TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Creating output ArtdaqMetadata storage for label " << label;
						metadata_coll[label] = std::make_unique<std::vector<artdaq::ArtdaqMetadata>>();
					}
					TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Adding Fragment " << frag.fragmentID() << " to storage with label " << label << " (sz=" << metadata_coll[label]->size() + 1 << ")";
					metadata_coll[label]->push_back(md);
				}
				for (auto& type : metadata_coll)
				{
					TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Adding " << type.second->size() << " ArtdaqMetadatas with label " << type.first << " to Run.";
					put_product_in_principal(std::move(type.second), *theRun, pretend_module_name, type.first);
				}
			}
			else if (type_code == artdaq::Fragment::EndOfSubrunFragmentType)
			{
				std::unordered_map<std::string, std::unique_ptr<std::vector<artdaq::ArtdaqMetadata>>> metadata_coll;
				for (auto& frag : *fragmentTypePair.second)
				{
					artdaq::MetadataFragment mf(frag);
					auto md = mf.get_metadata();

					std::pair<bool, std::string> instance_name_result =
					    translator->GetInstanceNameForFragment(frag);
					std::string label = instance_name_result.second;
					if (!instance_name_result.first)
					{
						TLOG_WARNING("ArtdaqInputHelper")
						    << "UnknownFragmentType: The product instance name mapping for fragment type \"" << static_cast<int>(type_code)
						    << "\" is not known. Fragments of this "
						    << "type will be stored in the event with an instance name of \"" << label << "\".";
					}
					if (!metadata_coll.count(label))
					{
						TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Creating output ArtdaqMetadata storage for label " << label;
						metadata_coll[label] = std::make_unique<std::vector<artdaq::ArtdaqMetadata>>();
					}
					TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Adding Fragment " << frag.fragmentID() << " to storage with label " << label << " (sz=" << metadata_coll[label]->size() + 1 << ")";
					metadata_coll[label]->push_back(md);
				}
				for (auto& type : metadata_coll)
				{
					TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Adding " << type.second->size() << " ArtdaqMetadatas with label " << type.first << " to SubRun.";
					put_product_in_principal(std::move(type.second), *theSubRun, pretend_module_name, type.first);
					subrunProductsRead = true;
				}
			}
			else
			{
				TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "Skipping system Fragment with type " << static_cast<int>(type_code) << " ( " << translator->GetInstanceNameForType(type_code) << " )";
			}
			continue;
		}
		TLOG(TLVL_DEBUG + 33, "ArtdaqInputHelper") << "type is " << static_cast<int>(type_code) << ", number of fragments is " << fragmentTypePair.second->size();

		std::unordered_map<std::string, std::unique_ptr<artdaq::Fragments>> derived_fragments;
		for (auto& frag : *fragmentTypePair.second)
		{
			TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Processing Fragment with ID " << frag.fragmentID();
			bytesRead += frag.sizeBytes();
			auto latency_s = frag.getLatency(true);
			double latency = latency_s.tv_sec + (latency_s.tv_nsec / 1000000000.0);

			fragmentLatency += latency;
			fragmentCount++;
			if (latency > fragmentLatencyMax) fragmentLatencyMax = latency;

			std::pair<bool, std::string> instance_name_result =
			    translator->GetInstanceNameForFragment(frag);
			std::string label = instance_name_result.second;
			if (!instance_name_result.first)
			{
				TLOG_WARNING("ArtdaqInputHelper")
				    << "UnknownFragmentType: The product instance name mapping for fragment type \"" << static_cast<int>(type_code)
				    << "\" is not known. Fragments of this "
				    << "type will be stored in the event with an instance name of \"" << label << "\".";
			}
			if (!derived_fragments.count(label))
			{
				TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Creating output Fragment storage for label " << label;
				derived_fragments[label] = std::make_unique<artdaq::Fragments>();
			}
			TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Adding Fragment " << frag.fragmentID() << " to storage with label " << label << " (sz=" << derived_fragments[label]->size() + 1 << ")";
			derived_fragments[label]->emplace_back(std::move(frag));
		}
		for (auto& type : derived_fragments)
		{
			TLOG(TLVL_DEBUG + 44, "ArtdaqInputHelper") << "Adding " << type.second->size() << " Fragments with label " << type.first << " to event.";
			put_product_in_principal(std::move(type.second), *theEvent, pretend_module_name, type.first);
			eventProductsRead = true;
		}
	}

	if (metricMan)
	{
		metricMan->sendMetric("bytesRead", bytesRead, "B", 3, artdaq::MetricMode::LastPoint);

		metricMan->sendMetric("ArtdaqInputHelper Latency", fragmentLatency / fragmentCount, "s", 4, artdaq::MetricMode::Average);
		metricMan->sendMetric("ArtdaqInputHelper Maximum Latency", fragmentLatencyMax, "s", 4, artdaq::MetricMode::Maximum);
	}

	return std::make_pair(subrunProductsRead, eventProductsRead);
}

template<typename U>
bool art::ArtdaqInputHelper<U>::readNext(art::RunPrincipal* const inR, art::SubRunPrincipal* const inSR,
                                         art::RunPrincipal*& outR, art::SubRunPrincipal*& outSR, art::EventPrincipal*& outE)
{
	TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "Begin: ArtdaqInputHelper::readNext";

	if (shutdownMsgReceived_)
	{
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "Shutdown message already received, returning false";
		TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::readNext";
		return false;
	}

	auto read_start_time = std::chrono::steady_clock::now();

	std::shared_ptr<ArtdaqEvent> eventMap = communicationWrapper_.receiveMessages();
	auto got_event_time = std::chrono::steady_clock::now();

	if (eventMap == nullptr)
	{
		TLOG(TLVL_ERROR, "ArtdaqInputHelper") << "No Fragments received! Aborting...";
		shutdownMsgReceived_ = true;
		TLOG(TLVL_DEBUG + 45, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::readNext";
		return false;
	}

	if (eventMap->FirstFragmentType() == artdaq::Fragment::EndOfDataFragmentType)
	{
		TLOG(TLVL_DEBUG + 45, "ArtdaqInputHelper") << "Shutdown message received!";
		shutdownMsgReceived_ = true;
		TLOG(TLVL_DEBUG + 45, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::readNext";
		return false;
	}

	if (eventMap->FirstFragmentType() == artdaq::Fragment::InitFragmentType)
	{
#if CAN_REINIT
		TLOG(TLVL_INFO, "ArtdaqInputHelper") << "Additional Init Message received! Attempting to register new products...";
		readInitMessage();
#else
		TLOG(TLVL_WARNING, "ArtdaqInputHelper") << "Received additional Init Message! Check init_fragment_count configuration!";
#endif
		TLOG(TLVL_DEBUG + 45, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::readNext";
		return true;
	}

	if (fragmentsOnlyMode_)
	{
		if (eventMap->fragments.count(artdaq::Fragment::DataFragmentType) || eventMap->fragments.count(artdaq::Fragment::RunDataFragmentType) || eventMap->fragments.count(artdaq::Fragment::SubrunDataFragmentType))
		{
			TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "ArtdaqInputHelper::readNext unexpectedly got a message with a DataFragment. This art Event will NOT be reconstructed!";
		}

		auto firstFragmentType = eventMap->FirstFragmentType();
		TLOG(TLVL_DEBUG + 32, "ArtdaqInputHelper") << "First Fragment type is " << static_cast<int>(firstFragmentType);
		if (constructPrincipal(eventMap, inR, inSR, outR, outSR, outE))
		{
			readFragments(eventMap->fragments, outR ? outR : inR, outSR ? outSR : inSR, outE);
		}
	}
	else
	{
		std::list<std::unique_ptr<TBufferFile>> msgs;
		if (eventMap->fragments.count(artdaq::Fragment::RunDataFragmentType))
			for (auto& dataFrag : *(eventMap->fragments[artdaq::Fragment::RunDataFragmentType]))
			{
				if (!dataFrag.hasMetadata()) continue;
				auto header = dataFrag.metadata<artdaq::NetMonHeader>();
				msgs.emplace_back(new TBufferFile(TBuffer::kRead, header->data_length, dataFrag.dataBegin(), kFALSE, nullptr));
			}
		if (eventMap->fragments.count(artdaq::Fragment::SubrunDataFragmentType))
			for (auto& dataFrag : *(eventMap->fragments[artdaq::Fragment::SubrunDataFragmentType]))
			{
				if (!dataFrag.hasMetadata()) continue;
				auto header = dataFrag.metadata<artdaq::NetMonHeader>();
				msgs.emplace_back(new TBufferFile(TBuffer::kRead, header->data_length, dataFrag.dataBegin(), kFALSE, nullptr));
			}
		if (eventMap->fragments.count(artdaq::Fragment::DataFragmentType))
			for (auto& dataFrag : *(eventMap->fragments[artdaq::Fragment::DataFragmentType]))
			{
				if (!dataFrag.hasMetadata()) continue;
				auto header = dataFrag.metadata<artdaq::NetMonHeader>();
				msgs.emplace_back(new TBufferFile(TBuffer::kRead, header->data_length, dataFrag.dataBegin(), kFALSE, nullptr));
			}

		//
		//  Read message type code.
		//
		artdaq::NetMonHeader::MessageType msg_type_code = artdaq::NetMonHeader::MessageType::Invalid;
		ULong_t msg_type_code_tmp = 0;
		for (auto& msg : msgs)
		{
			TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "ArtdaqInputHelper::readNext: "
			                                           << "getting message type code ...";
			msg->ReadULong(msg_type_code_tmp);
			TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "ArtdaqInputHelper::readNext: "
			                                           << "message type: " << msg_type_code_tmp;

			if (msg_type_code == artdaq::NetMonHeader::MessageType::Invalid)
				msg_type_code = static_cast<artdaq::NetMonHeader::MessageType>(msg_type_code_tmp);
			else if (msg_type_code != static_cast<artdaq::NetMonHeader::MessageType>(msg_type_code_tmp))
			{
				TLOG(TLVL_ERROR, "ArtdaqInputHelper") << "ArtdaqInputHelper::readNext: Received conflicting message type codes! Aborting...";

				shutdownMsgReceived_ = true;
				TLOG(TLVL_DEBUG + 45, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::readNext";
				return false;
			}
		}

		for (auto& msg : msgs)
		{
			readAndConstructPrincipal(msg, msg_type_code, inR, inSR, outR, outSR, outE);
		}
		//
		//  Read per-event metadata needed to construct principal.
		//
		if (msg_type_code == artdaq::NetMonHeader::MessageType::Run)
		{
			// EndRun message.
			// FIXME: We need to merge these into the input RunPrincipal.
			readDataProducts(msgs, outR ? outR : inR);
		}
		else if (msg_type_code == artdaq::NetMonHeader::MessageType::Subrun)
		{
			// FIXME: We need to merge these into the input SubRunPrincipal.
			readDataProducts(msgs, outSR ? outSR : inSR);
		}
		else if (msg_type_code == artdaq::NetMonHeader::MessageType::Event)
		{
			// Event message.
			readDataProducts(msgs, outE);

			if (eventMap->fragments.size() > 1)
			{
				readFragments(eventMap->fragments, outR ? outR : inR, outSR ? outSR : inSR, outE);
			}
		}
		else
		{
			// Did not have a valid message, try again
			return false;
		}
	}

	if (outE != nullptr)
	{
		auto artHdrPtr = std::make_unique<artdaq::detail::RawEventHeader>();
		auto daqHdrPtr = eventMap->header;

		if (daqHdrPtr != nullptr)
		{
			memcpy(artHdrPtr.get(), daqHdrPtr.get(), sizeof(artdaq::detail::RawEventHeader));
			put_product_in_principal(std::move(artHdrPtr), *outE, pretend_module_name, "RawEventHeader");
		}
	}

	auto read_finish_time = std::chrono::steady_clock::now();
	TLOG(TLVL_DEBUG + 43, "ArtdaqInputHelper") << "readNext: bytesRead=" << bytesRead
	                                           << " metricMan=" << static_cast<void*>(metricMan.get());
	if (metricMan)
	{
		metricMan->sendMetric("Avg Processing Time", artdaq::TimeUtils::GetElapsedTime(last_read_time, read_start_time),
		                      "s", 2, artdaq::MetricMode::Average);
		metricMan->sendMetric("Avg Input Wait Time", artdaq::TimeUtils::GetElapsedTime(read_start_time, got_event_time),
		                      "s", 3, artdaq::MetricMode::Average);
		metricMan->sendMetric("Avg Read Time", artdaq::TimeUtils::GetElapsedTime(got_event_time, read_finish_time), "s",
		                      3, artdaq::MetricMode::Average);
	}

	TLOG(TLVL_DEBUG + 48, "ArtdaqInputHelper") << "End:   ArtdaqInputHelper::readNext ret=" << std::boolalpha << (outR || outSR || outE);
	last_read_time = std::chrono::steady_clock::now();
	return outR || outSR || outE;
}

#endif  // ARTDAQ_ARTDAQ_ARTMODULES_ARTDAQINPUTHELPER_HH_
