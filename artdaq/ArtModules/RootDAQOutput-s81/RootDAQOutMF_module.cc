// vim: set sw=2 expandtab :
#include "TRACE/tracemf.h"  // TLOG
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RootDAQOutMF").c_str()

#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"
#include "artdaq/ArtModules/RootDAQOutput-s81/RootDAQOutFile.h"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Core/RPManager.h"
#include "art/Framework/Core/ResultsProducer.h"
#include "art/Framework/IO/ClosingCriteria.h"
#include "art/Framework/IO/FileStatsCollector.h"
#include "art/Framework/IO/PostCloseFileRenamer.h"
#include "art/Framework/IO/detail/logFileAction.h"
#include "art/Framework/IO/detail/validateFileNamePattern.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Principal.h"
#include "art/Framework/Principal/Results.h"
#include "art/Framework/Principal/ResultsPrincipal.h"
#include "art/Framework/Principal/Run.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRun.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Utilities/parent_path.h"
#include "art/Utilities/unique_filename.h"
#include "art_root_io/DropMetaData.h"
#include "art_root_io/RootFileBlock.h"
#include "art_root_io/detail/rootOutputConfigurationTools.h"
#include "art_root_io/setup.h"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "canvas/Persistency/Provenance/ProductTables.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/OptionalAtom.h"
#include "fhiclcpp/types/OptionalSequence.h"
#include "fhiclcpp/types/Table.h"
#include "fhiclcpp/types/TableFragment.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <deque>
#include <map>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

using namespace std;
using namespace hep::concurrency;

namespace {
string const dev_null{"/dev/null"};
}

namespace art {

class RootDAQOutFile;

// RootDAQOutMF is a variant of RootDAQOut that can keep multiple ROOT files
// open at the same time.  When the active file's closing criteria are met (e.g.
// maxEvents, maxSubRuns, maxRuns), it is moved to a "pending-close" queue and a
// new file is opened immediately.  Files in the pending-close queue still have
// their TFile open in memory; they are flushed to disk only when the queue
// would exceed maxOpenFiles.  This pipelining reduces the gap in data writing
// that occurs during file transitions.
class RootDAQOutMF final : public OutputModule
{
	// Constants.
public:
	static constexpr char const* default_tmpDir{"<parent-path-of-filename>"};

	// Config.
public:
	struct Config
	{
		using Name = fhicl::Name;
		using Comment = fhicl::Comment;
		template<typename T>
		using Atom = fhicl::Atom<T>;
		template<typename T>
		using OptionalAtom = fhicl::OptionalAtom<T>;
		fhicl::TableFragment<OutputModule::Config> omConfig;
		Atom<string> catalog{Name("catalog"), ""};
		OptionalAtom<bool> dropAllEvents{Name("dropAllEvents")};
		Atom<bool> dropAllSubRuns{Name("dropAllSubRuns"), false};
		OptionalAtom<bool> fastCloning{Name("fastCloning")};
		Atom<string> tmpDir{Name("tmpDir"), default_tmpDir};
		Atom<int> compressionLevel{Name("compressionLevel"), 7};
		Atom<unsigned> freePercent{Name("freePercent"), 0};
		Atom<unsigned> freeMB{Name("freeMB"), 0};
		Atom<int64_t> saveMemoryObjectThreshold{Name("saveMemoryObjectThreshold"),
		                                        -1l};
		Atom<int64_t> treeMaxVirtualSize{Name("treeMaxVirtualSize"), -1};
		Atom<int> splitLevel{Name("splitLevel"), 99};
		Atom<int> basketSize{Name("basketSize"), 16384};
		Atom<bool> dropMetaDataForDroppedData{Name("dropMetaDataForDroppedData"),
		                                      false};
		Atom<string> dropMetaData{Name("dropMetaData"), "NONE"};
		Atom<bool> writeParameterSets{Name("writeParameterSets"), true};
		fhicl::Table<ClosingCriteria::Config> fileProperties{
		    Name("fileProperties")};
		Atom<int> firstLoggerRank{Name("firstLoggerRank"), -1};
		Atom<unsigned> maxOpenFiles{
		    Name("maxOpenFiles"),
		    5u,
		    Comment("Maximum number of ROOT files that can be open simultaneously.\n"
		            "When this limit is reached, the oldest pending file is flushed\n"
		            "to disk before a new file is opened.  A value of 1 gives the\n"
		            "same behavior as RootDAQOut (no pipelining).  Higher values\n"
		            "allow TFile::Close() of older files to overlap with writing\n"
		            "new events to the current file.")};

		struct NewSubStringForApp
		{
			fhicl::Atom<string> appName{fhicl::Name("appName")};
			fhicl::Atom<string> newString{fhicl::Name("newString")};
		};
		struct FileNameSubstitution
		{
			fhicl::Atom<string> targetString{fhicl::Name("targetString")};
			fhicl::Sequence<fhicl::Table<NewSubStringForApp>> replacementList{fhicl::Name("replacementList")};
		};
		fhicl::OptionalSequence<fhicl::Table<FileNameSubstitution>> fileNameSubstitutions{Name("fileNameSubstitutions")};

		Config()
		{
			// Both RootDAQOutMF module and OutputModule use the "fileName"
			// FHiCL parameter.  However, whereas in OutputModule the
			// parameter has a default, for RootDAQOutMF the parameter should
			// not.  We therefore have to change the default flag setting
			// for 'OutputModule::Config::fileName'.
			using namespace fhicl::detail;
			ParameterBase* adjustFilename{
			    const_cast<fhicl::Atom<string>*>(&omConfig().fileName)};  // NOLINT(cppcoreguidelines-pro-type-const-cast)
			adjustFilename->set_par_style(fhicl::par_style::REQUIRED);
		}

		struct KeysToIgnore
		{
			set<string>
			operator()() const
			{
				set<string> keys{OutputModule::Config::KeysToIgnore::get()};
				keys.insert("results");
				return keys;
			}
		};
	};

	using Parameters = fhicl::WrappedTable<Config, Config::KeysToIgnore>;

	// Special Member Functions.
public:
	~RootDAQOutMF() override;
	explicit RootDAQOutMF(Parameters const& /*config*/);
	RootDAQOutMF(RootDAQOutMF const&) = delete;
	RootDAQOutMF(RootDAQOutMF&&) = delete;
	RootDAQOutMF& operator=(RootDAQOutMF const&) = delete;
	RootDAQOutMF& operator=(RootDAQOutMF&&) = delete;

	// Member Functions.
public:
	void postSelectProducts() override;
	void beginJob() override;
	void endJob() override;
	void beginRun(RunPrincipal const& /*rp*/) override;
	void endRun(RunPrincipal const& /*rp*/) override;
	void beginSubRun(SubRunPrincipal const& /*srp*/) override;
	void endSubRun(SubRunPrincipal const& /*srp*/) override;
	void event(EventPrincipal const& /*ep*/) override;

	// Member Functions -- Replace OutputModule Functions.
private:
	string fileNameAtOpen() const;
	string fileNameAtClose(PostCloseFileRenamer& renamer,
	                       string const& currentFileName);
	string const& lastClosedFileName() const override;
	Granularity fileGranularity() const override;
	void openFile(FileBlock const& /*fb*/) override;
	void respondToOpenInputFile(FileBlock const& /*fb*/) override;
	void readResults(ResultsPrincipal const& resp) override;
	void respondToCloseInputFile(FileBlock const& /*fb*/) override;
	void incrementInputFileNumber() override;
	void write(EventPrincipal& /*ep*/) override;
	void writeSubRun(SubRunPrincipal& /*sr*/) override;
	void writeRun(RunPrincipal& /*rp*/) override;
	void setSubRunAuxiliaryRangeSetID(RangeSet const& /*rs*/) override;
	void setRunAuxiliaryRangeSetID(RangeSet const& /*rs*/) override;
	bool isFileOpen() const override;
	void setFileStatus(OutputFileStatus /*ofs*/) override;
	bool requestsToCloseFile() const override;
	void startEndFile() override;
	void writeFileFormatVersion() override;
	void writeFileIndex() override;
#if ART_HEX_VERSION < 0x31100
	void writeEventHistory() override;
#endif
	void writeProcessConfigurationRegistry() override;
	void writeProcessHistoryRegistry() override;
	void writeParameterSetRegistry() override;
	void writeProductDescriptionRegistry() override;
	void writeParentageRegistry() override;
	void doWriteFileCatalogMetadata(
	    FileCatalogMetadata::collection_type const& md,
	    FileCatalogMetadata::collection_type const& ssmd) override;
	void writeProductDependencies() override;
	void finishEndFile() override;
	void doRegisterProducts(ProductDescriptions& producedProducts,
	                        ModuleDescription const& md) override;
	std::string modifyFilePattern(std::string const& /*inputPattern*/,
	                              Config const& /*config*/);

	// Per-file state bundle (non-copyable, non-movable -- stored via unique_ptr
	// so the PostCloseFileRenamer reference into fstats remains stable).
	struct OutputFileBundle
	{
		FileStatsCollector fstats;
		PostCloseFileRenamer fRenamer;
		std::unique_ptr<RootDAQOutFile> file{nullptr};
		std::string tmpFileName{};
		bool metadataNeedsRefresh{false};

		OutputFileBundle(std::string const& moduleLabel,
		                 std::string const& processName)
		    : fstats(moduleLabel, processName), fRenamer(fstats)
		{}

		OutputFileBundle(OutputFileBundle const&) = delete;
		OutputFileBundle(OutputFileBundle&&) = delete;
		OutputFileBundle& operator=(OutputFileBundle const&) = delete;
		OutputFileBundle& operator=(OutputFileBundle&&) = delete;
	};

	// Member Functions -- Implementation Details.
private:
	void doOpenFile();
	void closePendingFile(std::unique_ptr<OutputFileBundle>& bundle);
	void closeOldestPendingFileIfNeeded();
	void removeBundleMappings(OutputFileBundle* bundle);
	void markLateWrite(OutputFileBundle* bundle);
	OutputFileBundle* targetBundleForEvent(EventPrincipal const& ep);
	OutputFileBundle* targetBundleForSubRun(SubRunPrincipal const& sr);
	OutputFileBundle* targetBundleForRun(RunPrincipal const& rp);

	using SubRunKey = std::pair<unsigned, unsigned>;
	static SubRunKey makeSubRunKey(EventPrincipal const& ep);
	static SubRunKey makeSubRunKey(SubRunPrincipal const& sr);
	static unsigned makeRunKey(EventPrincipal const& ep);
	static unsigned makeRunKey(SubRunPrincipal const& sr);
	static unsigned makeRunKey(RunPrincipal const& rp);

	// Data Members.
private:
	mutable std::recursive_mutex mutex_;
	string const catalog_;
	bool dropAllEvents_{false};
	bool dropAllSubRuns_;
	string const moduleLabel_;
	int inputFileCount_{};

	// Multi-file state.
	// activeFile_: the file currently receiving events.
	// pendingFiles_: files whose writeTTrees() has been called but whose
	//                TFile has not yet been closed (destructed).
	std::unique_ptr<OutputFileBundle> activeFile_{nullptr};
	std::deque<std::unique_ptr<OutputFileBundle>> pendingFiles_;
	unsigned const maxOpenFiles_;
	std::map<SubRunKey, OutputFileBundle*> subRunToBundle_;
	std::map<unsigned, OutputFileBundle*> runToBundle_;
	FileCatalogMetadata::collection_type lastFileCatalogMetadata_{};
	FileCatalogMetadata::collection_type lastSubRunMetadata_{};
	bool hasCatalogMetadata_{false};

	string const filePattern_;
	string tmpDir_;
	string lastClosedFileName_{};
	int const compressionLevel_;
	unsigned freePercent_;
	unsigned freeMB_;
	int64_t const saveMemoryObjectThreshold_;
	int64_t const treeMaxVirtualSize_;
	int const splitLevel_;
	int const basketSize_;
	DropMetaData dropMetaData_;
	bool dropMetaDataForDroppedData_;
	bool fastCloningEnabled_{true};
	// Set false only for cases where we are guaranteed never to need historical
	// ParameterSet information in the downstream file, such as when mixing.
	bool writeParameterSets_;
	ClosingCriteria fileProperties_;
	ProductDescriptions productsToProduce_{};
	ProductTables producedResultsProducts_{ProductTables::invalid()};
	RPManager rpm_;
};

RootDAQOutMF::~RootDAQOutMF() = default;

RootDAQOutMF::RootDAQOutMF(Parameters const& config)
#if ART_HEX_VERSION < 0x31100
    : OutputModule{
          config().omConfig, config.get_PSet()}
#else
    : OutputModule{
          config().omConfig}
#endif
    , catalog_{config().catalog()}
    , dropAllSubRuns_{config().dropAllSubRuns()}
    , moduleLabel_{config.get_PSet().get<string>("module_label")}
    , maxOpenFiles_{config().maxOpenFiles()}
    , filePattern_{modifyFilePattern(config().omConfig().fileName(), config())}
    , tmpDir_{config().tmpDir() == default_tmpDir ? parent_path(filePattern_) : config().tmpDir()}
    , compressionLevel_{config().compressionLevel()}
    , freePercent_{config().freePercent()}
    , freeMB_{config().freeMB()}
    , saveMemoryObjectThreshold_{config().saveMemoryObjectThreshold()}
    , treeMaxVirtualSize_{config().treeMaxVirtualSize()}
    , splitLevel_{config().splitLevel()}
    , basketSize_{config().basketSize()}
    , dropMetaData_{config().dropMetaData()}
    , dropMetaDataForDroppedData_{config().dropMetaDataForDroppedData()}
    , writeParameterSets_{config().writeParameterSets()}
    , fileProperties_{(detail::validateFileNamePattern(config.get_PSet().has_key(config().fileProperties.name()),
                                                       filePattern_),  // comma operator!
                       config().fileProperties())}
    , rpm_{config.get_PSet()}
{
	TLOG(TLVL_INFO) << "RootDAQOutMF_module (s81 version) CONSTRUCTOR Start";
	if (maxOpenFiles_ == 0)
	{
		throw Exception(errors::Configuration)  // NOLINT(cert-err60-cpp)
		    << "RootDAQOutMF: maxOpenFiles must be >= 1.\n";
	}

	// Setup the streamers and error handlers.
	root::setup();

	bool const dropAllEventsSet{config().dropAllEvents(dropAllEvents_)};
	dropAllEvents_ = detail::shouldDropEvents(
	    dropAllEventsSet, dropAllEvents_, dropAllSubRuns_);
	// N.B. Any time file switching is enabled at a boundary other than
	//      InputFile, fastCloningEnabled_ ***MUST*** be deactivated.  This is
	//      to ensure that the Event tree from the InputFile is not
	//      accidentally cloned to the output file before the output
	//      module has seen the events that are going to be processed.
	bool const fastCloningSet{config().fastCloning(fastCloningEnabled_)};
	fastCloningEnabled_ = RootDAQOutFile::shouldFastClone(
	    fastCloningSet, fastCloningEnabled_, wantAllEvents(), fileProperties_);
	if (!writeParameterSets_)
	{
		mf::LogWarning("PROVENANCE")
		    << "Output module " << moduleLabel_
		    << " has parameter writeParameterSets set to false.\n"
		    << "Parameter set provenance will not be available in subsequent "
		       "jobs.\n"
		    << "Check your experiment's policy on this issue to avoid future "
		       "problems\n"
		    << "with analysis reproducibility.\n";
	}
}

void RootDAQOutMF::openFile(FileBlock const& fb)
{
	std::lock_guard sentry{mutex_};
	// Note: The file block here refers to the currently open
	//       input file, so we can find out about the available
	//       products by looping over the branches of the input
	//       file data trees.
	if (!isFileOpen())
	{
		// Close oldest pending file if opening a new one would exceed the limit.
		closeOldestPendingFileIfNeeded();
		doOpenFile();
		respondToOpenInputFile(fb);
	}
}

void RootDAQOutMF::postSelectProducts()
{
	std::lock_guard sentry{mutex_};
	if (isFileOpen())
	{
		activeFile_->file->selectProducts();
	}
}

void RootDAQOutMF::respondToOpenInputFile(FileBlock const& fb)
{
	std::lock_guard sentry{mutex_};
	++inputFileCount_;
	if (!isFileOpen())
	{
		return;
	}
	auto const* rfb = dynamic_cast<RootFileBlock const*>(&fb);
	bool fastCloneThisOne = fastCloningEnabled_ && (rfb != nullptr) &&
	                        (rfb->tree() != nullptr);
	if (fastCloningEnabled_ && !fastCloneThisOne)
	{
		mf::LogWarning("FastCloning")
		    << "Fast cloning deactivated for this input file due to "
		    << "empty event tree and/or event limits.";
	}
	if (fastCloneThisOne && !rfb->fastClonable())
	{
		mf::LogWarning("FastCloning")
		    << "Fast cloning deactivated for this input file due to "
		    << "information in FileBlock.";
		fastCloneThisOne = false;
	}
	activeFile_->file->beginInputFile(rfb, fastCloneThisOne);
	activeFile_->fstats.recordInputFile(fb.fileName());
}

void RootDAQOutMF::readResults(ResultsPrincipal const& resp)
{
	std::lock_guard sentry{mutex_};
	rpm_.for_each_RPWorker(
	    [&resp](RPWorker& w) { w.rp().doReadResults(resp); });
}

void RootDAQOutMF::respondToCloseInputFile(FileBlock const& fb)
{
	std::lock_guard sentry{mutex_};
	if (isFileOpen())
	{
		activeFile_->file->respondToCloseInputFile(fb);
	}
}

void RootDAQOutMF::write(EventPrincipal& ep)
{
	std::lock_guard sentry{mutex_};
	if (dropAllEvents_)
	{
		return;
	}
	if (hasNewlyDroppedBranch()[InEvent])
	{
		ep.addToProcessHistory();
	}
	auto* bundle = targetBundleForEvent(ep);
	bundle->file->writeOne(ep);
	bundle->fstats.recordEvent(ep.eventID());
	subRunToBundle_[makeSubRunKey(ep)] = bundle;
	runToBundle_[makeRunKey(ep)] = bundle;
}

void RootDAQOutMF::setSubRunAuxiliaryRangeSetID(RangeSet const& rs)
{
	std::lock_guard sentry{mutex_};
	if (activeFile_)
	{
		activeFile_->file->setSubRunAuxiliaryRangeSetID(rs);
	}
	for (auto const& bundle : pendingFiles_)
	{
		bundle->file->setSubRunAuxiliaryRangeSetID(rs);
	}
}

void RootDAQOutMF::writeSubRun(SubRunPrincipal& sr)
{
	std::lock_guard sentry{mutex_};
	if (dropAllSubRuns_)
	{
		return;
	}
	if (hasNewlyDroppedBranch()[InSubRun])
	{
		sr.addToProcessHistory();
	}
	auto* bundle = targetBundleForSubRun(sr);
	bundle->file->writeSubRun(sr);
	bundle->fstats.recordSubRun(sr.subRunID());
	subRunToBundle_[makeSubRunKey(sr)] = bundle;
	runToBundle_[makeRunKey(sr)] = bundle;
}

void RootDAQOutMF::setRunAuxiliaryRangeSetID(RangeSet const& rs)
{
	std::lock_guard sentry{mutex_};
	if (activeFile_)
	{
		activeFile_->file->setRunAuxiliaryRangeSetID(rs);
	}
	for (auto const& bundle : pendingFiles_)
	{
		bundle->file->setRunAuxiliaryRangeSetID(rs);
	}
}

void RootDAQOutMF::writeRun(RunPrincipal& rp)
{
	std::lock_guard sentry{mutex_};
	if (hasNewlyDroppedBranch()[InRun])
	{
		rp.addToProcessHistory();
	}
	auto* bundle = targetBundleForRun(rp);
	bundle->file->writeRun(rp);
	bundle->fstats.recordRun(rp.runID());
	runToBundle_[makeRunKey(rp)] = bundle;
}

void RootDAQOutMF::startEndFile()
{
	std::lock_guard sentry{mutex_};
	auto resp = make_unique<ResultsPrincipal>(
	    ResultsAuxiliary{}, moduleDescription().processConfiguration(), nullptr);
	resp->createGroupsForProducedProducts(producedResultsProducts_);
#if ART_HEX_VERSION < 0x31100
	resp->enableLookupOfProducedProducts(producedResultsProducts_);
#else
	resp->enableLookupOfProducedProducts();
#endif
	if (!producedResultsProducts_.descriptions(InResults).empty() ||
	    hasNewlyDroppedBranch()[InResults])
	{
		resp->addToProcessHistory();
	}
	rpm_.for_each_RPWorker(
	    [&resp](RPWorker& w) { w.rp().doWriteResults(*resp); });
	activeFile_->file->writeResults(*resp);
}

void RootDAQOutMF::writeFileFormatVersion()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeFileFormatVersion();
}

void RootDAQOutMF::writeFileIndex()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeFileIndex();
}

#if ART_HEX_VERSION < 0x31100
void RootDAQOutMF::writeEventHistory()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeEventHistory();
}
#endif

void RootDAQOutMF::writeProcessConfigurationRegistry()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeProcessConfigurationRegistry();
}

void RootDAQOutMF::writeProcessHistoryRegistry()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeProcessHistoryRegistry();
}

void RootDAQOutMF::writeParameterSetRegistry()
{
	std::lock_guard sentry{mutex_};
	if (writeParameterSets_)
	{
		activeFile_->file->writeParameterSetRegistry();
	}
}

void RootDAQOutMF::writeProductDescriptionRegistry()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeProductDescriptionRegistry();
}

void RootDAQOutMF::writeParentageRegistry()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeParentageRegistry();
}

void RootDAQOutMF::doWriteFileCatalogMetadata(
    FileCatalogMetadata::collection_type const& md,
    FileCatalogMetadata::collection_type const& ssmd)
{
	std::lock_guard sentry{mutex_};
	lastFileCatalogMetadata_ = md;
	lastSubRunMetadata_ = ssmd;
	hasCatalogMetadata_ = true;
	activeFile_->metadataNeedsRefresh = false;
	activeFile_->file->writeFileCatalogMetadata(activeFile_->fstats, md, ssmd);
}

void RootDAQOutMF::writeProductDependencies()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeProductDependencies();
}

void RootDAQOutMF::finishEndFile()
{
	std::lock_guard sentry{mutex_};
	string const tmpFileName{activeFile_->file->currentFileName()};

	// Record that this file is closed in its stats, then rename it now.
	// We rename before actually calling TFile::Close() so that the final
	// name is established immediately (art calls lastClosedFileName()
	// after this method returns).  On Linux, rename(2) is safe even
	// while the file descriptor is still open.
	lastClosedFileName_ = fileNameAtClose(activeFile_->fRenamer, tmpFileName);
	TLOG(TLVL_INFO) << __func__ << ": Queued output file \"" << lastClosedFileName_
	                << "\" for deferred TFile::Close() (pendingFiles will have "
	                << pendingFiles_.size() + 1 << " entries)";

	// Move the active bundle to the pending queue.  The TFile is still open
	// in memory; it will be closed (flushed to disk) when closePendingFile()
	// is called.
	pendingFiles_.push_back(std::move(activeFile_));
	activeFile_.reset();  // Mark as "no active file" from art's perspective.

	rpm_.invoke(&ResultsProducer::doClear);
}

void RootDAQOutMF::doRegisterProducts(ProductDescriptions& producedProducts,
                                      ModuleDescription const& md)
{
	std::lock_guard sentry{mutex_};
	// Register Results products from ResultsProducers.
	rpm_.for_each_RPWorker([&producedProducts, &md](RPWorker& w) {
		auto const& params = w.params();
		w.setModuleDescription(
		    ModuleDescription{params.rpPSetID,
		                      params.rpPluginType,
		                      md.moduleLabel() + '#' + params.rpLabel,
		                      ModuleThreadingType::legacy,
		                      md.processConfiguration()});
		w.rp().registerProducts(producedProducts, w.moduleDescription());
	});
	// Form product table for Results products.  We do this here so we
	// can appropriately set the product tables for the
	// ResultsPrincipal.
	productsToProduce_ = producedProducts;
	producedResultsProducts_ = ProductTables{productsToProduce_};
}

void RootDAQOutMF::setFileStatus(OutputFileStatus const ofs)
{
	std::lock_guard sentry{mutex_};
	if (isFileOpen())
	{
		activeFile_->file->setFileStatus(ofs);
	}
}

bool RootDAQOutMF::isFileOpen() const
{
	std::lock_guard sentry{mutex_};
	return activeFile_ != nullptr;
}

void RootDAQOutMF::incrementInputFileNumber()
{
	std::lock_guard sentry{mutex_};
	if (isFileOpen())
	{
		activeFile_->file->incrementInputFileNumber();
	}
}

bool RootDAQOutMF::requestsToCloseFile() const
{
	std::lock_guard sentry{mutex_};
	return isFileOpen() ? activeFile_->file->requestsToCloseFile() : false;
}

Granularity
RootDAQOutMF::fileGranularity() const
{
	std::lock_guard sentry{mutex_};
	return fileProperties_.granularity();
}

void RootDAQOutMF::doOpenFile()
{
	std::lock_guard sentry{mutex_};
	if (inputFileCount_ == 0)
	{
		throw Exception(errors::LogicError)  // NOLINT(cert-err60-cpp)
		    << "Attempt to open output file before input file. "
		    << "Please report this to the core framework developers.\n";
	}
	activeFile_ = std::make_unique<OutputFileBundle>(moduleLabel_, processName());
	activeFile_->file = make_unique<RootDAQOutFile>(this,
	                                                fileNameAtOpen(),
	                                                fileProperties_,
	                                                compressionLevel_,
	                                                freePercent_,
	                                                freeMB_,
	                                                saveMemoryObjectThreshold_,
	                                                treeMaxVirtualSize_,
	                                                splitLevel_,
	                                                basketSize_,
	                                                dropMetaData_,
	                                                dropMetaDataForDroppedData_,
	                                                fastCloningEnabled_);
	activeFile_->fstats.recordFileOpen();
	TLOG(TLVL_INFO) << __func__ << ": Opened output file with pattern \"" << filePattern_ << "\"";
}

void RootDAQOutMF::closePendingFile(std::unique_ptr<OutputFileBundle>& bundle)
{
	removeBundleMappings(bundle.get());
	if (bundle->metadataNeedsRefresh && hasCatalogMetadata_)
	{
		bundle->file->writeFileCatalogMetadata(
		    bundle->fstats, lastFileCatalogMetadata_, lastSubRunMetadata_);
	}
	bundle->fstats.recordFileClose();
	bundle->file->writeTTrees();
	TLOG(TLVL_INFO) << __func__ << ": Closing pending file (TFile::Close)";
	bundle->file.reset();
	bundle.reset();
}

void RootDAQOutMF::closeOldestPendingFileIfNeeded()
{
	std::lock_guard sentry{mutex_};
	// Total open files after opening a new active = pendingFiles_.size() + 1.
	// Close oldest pending entries until we are within the limit.
	while (!pendingFiles_.empty() &&
	       (pendingFiles_.size() + 1 > maxOpenFiles_))
	{
		TLOG(TLVL_INFO) << __func__
		                << ": Closing oldest pending file to respect maxOpenFiles="
		                << maxOpenFiles_
		                << " (currently pending=" << pendingFiles_.size() << ")";
		closePendingFile(pendingFiles_.front());
		pendingFiles_.pop_front();
	}
}

void RootDAQOutMF::removeBundleMappings(OutputFileBundle* bundle)
{
	for (auto it = subRunToBundle_.begin(); it != subRunToBundle_.end();)
	{
		if (it->second == bundle)
		{
			it = subRunToBundle_.erase(it);
		}
		else
		{
			++it;
		}
	}
	for (auto it = runToBundle_.begin(); it != runToBundle_.end();)
	{
		if (it->second == bundle)
		{
			it = runToBundle_.erase(it);
		}
		else
		{
			++it;
		}
	}
}

void RootDAQOutMF::markLateWrite(OutputFileBundle* bundle)
{
	if (bundle != activeFile_.get())
	{
		bundle->metadataNeedsRefresh = true;
	}
}

RootDAQOutMF::OutputFileBundle*
RootDAQOutMF::targetBundleForEvent(EventPrincipal const& ep)
{
	auto* bundle = activeFile_.get();
	auto const subRunKey = makeSubRunKey(ep);
	if (auto const it = subRunToBundle_.find(subRunKey); it != subRunToBundle_.end())
	{
		bundle = it->second;
	}
	else if (auto const runIt = runToBundle_.find(makeRunKey(ep)); runIt != runToBundle_.end())
	{
		bundle = runIt->second;
	}
	markLateWrite(bundle);
	return bundle;
}

RootDAQOutMF::OutputFileBundle*
RootDAQOutMF::targetBundleForSubRun(SubRunPrincipal const& sr)
{
	auto* bundle = activeFile_.get();
	if (auto const it = subRunToBundle_.find(makeSubRunKey(sr)); it != subRunToBundle_.end())
	{
		bundle = it->second;
	}
	else if (auto const runIt = runToBundle_.find(makeRunKey(sr)); runIt != runToBundle_.end())
	{
		bundle = runIt->second;
	}
	markLateWrite(bundle);
	return bundle;
}

RootDAQOutMF::OutputFileBundle*
RootDAQOutMF::targetBundleForRun(RunPrincipal const& rp)
{
	auto* bundle = activeFile_.get();
	if (auto const it = runToBundle_.find(makeRunKey(rp)); it != runToBundle_.end())
	{
		bundle = it->second;
	}
	markLateWrite(bundle);
	return bundle;
}

RootDAQOutMF::SubRunKey
RootDAQOutMF::makeSubRunKey(EventPrincipal const& ep)
{
	auto const id = ep.eventID();
	return {static_cast<unsigned>(id.run()), static_cast<unsigned>(id.subRun())};
}

RootDAQOutMF::SubRunKey
RootDAQOutMF::makeSubRunKey(SubRunPrincipal const& sr)
{
	auto const id = sr.subRunID();
	return {static_cast<unsigned>(id.run()), static_cast<unsigned>(id.subRun())};
}

unsigned
RootDAQOutMF::makeRunKey(EventPrincipal const& ep)
{
	return static_cast<unsigned>(ep.eventID().run());
}

unsigned
RootDAQOutMF::makeRunKey(SubRunPrincipal const& sr)
{
	return static_cast<unsigned>(sr.subRunID().run());
}

unsigned
RootDAQOutMF::makeRunKey(RunPrincipal const& rp)
{
	return static_cast<unsigned>(rp.runID().run());
}

string
RootDAQOutMF::fileNameAtOpen() const
{
	return (filePattern_ == dev_null) ? dev_null : unique_filename(tmpDir_ + "/RootDAQOutMF");
}

string
RootDAQOutMF::fileNameAtClose(PostCloseFileRenamer& renamer,
                               std::string const& currentFileName)
{
	return (filePattern_ == dev_null) ? dev_null : renamer.maybeRenameFile(currentFileName, filePattern_);
}

string const&
RootDAQOutMF::lastClosedFileName() const
{
	std::lock_guard sentry{mutex_};
	if (lastClosedFileName_.empty())
	{
		throw Exception(errors::LogicError, "RootDAQOutMF::lastClosedFileName(): ")  // NOLINT(cert-err60-cpp)
		    << "called before meaningful.\n";
	}
	return lastClosedFileName_;
}

void RootDAQOutMF::beginJob()
{
	std::lock_guard sentry{mutex_};
	rpm_.invoke(&ResultsProducer::doBeginJob);
}

void RootDAQOutMF::endJob()
{
	std::lock_guard sentry{mutex_};
	// Close any files still in the pending queue (TFile::Close not yet called).
	TLOG(TLVL_INFO) << __func__ << ": Closing " << pendingFiles_.size()
	                << " pending file(s) at end of job";
	while (!pendingFiles_.empty())
	{
		closePendingFile(pendingFiles_.front());
		pendingFiles_.pop_front();
	}
	rpm_.invoke(&ResultsProducer::doEndJob);
}

void RootDAQOutMF::event(EventPrincipal const& ep)
{
	std::lock_guard sentry{mutex_};
	rpm_.for_each_RPWorker([&ep](RPWorker& w) { w.rp().doEvent(ep); });
}

void RootDAQOutMF::beginSubRun(SubRunPrincipal const& srp)
{
	std::lock_guard sentry{mutex_};
	rpm_.for_each_RPWorker([&srp](RPWorker& w) { w.rp().doBeginSubRun(srp); });
}

void RootDAQOutMF::endSubRun(SubRunPrincipal const& srp)
{
	std::lock_guard sentry{mutex_};
	rpm_.for_each_RPWorker([&srp](RPWorker& w) { w.rp().doEndSubRun(srp); });
}

void RootDAQOutMF::beginRun(RunPrincipal const& rp)
{
	std::lock_guard sentry{mutex_};
	rpm_.for_each_RPWorker([&rp](RPWorker& w) { w.rp().doBeginRun(rp); });
}

void RootDAQOutMF::endRun(RunPrincipal const& rp)
{
	std::lock_guard sentry{mutex_};
	rpm_.for_each_RPWorker([&rp](RPWorker& w) { w.rp().doEndRun(rp); });
}

std::string
RootDAQOutMF::modifyFilePattern(std::string const& inputPattern, Config const& config)
{
	// Make sure that the shared memory is connected
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;

	TLOG(TLVL_DEBUG + 32) << __func__ << ": inputPattern=\"" << inputPattern << "\"";

	// fetch the firstLoggerRank and fileNameSubstitutions (if provided) for use in
	// substituting keywords in the filename pattern
	int firstLoggerRank = config.firstLoggerRank();
	std::vector<Config::FileNameSubstitution> subs;
	config.fileNameSubstitutions(subs);
	TLOG(TLVL_DEBUG + 33) << __func__ << ": firstLoggerRank=" << firstLoggerRank
	                      << ", numberOfSubstitutionsProvided=" << subs.size();

	// initialization
	std::string modifiedPattern = inputPattern;
	std::string searchString;
	size_t targetLocation;
	int zeroBasedRelativeRank = my_rank;
	int oneBasedRelativeRank = my_rank + 1;
	if (firstLoggerRank >= 0)
	{
		zeroBasedRelativeRank -= firstLoggerRank;
		oneBasedRelativeRank -= firstLoggerRank;
	}
	TLOG(TLVL_DEBUG + 33) << __func__ << ": my_rank=" << my_rank << ", zeroBasedRelativeRank=" << zeroBasedRelativeRank
	                      << ", oneBasedRelativeRank=" << oneBasedRelativeRank;

	// if the "ZeroBasedRelativeRank" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${ZeroBasedRelativeRank}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << zeroBasedRelativeRank;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	}

	// if the "OneBasedRelativeRank" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${OneBasedRelativeRank}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << oneBasedRelativeRank;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	}

	// if the "Rank" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${Rank}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << my_rank;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	}

	// if the "app_name" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${app_name}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << artdaq::Globals::app_name_;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	}

	// if one or more free-form substitutions were provided, we'll do them here
	for (auto& sub : subs)
	{
		// first look up the replacement string for this process's app_name
		const std::string BLAH = "none_provided";
		std::string newString = BLAH;
		std::vector<Config::NewSubStringForApp> replacementList = sub.replacementList();
		for (auto& rdx : replacementList)
		{
			if (rdx.appName() == artdaq::Globals::app_name_)
			{
				newString = rdx.newString();
				break;
			}
		}
		TLOG(TLVL_DEBUG + 33) << __func__ << ": app_name=" << artdaq::Globals::app_name_ << ", newString=" << newString;
		if (newString != BLAH)
		{
			// first, add the expected surrounding text, and search for that
			searchString = "${" + sub.targetString() + "}";
			targetLocation = modifiedPattern.find(searchString);
			TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			while (targetLocation != std::string::npos)
			{
				modifiedPattern.replace(targetLocation, searchString.length(), newString);
				targetLocation = modifiedPattern.find(searchString);
				TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			}

			// then, search for the provided string, verbatim, in case the user specified
			// the enclosing text in the configuration document
			searchString = sub.targetString();
			targetLocation = modifiedPattern.find(searchString);
			TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			while (targetLocation != std::string::npos)
			{
				modifiedPattern.replace(targetLocation, searchString.length(), newString);
				targetLocation = modifiedPattern.find(searchString);
				TLOG(TLVL_DEBUG + 33) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			}
		}
	}

	TLOG(TLVL_DEBUG + 32) << __func__ << ": modifiedPattern = \"" << modifiedPattern << "\"";
	return modifiedPattern;
}

}  // namespace art

DEFINE_ART_MODULE(art::RootDAQOutMF)  // NOLINT(performance-unnecessary-value-param)
