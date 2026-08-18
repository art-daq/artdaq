// vim: set sw=2 expandtab :
#include "TRACE/tracemf.h"  // TLOG
#include "artdaq-utilities/Plugins/MetricData.hh"
#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_RootDAQOutMF").c_str()

#include "artdaq/ArtModules/ArtdaqRunInfoServiceInterface.h"
#include "artdaq/ArtModules/ArtdaqSharedMemoryServiceInterface.h"
#include "artdaq/ArtModules/RootDAQOutFile.h"

#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Core/RPManager.h"
#include "art/Framework/Core/ResultsProducer.h"
#include "art/Framework/IO/ClosingCriteria.h"
#include "art/Framework/IO/FileStatsCollector.h"
#include "art/Framework/IO/PostCloseFileRenamer.h"
#include "art/Framework/IO/detail/SafeFileNameConfig.h"
#include "art/Framework/IO/detail/logFileAction.h"
#include "art/Framework/IO/detail/validateFileNamePattern.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/ResultsPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Utilities/Globals.h"
#include "art/Utilities/parent_path.h"
#include "art/Utilities/unique_filename.h"
#include "art_root_io/DropMetaData.h"
#include "art_root_io/FastCloningEnabled.h"
#include "art_root_io/RootFileBlock.h"
#include "art_root_io/detail/rootOutputConfigurationTools.h"
#include "art_root_io/setup.h"
#include "canvas/Persistency/Provenance/ProductTables.h"
#include "canvas/Utilities/Exception.h"
#include "cetlib_except/exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/OptionalAtom.h"
#include "fhiclcpp/types/OptionalSequence.h"
#include "fhiclcpp/types/Table.h"
#include "fhiclcpp/types/TableFragment.h"
#include "messagefacility/MessageLogger/MessageLogger.h"

#include <unistd.h>

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace std;
using namespace hep::concurrency;

namespace {
string const dev_null{"/dev/null"};

bool maxCriterionSpecified(art::ClosingCriteria const& cc)
{
	auto fp = mem_fn(&art::ClosingCriteria::fileProperties);
	return (fp(cc).nEvents() !=
	        art::ClosingCriteria::Defaults::unsigned_max()) ||
	       (fp(cc).nSubRuns() !=
	        art::ClosingCriteria::Defaults::unsigned_max()) ||
	       (fp(cc).nRuns() != art::ClosingCriteria::Defaults::unsigned_max()) ||
	       (fp(cc).size() != art::ClosingCriteria::Defaults::size_max()) ||
	       (fp(cc).age().count() !=
	        art::ClosingCriteria::Defaults::seconds_max());
}

auto shouldFastClone(bool const fastCloningSet,
                     bool const fastCloning,
                     bool const wantAllEvents,
                     art::ClosingCriteria const& cc)
{
	art::FastCloningEnabled enabled;
	if (fastCloningSet and not fastCloning)
	{
		enabled.disable(
		    "RootDAQOutMF configuration explicitly disables fast cloning.");
		return enabled;
	}

	if (not wantAllEvents)
	{
		enabled.disable(
		    "Event-selection has been specified in the RootDAQOutMF configuration.");
	}
	if (fastCloning && maxCriterionSpecified(cc) &&
	    cc.granularity() < art::Granularity::InputFile)
	{
		enabled.disable(
		    "File-switching has been requested at event, subrun, or "
		    "run boundaries.");
	}
	return enabled;
}

struct SubrunStats
{
	size_t nEvents{0};
	art::EventNumber_t firstEvent{std::numeric_limits<art::EventNumber_t>::max()};
	art::EventNumber_t lastEvent{0};
};

}  // namespace

namespace art {

// RootDAQOutMF is a variant of RootDAQOut that can keep multiple ROOT files
// open at the same time.  When the active file's closing criteria are met (e.g.
// maxEvents, maxSubRuns, maxRuns), it is moved to a "pending-close" queue and a
// new file is opened immediately.  Files in the pending-close queue still have
// their TFile open in memory; they are flushed to disk only when the queue
// would exceed maxOpenFiles.  This pipelining reduces the gap in data writing
// that occurs during file transitions.
class RootDAQOutMF final : public OutputModule
{
public:
	static constexpr char const* default_tmpDir{"<parent-path-of-filename>"};

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
		Atom<int> splitLevel{Name("splitLevel"), 1};
		Atom<int> basketSize{Name("basketSize"), 16384};
		Atom<bool> dropMetaDataForDroppedData{Name("dropMetaDataForDroppedData"),
		                                      false};
		Atom<string> dropMetaData{Name("dropMetaData"), "NONE"};
		Atom<bool> writeParameterSets{Name("writeParameterSets"), true};
		fhicl::Table<ClosingCriteria::Config> fileProperties{
		    Name("fileProperties"),
		    Comment("The 'fileProperties' parameter is specified to enable "
		            "output-file switching.")};
		fhicl::TableFragment<detail::SafeFileNameConfig> safeFileName;
		Atom<int> firstLoggerRank{Name("firstLoggerRank"), -1};
		Atom<unsigned> maxOpenFiles{
		    Name("maxOpenFiles"),
		    Comment("Maximum number of ROOT files that can be open simultaneously.\n"
		            "When this limit is reached, the oldest pending file is flushed\n"
		            "to disk before a new file is opened.  A value of 1 gives the\n"
		            "same behavior as RootDAQOut (no pipelining).  Higher values\n"
		            "allow TFile::Close() of older files to overlap with writing\n"
		            "new events to the current file."),
		    5u};

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
		Atom<string> datastream{Name("datastream"),
		    Comment("Datastream label included in summary records written by\n"
		            "ArtdaqRunInfoServiceInterface (e.g. \"triggered\", \"lumistream\").\n"
		            "The default implementation (ArtdaqRunInfoService) writes CSV files;\n"
		            "experiments can provide their own (e.g. Postgres).\n"
		            "Enable the service in FHiCL:\n"
		            "  services.ArtdaqRunInfoServiceInterface: {\n"
		            "    service_provider: \"ArtdaqRunInfoService\"\n"
		            "    summaryDir: \"/path/to/output\"\n"
		            "  }"),
		    "default"};

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

	~RootDAQOutMF() override;
	explicit RootDAQOutMF(Parameters const&);
	RootDAQOutMF(RootDAQOutMF const&) = delete;
	RootDAQOutMF(RootDAQOutMF&&) = delete;
	RootDAQOutMF& operator=(RootDAQOutMF const&) = delete;
	RootDAQOutMF& operator=(RootDAQOutMF&&) = delete;

	void postSelectProducts() override;
	void beginJob() override;
	void endJob() override;
	void beginRun(RunPrincipal const&) override;
	void endRun(RunPrincipal const&) override;
	void beginSubRun(SubRunPrincipal const&) override;
	void endSubRun(SubRunPrincipal const&) override;
	void event(EventPrincipal const&) override;

private:
	// Replace OutputModule Functions.
	string fileNameAtOpen() const;
	string fileNameAtClose(PostCloseFileRenamer& renamer,
	                       string const& currentFileName);
	string const& lastClosedFileName() const override;
	Granularity fileGranularity() const override;
	void openFile(FileBlock const&) override;
	void respondToOpenInputFile(FileBlock const&) override;
	void readResults(ResultsPrincipal const& resp) override;
	void respondToCloseInputFile(FileBlock const&) override;
	void incrementInputFileNumber() override;
	void write(EventPrincipal&) override;
	void writeSubRun(SubRunPrincipal&) override;
	void writeRun(RunPrincipal&) override;
	void setSubRunAuxiliaryRangeSetID(RangeSet const&) override;
	void setRunAuxiliaryRangeSetID(RangeSet const&) override;
	bool isFileOpen() const override;
	void setFileStatus(OutputFileStatus) override;
	bool requestsToCloseFile() const override;
	void startEndFile() override;
	void writeFileFormatVersion() override;
	void writeFileIndex() override;
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
	void doRegisterProducts(ProductDescriptions& productsToProduce,
	                        ModuleDescription const& md) override;
	std::string modifyFilePattern(std::string const& inputPattern,
	                              Config const& config);

	// Per-file state bundle (non-copyable, non-movable -- stored via unique_ptr
	// so the PostCloseFileRenamer reference into fstats remains stable).
	struct OutputFileBundle
	{
		FileStatsCollector fstats;
		PostCloseFileRenamer fRenamer;
		std::unique_ptr<RootDAQOutFile> file{nullptr};
		std::string tmpFileName{};
		bool metadataNeedsRefresh{false};
		std::string closedFileName{};
		std::map<art::SubRunID, SubrunStats> subrunStats;

		OutputFileBundle(std::string const& moduleLabel,
		                 std::string const& processName)
		    : fstats(moduleLabel, processName), fRenamer(fstats)
		{}

		OutputFileBundle(OutputFileBundle const&) = delete;
		OutputFileBundle(OutputFileBundle&&) = delete;
		OutputFileBundle& operator=(OutputFileBundle const&) = delete;
		OutputFileBundle& operator=(OutputFileBundle&&) = delete;
	};

	// Implementation Details.
	void doOpenFile();
	void closePendingFile(std::unique_ptr<OutputFileBundle>& bundle);
	void closeOldestPendingFileIfNeeded();
	void removeBundleMappings(OutputFileBundle* bundle);
	void markLateWrite(OutputFileBundle* bundle);
	OutputFileBundle* targetBundleForEvent(EventPrincipal const& ep);
	OutputFileBundle* targetBundleForSubRun(SubRunPrincipal const& sr);
	OutputFileBundle* targetBundleForRun(RunPrincipal const& rp);

	using RoutingKey = std::tuple<unsigned, unsigned, unsigned>;
	using SubRunIDKey = std::pair<unsigned, unsigned>;
	unsigned bucketIndex(unsigned idValue, unsigned maxPerFile) const;
	RoutingKey makeRoutingKey(EventPrincipal const& ep) const;
	RoutingKey makeRoutingKey(SubRunPrincipal const& sr) const;
	RoutingKey makeRoutingKey(RunPrincipal const& rp) const;
	static SubRunIDKey makeSubRunIDKey(EventPrincipal const& ep);
	static SubRunIDKey makeSubRunIDKey(SubRunPrincipal const& sr);
	static unsigned makeRunIDKey(SubRunPrincipal const& sr);
	static unsigned makeRunIDKey(RunPrincipal const& rp);

	// Data Members.
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
	std::map<RoutingKey, OutputFileBundle*> routeToBundle_;
	std::map<SubRunIDKey, std::set<OutputFileBundle*>> subRunToBundles_;
	std::map<unsigned, std::set<OutputFileBundle*>> runToBundles_;
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
	FastCloningEnabled fastCloningEnabled_{};
	// Set false only for cases where we are guaranteed never to need historical
	// ParameterSet information in the downstream file, such as when mixing.
	bool writeParameterSets_;
	ClosingCriteria fileProperties_;
	string datastream_;
	ArtdaqRunInfoServiceInterface* runInfoService_{nullptr};
	size_t filesOpenedInRun_{0};
	size_t filesClosedInRun_{0};
	// Shared %# sequence counter across all OutputFileBundle instances.
	// Key = intermediate filename with sentinel in place of the index.
	std::map<std::string, size_t> sharedFileIndex_;
	ProductDescriptions productsToProduce_{};
	ProductTables producedResultsProducts_{ProductTables::invalid()};
	RPManager rpm_;
};

RootDAQOutMF::~RootDAQOutMF()
{
	TLOG(TLVL_INFO) << "RootDAQOutMF DESTRUCTOR this=" << static_cast<void const*>(this)
	                << " label=" << moduleLabel_
	                << " filesOpenedInRun_=" << filesOpenedInRun_
	                << " filesClosedInRun_=" << filesClosedInRun_
	                << " activeFile=" << (activeFile_ ? "non-null" : "null")
	                << " pendingFiles=" << pendingFiles_.size();

	if (activeFile_)
	{
		TLOG(TLVL_WARNING) << "RootDAQOutMF DESTRUCTOR: active output file still present at destruction. "
		                   << "This indicates shutdown before normal end-file finalization.";
	}

	// Best-effort fallback: if endJob is bypassed but destruction proceeds
	// normally, close any pending files so metadata/trees are flushed.
	// This must never throw from a destructor.
	try
	{
		std::lock_guard sentry{mutex_};
		while (!pendingFiles_.empty())
		{
			closePendingFile(pendingFiles_.front());
			pendingFiles_.pop_front();
		}
	}
	catch (cet::exception const& ex)
	{
		TLOG(TLVL_ERROR) << "RootDAQOutMF DESTRUCTOR: exception while closing pending files: "
		                 << ex.what();
	}
	catch (std::exception const& ex)
	{
		TLOG(TLVL_ERROR) << "RootDAQOutMF DESTRUCTOR: std::exception while closing pending files: "
		                 << ex.what();
	}
	catch (...)
	{
		TLOG(TLVL_ERROR) << "RootDAQOutMF DESTRUCTOR: unknown exception while closing pending files";
	}
}

RootDAQOutMF::RootDAQOutMF(Parameters const& config)
    : OutputModule{
          config().omConfig}
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
    , fileProperties_{config().fileProperties()}
    , datastream_{config().datastream()}
    , rpm_{config.get_PSet()}
{
	TLOG(TLVL_INFO) << "RootDAQOutMF_module (s124 version) CONSTRUCTOR Start this=" << static_cast<void const*>(this)
	                << " label=" << moduleLabel_;

	if (maxOpenFiles_ == 0)
	{
		throw Exception(errors::Configuration)  // NOLINT(cert-err60-cpp)
		    << "RootDAQOutMF: maxOpenFiles must be >= 1.\n";
	}

	bool const check_filename = config.get_PSet().has_key("fileProperties") and
	                            config().safeFileName().checkFileName();
	detail::validateFileNamePattern(check_filename, filePattern_);

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
	bool fastCloningEnabled{true};
	bool const fastCloningSet{config().fastCloning(fastCloningEnabled)};
	fastCloningEnabled_ = shouldFastClone(
	    fastCloningSet, fastCloningEnabled, wantAllEvents(), fileProperties_);

	if (auto const n = Globals::instance()->nschedules(); n > 1)
	{
		std::ostringstream oss;
		oss << "More than one schedule (" << n << ") is being used.";
		fastCloningEnabled_.disable(oss.str());
	}

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

	// Probe for ArtdaqRunInfoServiceInterface — writes per-subrun and per-file
	// summary records at file close.  See the "datastream" Config comment above.
	try
	{
		art::ServiceHandle<ArtdaqRunInfoServiceInterface> svc;
		runInfoService_ = &*svc;
		TLOG(TLVL_INFO) << "RootDAQOutMF: ArtdaqRunInfoServiceInterface available";
	}
	catch (art::Exception const&)
	{
		runInfoService_ = nullptr;
		TLOG(TLVL_INFO) << "RootDAQOutMF: ArtdaqRunInfoServiceInterface not configured, summary records disabled";
	}
}

void RootDAQOutMF::openFile(FileBlock const& fb)
{
	std::lock_guard sentry{mutex_};
	TLOG(TLVL_DEBUG) << __func__ << ": entered, isFileOpen=" << isFileOpen()
	                 << ", pendingFiles=" << pendingFiles_.size()
	                 << ", filesOpenedInRun_=" << filesOpenedInRun_;
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
	else
	{
		TLOG(TLVL_DEBUG) << __func__ << ": skipped doOpenFile because file already open";
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
	auto fastCloneThisOne = fastCloningEnabled_;
	if (!rfb)
	{
		fastCloneThisOne.disable("Input source does not read art/ROOT files.");
	}
	else
	{
		fastCloneThisOne.merge(rfb->fastClonable());
	}
	activeFile_->file->beginInputFile(rfb, std::move(fastCloneThisOne));
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
		ep.refreshProcessHistoryID();
	}
	auto* bundle = targetBundleForEvent(ep);
	bundle->file->writeOne(ep);
	bundle->fstats.recordEvent(ep.eventID());
	auto const& eid = ep.eventID();
	auto& sr = bundle->subrunStats[eid.subRunID()];
	++sr.nEvents;
	if (eid.event() < sr.firstEvent) { sr.firstEvent = eid.event(); }
	if (eid.event() > sr.lastEvent) { sr.lastEvent = eid.event(); }
	routeToBundle_[makeRoutingKey(ep)] = bundle;
	subRunToBundles_[makeSubRunIDKey(ep)].insert(bundle);
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
	auto* const targetBundle = targetBundleForSubRun(sr);
	std::set<OutputFileBundle*> bundlesToWrite{targetBundle};
	if (auto const it = subRunToBundles_.find(makeSubRunIDKey(sr));
	    it != subRunToBundles_.end())
	{
		bundlesToWrite.insert(it->second.begin(), it->second.end());
	}
	for (auto* bundle : bundlesToWrite)
	{
		markLateWrite(bundle);
		bundle->file->writeSubRun(sr);
		bundle->fstats.recordSubRun(sr.subRunID());
		runToBundles_[makeRunIDKey(sr)].insert(bundle);
	}
	routeToBundle_[makeRoutingKey(sr)] = targetBundle;
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
	auto* const targetBundle = targetBundleForRun(rp);
	std::set<OutputFileBundle*> bundlesToWrite{targetBundle};
	if (auto const it = runToBundles_.find(makeRunIDKey(rp));
	    it != runToBundles_.end())
	{
		bundlesToWrite.insert(it->second.begin(), it->second.end());
	}
	for (auto* bundle : bundlesToWrite)
	{
		markLateWrite(bundle);
		bundle->file->writeRun(rp);
		bundle->fstats.recordRun(rp.runID());
	}
	routeToBundle_[makeRoutingKey(rp)] = targetBundle;
}

void RootDAQOutMF::startEndFile()
{
	std::lock_guard sentry{mutex_};
	auto resp = make_unique<ResultsPrincipal>(
	    ResultsAuxiliary{}, moduleDescription().processConfiguration(), nullptr);
	resp->createGroupsForProducedProducts(producedResultsProducts_);
	resp->enableLookupOfProducedProducts();
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
	// Intentionally deferred for RootDAQOutMF:
	// events may still be routed to a pending file after finishEndFile().
	// Writing the index here can therefore miss late events.  We write the
	// FileIndex immediately before writeTTrees() when the pending file is
	// actually being closed in closePendingFile().
}

void RootDAQOutMF::writeProcessConfigurationRegistry()
{
	std::lock_guard sentry{mutex_};
	activeFile_->file->writeProcessConfigurationRegistry();
}

void RootDAQOutMF::writeProcessHistoryRegistry()
{
	std::lock_guard sentry{mutex_};
	// Intentionally deferred for RootDAQOutMF:
	// events may still be routed to a pending file after finishEndFile().
	// Writing ProcessHistory here can therefore miss entries needed by late
	// events. We write ProcessHistory immediately before writeTTrees() when the
	// pending file is actually being closed in closePendingFile().
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
	TLOG(TLVL_DEBUG) << __func__ << ": entered, isFileOpen=" << (activeFile_ != nullptr)
	                 << ", pendingFiles before move=" << pendingFiles_.size()
	                 << ", filesClosedInRun_=" << filesClosedInRun_;
	string const tmpFileName{activeFile_->file->currentFileName()};

	// Record the close before computing the final name so the %# sequence
	// advances the same way it does in RootDAQOut.
	activeFile_->fstats.recordFileClose();

	// Determine the final output file name now, but defer the actual rename
	// until the file has been fully closed.
	activeFile_->tmpFileName = tmpFileName;
	lastClosedFileName_ = (filePattern_ == dev_null)
	                          ? dev_null
	                          : activeFile_->fRenamer.applySubstitutions(
	                                filePattern_);

	activeFile_->closedFileName = lastClosedFileName_;
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
	// can appropriately set the product tables for the ResultsPrincipal.
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
	                                                dropMetaDataForDroppedData_);
	activeFile_->fstats.recordFileOpen();
	++filesOpenedInRun_;
	TLOG(TLVL_DEBUG) << __func__ << ": filesOpenedInRun_ now " << filesOpenedInRun_
	                 << ", metricMan=" << (metricMan ? "non-null" : "NULL");
	if (metricMan)
	{
		metricMan->sendMetric("Output Files Opened", filesOpenedInRun_, "files", 3, artdaq::MetricMode::LastPoint | artdaq::MetricMode::Persist);
	}
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
	// ProcessHistory must be written at true close time so it captures all
	// history information needed by late-routed events.
	bundle->file->writeProcessHistoryRegistry();
	// FileIndex must be written at true close time so it captures all events
	// that may have been routed to this file while it was pending.
	bundle->file->writeFileIndex();
	bundle->file->writeTTrees();
	// Destroying the RootDAQOutFile calls TFile::Close(), which flushes the
	// ROOT key directory and closes the file descriptor.
	TLOG(TLVL_INFO) << __func__ << ": Closing pending file (TFile::Close)";
	bundle->file.reset();
	bundle->closedFileName =
	    fileNameAtClose(bundle->fRenamer, bundle->tmpFileName);
	if (runInfoService_ && !bundle->subrunStats.empty())
	{
		for (auto const& [srid, stats] : bundle->subrunStats)
		{
			runInfoService_->addSubrunRecord(
			    srid.run(), srid.subRun(),
			    stats.nEvents, stats.firstEvent, stats.lastEvent,
			    datastream_);
		}

		art::SubRunNumber_t firstSubrun = bundle->subrunStats.begin()->first.subRun();
		art::SubRunNumber_t lastSubrun  = bundle->subrunStats.rbegin()->first.subRun();
		size_t totalEvents = 0;
		for (auto const& [_, stats] : bundle->subrunStats)
			totalEvents += stats.nEvents;

		size_t fileSize = 0;
		std::error_code ec;
		auto sz = std::filesystem::file_size(bundle->closedFileName, ec);
		if (!ec) fileSize = static_cast<size_t>(sz);

		art::RunNumber_t run = bundle->subrunStats.begin()->first.run();

		std::string dirPath = std::filesystem::path(bundle->closedFileName).parent_path().string();
		char hostname[256] = {};
		gethostname(hostname, sizeof(hostname) - 1);
		std::string metadata = "{\"path\":\"" + dirPath + "\",\"hostname\":\"" + hostname + "\"}";

		runInfoService_->addFileSummary(
		    bundle->closedFileName, run,
		    firstSubrun, lastSubrun, totalEvents, fileSize,
		    metadata, datastream_);
	}
	++filesClosedInRun_;
	TLOG(TLVL_DEBUG) << __func__ << ": filesClosedInRun_ now " << filesClosedInRun_
	                 << ", metricMan=" << (metricMan ? "non-null" : "NULL");
	if (metricMan)
	{
		metricMan->sendMetric("Last Closed Output File", bundle->closedFileName, "", 3, artdaq::MetricMode::LastPoint);
		metricMan->sendMetric("Output Files Closed", filesClosedInRun_, "files", 3, artdaq::MetricMode::LastPoint | artdaq::MetricMode::Persist);
	}
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
	for (auto it = routeToBundle_.begin(); it != routeToBundle_.end();)
	{
		if (it->second == bundle)
		{
			it = routeToBundle_.erase(it);
		}
		else
		{
			++it;
		}
	}
	for (auto it = subRunToBundles_.begin(); it != subRunToBundles_.end();)
	{
		it->second.erase(bundle);
		if (it->second.empty())
		{
			it = subRunToBundles_.erase(it);
		}
		else
		{
			++it;
		}
	}
	for (auto it = runToBundles_.begin(); it != runToBundles_.end();)
	{
		it->second.erase(bundle);
		if (it->second.empty())
		{
			it = runToBundles_.erase(it);
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
	if (auto const it = routeToBundle_.find(makeRoutingKey(ep));
	    it != routeToBundle_.end())
	{
		bundle = it->second;
	}
	markLateWrite(bundle);
	return bundle;
}

RootDAQOutMF::OutputFileBundle*
RootDAQOutMF::targetBundleForSubRun(SubRunPrincipal const& sr)
{
	auto* bundle = activeFile_.get();
	if (auto const it = routeToBundle_.find(makeRoutingKey(sr));
	    it != routeToBundle_.end())
	{
		bundle = it->second;
	}
	markLateWrite(bundle);
	return bundle;
}

RootDAQOutMF::OutputFileBundle*
RootDAQOutMF::targetBundleForRun(RunPrincipal const& rp)
{
	auto* bundle = activeFile_.get();
	if (auto const it = routeToBundle_.find(makeRoutingKey(rp));
	    it != routeToBundle_.end())
	{
		bundle = it->second;
	}
	markLateWrite(bundle);
	return bundle;
}

unsigned
RootDAQOutMF::bucketIndex(unsigned const idValue, unsigned const maxPerFile) const
{
	return (maxPerFile == ClosingCriteria::Defaults::unsigned_max() ||
	        maxPerFile == 0u || idValue == 0u)
	           ? 0u
	           : (idValue - 1u) / maxPerFile;
}

RootDAQOutMF::RoutingKey
RootDAQOutMF::makeRoutingKey(EventPrincipal const& ep) const
{
	auto const limits = fileProperties_.fileProperties();
	auto const id = ep.eventID();
	return {bucketIndex(static_cast<unsigned>(id.run()), limits.nRuns()),
	        bucketIndex(static_cast<unsigned>(id.subRun()), limits.nSubRuns()),
	        bucketIndex(static_cast<unsigned>(id.event()), limits.nEvents())};
}

RootDAQOutMF::RoutingKey
RootDAQOutMF::makeRoutingKey(SubRunPrincipal const& sr) const
{
	auto const limits = fileProperties_.fileProperties();
	auto const id = sr.subRunID();
	return {bucketIndex(static_cast<unsigned>(id.run()), limits.nRuns()),
	        bucketIndex(static_cast<unsigned>(id.subRun()), limits.nSubRuns()),
	        0u};
}

RootDAQOutMF::RoutingKey
RootDAQOutMF::makeRoutingKey(RunPrincipal const& rp) const
{
	auto const limits = fileProperties_.fileProperties();
	return {bucketIndex(static_cast<unsigned>(rp.runID().run()),
	                    limits.nRuns()),
	        0u,
	        0u};
}

RootDAQOutMF::SubRunIDKey
RootDAQOutMF::makeSubRunIDKey(EventPrincipal const& ep)
{
	auto const id = ep.eventID();
	return {static_cast<unsigned>(id.run()), static_cast<unsigned>(id.subRun())};
}

RootDAQOutMF::SubRunIDKey
RootDAQOutMF::makeSubRunIDKey(SubRunPrincipal const& sr)
{
	auto const id = sr.subRunID();
	return {static_cast<unsigned>(id.run()), static_cast<unsigned>(id.subRun())};
}

unsigned
RootDAQOutMF::makeRunIDKey(SubRunPrincipal const& sr)
{
	return static_cast<unsigned>(sr.subRunID().run());
}

unsigned
RootDAQOutMF::makeRunIDKey(RunPrincipal const& rp)
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
                              string const& currentFileName)
{
	if (filePattern_ == dev_null) return dev_null;

	// We need a shared %# index counter across all OutputFileBundle instances,
	// because multiple bundles may write files with the same %r/%s values
	// (same subrun), causing independent per-bundle counters to produce the
	// same destination filename.
	//
	// Strategy:
	//  1. Replace "%N#" in the pattern with a sentinel literal that the renamer
	//     will pass through unchanged (it only substitutes %X tokens).
	//  2. Let the bundle's renamer resolve %r, %s, timestamps, etc., producing
	//     an intermediate filename that contains the sentinel.
	//  3. Use that intermediate name as the key into the module-level shared
	//     counter, increment it, then rename to the final destination.
	static const boost::regex kSeqRe{R"(%([0-9]*)#)"};
	static const std::string kSentinel{"__SEQIDX__"};

	bool const hasSeq = boost::regex_search(filePattern_, kSeqRe);
	if (!hasSeq)
	{
		// No %# in pattern — delegate directly to the bundle's renamer.
		return renamer.maybeRenameFile(currentFileName, filePattern_);
	}

	// Extract optional width from the first %N# occurrence (default 3).
	unsigned width = 3;
	{
		boost::smatch m;
		if (boost::regex_search(filePattern_, m, boost::regex{R"(%([0-9]+)#)"}))
		{
			width = static_cast<unsigned>(std::stoul(m[1].str()));
		}
	}

	// Build a modified pattern with %N# replaced by the sentinel literal.
	std::string const sentinelPattern =
	    boost::regex_replace(filePattern_, kSeqRe, kSentinel);

	// Rename tmpFile → intermediate path (sentinel still present in name).
	// This resolves %r, %s, timestamps, etc. via the bundle's renamer.
	std::string const intermediateName =
	    renamer.maybeRenameFile(currentFileName, sentinelPattern);

	// Assign the next unique index for this base name across all bundles.
	size_t const idx = ++sharedFileIndex_[intermediateName];
	if (idx > 1)
	{
		TLOG(TLVL_INFO) << __func__ << ": shared %# index=" << idx
		                << " for base pattern \"" << intermediateName
		                << "\" (multiple bundles had the same run/subrun)";
	}

	// Build final name by replacing the sentinel with the formatted index.
	std::ostringstream oss;
	oss << std::setfill('0') << std::setw(width) << idx;
	std::string const finalName =
	    boost::regex_replace(intermediateName, boost::regex{kSentinel}, oss.str());

	// Rename intermediate → final (single atomic rename on same filesystem).
	std::error_code ec;
	std::filesystem::rename(intermediateName, finalName, ec);
	if (ec)
	{
		// Cross-filesystem fallback: copy then remove.
		std::filesystem::copy_file(intermediateName, finalName,
		                           std::filesystem::copy_options::overwrite_existing);
		std::filesystem::remove(intermediateName);
	}
	return finalName;
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
