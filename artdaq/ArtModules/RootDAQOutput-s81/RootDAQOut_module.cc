// vim: set sw=2 expandtab :

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
#include "artdaq/ArtModules/ArtdaqSharedMemoryService.h"
#include "artdaq/ArtModules/RootDAQOutput-s81/RootDAQOutFile.h"
#include "artdaq/DAQdata/Globals.hh"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "canvas/Persistency/Provenance/ProductTables.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/ConfigurationTable.h"
#include "fhiclcpp/types/OptionalAtom.h"
#include "fhiclcpp/types/OptionalSequence.h"
#include "fhiclcpp/types/Table.h"
#include "hep_concurrency/RecursiveMutex.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "tracemf.h"  // TLOG
#define TRACE_NAME (app_name + "_RootDAQOut").c_str()

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

class RootDAQOut final : public OutputModule
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
			// Both RootDAQOut module and OutputModule use the "fileName"
			// FHiCL parameter.  However, whereas in OutputModule the
			// parameter has a default, for RootDAQOut the parameter should
			// not.  We therefore have to change the default flag setting
			// for 'OutputModule::Config::fileName'.
			using namespace fhicl::detail;
			ParameterBase* adjustFilename{
			    const_cast<fhicl::Atom<string>*>(&omConfig().fileName)}; // NOLINT(cppcoreguidelines-pro-type-const-cast)
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
	~RootDAQOut() override;
	explicit RootDAQOut(Parameters const& /*config*/);
	RootDAQOut(RootDAQOut const&) = delete;
	RootDAQOut(RootDAQOut&&) = delete;
	RootDAQOut& operator=(RootDAQOut const&) = delete;
	RootDAQOut& operator=(RootDAQOut&&) = delete;

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
	string fileNameAtClose(string const& currentFileName);
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
	void writeEventHistory() override;
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
	std::string modifyFilePattern(std::string const& /*inputPattern*/, Config const& /*config*/);

	// Member Functions -- Implementation Details.
private:
	void doOpenFile();

	// Data Members.
private:
	mutable RecursiveMutex mutex_{"RootDAQOut::mutex"};
	string const catalog_;
	bool dropAllEvents_{false};
	bool dropAllSubRuns_;
	string const moduleLabel_;
	int inputFileCount_{};
	unique_ptr<RootDAQOutFile> rootOutputFile_{nullptr};
	FileStatsCollector fstats_;
	PostCloseFileRenamer fRenamer_;
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

RootDAQOut::~RootDAQOut() = default;

RootDAQOut::RootDAQOut(Parameters const& config)
    : OutputModule{config().omConfig, config.get_PSet()}
    , catalog_{config().catalog()}
    , dropAllSubRuns_{config().dropAllSubRuns()}
    , moduleLabel_{config.get_PSet().get<string>("module_label")}
    , fstats_{moduleLabel_, processName()}
    , fRenamer_{fstats_}
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
    , fileProperties_{(
          detail::validateFileNamePattern(
              config.get_PSet().has_key(config().fileProperties.name()),
              filePattern_),  // comma operator!
          config().fileProperties())}
    , rpm_{config.get_PSet()}
{
	TLOG(TLVL_INFO) << "RootDAQOut_module (s81 version) CONSTRUCTOR Start";
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

void RootDAQOut::openFile(FileBlock const& fb)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	// Note: The file block here refers to the currently open
	//       input file, so we can find out about the available
	//       products by looping over the branches of the input
	//       file data trees.
	if (!isFileOpen())
	{
		doOpenFile();
		respondToOpenInputFile(fb);
	}
}

void RootDAQOut::postSelectProducts()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (isFileOpen())
	{
		rootOutputFile_->selectProducts();
	}
}

void RootDAQOut::respondToOpenInputFile(FileBlock const& fb)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	++inputFileCount_;
	if (!isFileOpen())
	{
		return;
	}
	auto const* rfb = dynamic_cast<RootFileBlock const*>(&fb);
	bool fastCloneThisOne = fastCloningEnabled_ && (rfb != nullptr) &&
	                        (rfb->tree() != nullptr) &&
	                        ((remainingEvents() < 0) ||
	                         (remainingEvents() >= rfb->tree()->GetEntries()));
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
	rootOutputFile_->beginInputFile(rfb, fastCloneThisOne);
	fstats_.recordInputFile(fb.fileName());
}

void RootDAQOut::readResults(ResultsPrincipal const& resp)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.for_each_RPWorker(
	    [&resp](RPWorker& w) { w.rp().doReadResults(resp); });
}

void RootDAQOut::respondToCloseInputFile(FileBlock const& fb)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (isFileOpen())
	{
		rootOutputFile_->respondToCloseInputFile(fb);
	}
}

void RootDAQOut::write(EventPrincipal& ep)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (dropAllEvents_)
	{
		return;
	}
	if (hasNewlyDroppedBranch()[InEvent])
	{
		ep.addToProcessHistory();
	}
	rootOutputFile_->writeOne(ep);
	fstats_.recordEvent(ep.eventID());
}

void RootDAQOut::setSubRunAuxiliaryRangeSetID(RangeSet const& rs)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->setSubRunAuxiliaryRangeSetID(rs);
}

void RootDAQOut::writeSubRun(SubRunPrincipal& sr)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (dropAllSubRuns_)
	{
		return;
	}
	if (hasNewlyDroppedBranch()[InSubRun])
	{
		sr.addToProcessHistory();
	}
	rootOutputFile_->writeSubRun(sr);
	fstats_.recordSubRun(sr.subRunID());
}

void RootDAQOut::setRunAuxiliaryRangeSetID(RangeSet const& rs)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->setRunAuxiliaryRangeSetID(rs);
}

void RootDAQOut::writeRun(RunPrincipal& rp)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (hasNewlyDroppedBranch()[InRun])
	{
		rp.addToProcessHistory();
	}
	rootOutputFile_->writeRun(rp);
	fstats_.recordRun(rp.runID());
}

void RootDAQOut::startEndFile()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	auto resp = make_unique<ResultsPrincipal>(
	    ResultsAuxiliary{}, moduleDescription().processConfiguration(), nullptr);
	resp->createGroupsForProducedProducts(producedResultsProducts_);
	resp->enableLookupOfProducedProducts(producedResultsProducts_);
	if (!producedResultsProducts_.descriptions(InResults).empty() ||
	    hasNewlyDroppedBranch()[InResults])
	{
		resp->addToProcessHistory();
	}
	rpm_.for_each_RPWorker(
	    [&resp](RPWorker& w) { w.rp().doWriteResults(*resp); });
	rootOutputFile_->writeResults(*resp);
}

void RootDAQOut::writeFileFormatVersion()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeFileFormatVersion();
}

void RootDAQOut::writeFileIndex()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeFileIndex();
}

void RootDAQOut::writeEventHistory()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeEventHistory();
}

void RootDAQOut::writeProcessConfigurationRegistry()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeProcessConfigurationRegistry();
}

void RootDAQOut::writeProcessHistoryRegistry()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeProcessHistoryRegistry();
}

void RootDAQOut::writeParameterSetRegistry()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (writeParameterSets_)
	{
		rootOutputFile_->writeParameterSetRegistry();
	}
}

void RootDAQOut::writeProductDescriptionRegistry()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeProductDescriptionRegistry();
}

void RootDAQOut::writeParentageRegistry()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeParentageRegistry();
}

void RootDAQOut::doWriteFileCatalogMetadata(
    FileCatalogMetadata::collection_type const& md,
    FileCatalogMetadata::collection_type const& ssmd)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeFileCatalogMetadata(fstats_, md, ssmd);
}

void RootDAQOut::writeProductDependencies()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rootOutputFile_->writeProductDependencies();
}

void RootDAQOut::finishEndFile()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	string const currentFileName{rootOutputFile_->currentFileName()};
	rootOutputFile_->writeTTrees();
	rootOutputFile_.reset();
	fstats_.recordFileClose();
	lastClosedFileName_ = fileNameAtClose(currentFileName);
	TLOG(TLVL_INFO) << __func__ << ": Closed output file \"" << lastClosedFileName_ << "\"";
	rpm_.invoke(&ResultsProducer::doClear);
}

void RootDAQOut::doRegisterProducts(ProductDescriptions& producedProducts,
                                    ModuleDescription const& md)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
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

void RootDAQOut::setFileStatus(OutputFileStatus const ofs)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (isFileOpen())
	{
		rootOutputFile_->setFileStatus(ofs);
	}
}

bool RootDAQOut::isFileOpen() const
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	return rootOutputFile_ != nullptr;
}

void RootDAQOut::incrementInputFileNumber()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (isFileOpen())
	{
		rootOutputFile_->incrementInputFileNumber();
	}
}

bool RootDAQOut::requestsToCloseFile() const
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	return isFileOpen() ? rootOutputFile_->requestsToCloseFile() : false;
}

Granularity
RootDAQOut::fileGranularity() const
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	return fileProperties_.granularity();
}

void RootDAQOut::doOpenFile()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (inputFileCount_ == 0)
	{
		throw Exception(errors::LogicError) // NOLINT(cert-err60-cpp)
		    << "Attempt to open output file before input file. "
		    << "Please report this to the core framework developers.\n";
	}
	rootOutputFile_ = make_unique<RootDAQOutFile>(this,
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
	fstats_.recordFileOpen();
	TLOG(TLVL_INFO) << __func__ << ": Opened output file with pattern \"" << filePattern_ << "\"";
}

string
RootDAQOut::fileNameAtOpen() const
{
	return (filePattern_ == dev_null) ? dev_null : unique_filename(tmpDir_ + "/RootDAQOut");
}

string
RootDAQOut::fileNameAtClose(std::string const& currentFileName)
{
	return (filePattern_ == dev_null) ? dev_null : fRenamer_.maybeRenameFile(currentFileName, filePattern_);
}

string const&
RootDAQOut::lastClosedFileName() const
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	if (lastClosedFileName_.empty())
	{
		throw Exception(errors::LogicError, "RootDAQOut::currentFileName(): ")  // NOLINT(cert-err60-cpp)
		    << "called before meaningful.\n";
	}
	return lastClosedFileName_;
}

void RootDAQOut::beginJob()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.invoke(&ResultsProducer::doBeginJob);
}

void RootDAQOut::endJob()
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.invoke(&ResultsProducer::doEndJob);
}

void RootDAQOut::event(EventPrincipal const& ep)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.for_each_RPWorker([&ep](RPWorker& w) { w.rp().doEvent(ep); });
}

void RootDAQOut::beginSubRun(SubRunPrincipal const& srp)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.for_each_RPWorker([&srp](RPWorker& w) { w.rp().doBeginSubRun(srp); });
}

void RootDAQOut::endSubRun(SubRunPrincipal const& srp)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.for_each_RPWorker([&srp](RPWorker& w) { w.rp().doEndSubRun(srp); });
}

void RootDAQOut::beginRun(RunPrincipal const& rp)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.for_each_RPWorker([&rp](RPWorker& w) { w.rp().doBeginRun(rp); });
}

void RootDAQOut::endRun(RunPrincipal const& rp)
{
	RecursiveMutexSentry sentry{mutex_, __func__};
	rpm_.for_each_RPWorker([&rp](RPWorker& w) { w.rp().doEndRun(rp); });
}

std::string
RootDAQOut::modifyFilePattern(std::string const& inputPattern, Config const& config)
{
	// Make sure that the shared memory is connected
	art::ServiceHandle<ArtdaqSharedMemoryServiceInterface> shm;

	TLOG(TLVL_DEBUG) << __func__ << ": inputPattern=\"" << inputPattern << "\"";

	// fetch the firstLoggerRank and fileNameSubstitutions (if provided) for use in
	// substituting keywords in the filename pattern
	int firstLoggerRank = config.firstLoggerRank();
	std::vector<Config::FileNameSubstitution> subs;
	config.fileNameSubstitutions(subs);
	TLOG(TLVL_TRACE) << __func__ << ": firstLoggerRank=" << firstLoggerRank
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
	TLOG(TLVL_TRACE) << __func__ << ": my_rank=" << my_rank << ", zeroBasedRelativeRank=" << zeroBasedRelativeRank
	                 << ", oneBasedRelativeRank=" << oneBasedRelativeRank;

	// if the "ZeroBasedRelativeRank" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${ZeroBasedRelativeRank}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << zeroBasedRelativeRank;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	}

	// if the "OneBasedRelativeRank" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${OneBasedRelativeRank}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << oneBasedRelativeRank;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	}

	// if the "Rank" keyword was specified in the filename pattern,
	// perform the substitution
	searchString = "${Rank}";
	targetLocation = modifiedPattern.find(searchString);
	TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
	while (targetLocation != std::string::npos)
	{
		std::ostringstream oss;
		oss << my_rank;
		modifiedPattern.replace(targetLocation, searchString.length(), oss.str());
		targetLocation = modifiedPattern.find(searchString);
		TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
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
		TLOG(TLVL_TRACE) << __func__ << ": app_name=" << artdaq::Globals::app_name_ << ", newString=" << newString;
		if (newString != BLAH)
		{
			// first, add the expected surrounding text, and search for that
			searchString = "${" + sub.targetString() + "}";
			targetLocation = modifiedPattern.find(searchString);
			TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			while (targetLocation != std::string::npos)
			{
				modifiedPattern.replace(targetLocation, searchString.length(), newString);
				targetLocation = modifiedPattern.find(searchString);
				TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			}

			// then, search for the provided string, verbatim, in case the user specified
			// the enclosing text in the configuration document
			searchString = sub.targetString();
			targetLocation = modifiedPattern.find(searchString);
			TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			while (targetLocation != std::string::npos)
			{
				modifiedPattern.replace(targetLocation, searchString.length(), newString);
				targetLocation = modifiedPattern.find(searchString);
				TLOG(TLVL_TRACE) << __func__ << ":" << __LINE__ << " searchString=" << searchString << ", targetLocation=" << targetLocation;
			}
		}
	}

	TLOG(TLVL_DEBUG) << __func__ << ": modifiedPattern = \"" << modifiedPattern << "\"";
	return modifiedPattern;
}

}  // namespace art

DEFINE_ART_MODULE(art::RootDAQOut)// NOLINT(performance-unnecessary-value-param)
