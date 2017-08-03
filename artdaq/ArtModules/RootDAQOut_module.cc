#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/ModuleMacros.h"
#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/Core/ResultsProducer.h"
#include "art/Framework/Core/RPManager.h"
#include "art/Framework/IO/FileStatsCollector.h"
#include "art/Framework/IO/PostCloseFileRenamer.h"
#include "art/Framework/IO/Root/DropMetaData.h"
#include "art/Framework/IO/Root/RootOutputClosingCriteria.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/Event.h"
#include "art/Framework/Principal/ResultsPrincipal.h"
#include "art/Framework/Principal/Results.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/Run.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "art/Framework/Principal/SubRun.h"
#include "art/Persistency/Provenance/ProductMetaData.h"
#include "art/Utilities/ConfigurationTable.h"
#include "art/Utilities/parent_path.h"
#include "art/Utilities/unique_filename.h"

#include "artdaq/ArtModules/RootDAQOutFile.h"
#include "artdaq/ArtModules/detail/rootOutputConfigurationTools.h"
#include "artdaq/ArtModules/detail/logFileAction.h"

#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include "canvas/Utilities/Exception.h"
#include "fhiclcpp/ParameterSet.h"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/OptionalAtom.h"
#include "fhiclcpp/types/Table.h"
#include "messagefacility/MessageLogger/MessageLogger.h"
#define TRACE_NAME "RootDAQOut_module.cc"
#include "trace.h"				// TRACE

#if ART_HEX_VERSION >= 0x20703
# include "Rtypes.h"
# include "TBranchElement.h"
# include "TClass.h"
# include "TFile.h"
# include "TTree.h"
#endif

#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

using std::string;

namespace art {
  class RootDAQOut;
  class RootDAQOutFile;
}

class art::RootDAQOut final : public OutputModule {
public:

  static constexpr char const* default_tmpDir {"<parent-path-of-filename>"};

  struct Config {

    using Name = fhicl::Name;
    using Comment = fhicl::Comment;
    template <typename T> using Atom = fhicl::Atom<T>;
    template <typename T> using OptionalAtom = fhicl::OptionalAtom<T>;

    fhicl::TableFragment<art::OutputModule::Config> omConfig;
    Atom<std::string> catalog { Name("catalog"), "" };
    OptionalAtom<bool> dropAllEvents { Name("dropAllEvents") };
    Atom<bool> dropAllSubRuns { Name("dropAllSubRuns"), false };
    OptionalAtom<bool> fastCloning { Name("fastCloning") };
    Atom<std::string> tmpDir { Name("tmpDir"), default_tmpDir };
    Atom<int> compressionLevel { Name("compressionLevel"), 7 };
    Atom<int64_t> saveMemoryObjectThreshold { Name("saveMemoryObjectThreshold"), -1l };
    Atom<int64_t> treeMaxVirtualSize { Name("treeMaxVirtualSize"), -1 };
    Atom<int> splitLevel { Name("splitLevel"), 99 };
    Atom<int> basketSize { Name("basketSize"), 16384 };
    Atom<bool> dropMetaDataForDroppedData { Name("dropMetaDataForDroppedData"), false };
    Atom<std::string> dropMetaData { Name("dropMetaData"), "NONE" };
    Atom<bool> writeParameterSets { Name("writeParameterSets"), true };
    fhicl::Table<ClosingCriteria::Config> fileProperties { Name("fileProperties") };

    Config()
    {
      // Both RootDAQOut module and OutputModule use the "fileName"
      // FHiCL parameter.  However, whereas in OutputModule the
      // parameter has a default, for RootDAQOut the parameter should
      // not.  We therefore have to change the default flag setting
      // for 'OutputModule::Config::fileName'.
      using namespace fhicl::detail;
      ParameterBase* adjustFilename {const_cast<fhicl::Atom<std::string>*>(&omConfig().fileName)};
      adjustFilename->set_value_type(fhicl::value_type::REQUIRED);
    }

    struct KeysToIgnore {
      std::set<std::string> operator()() const
      {
        std::set<std::string> keys {art::OutputModule::Config::KeysToIgnore::get()};
        keys.insert("results");
        return keys;
      }
    };

  };

  using Parameters = art::WrappedTable<Config,Config::KeysToIgnore>;

  explicit RootDAQOut(Parameters const&);

  void postSelectProducts(FileBlock const&) override;

  void beginJob() override;
  void endJob() override;

  void event(EventPrincipal const&) override;

  void beginSubRun(SubRunPrincipal const&) override;
  void endSubRun(SubRunPrincipal const&) override;

  void beginRun(RunPrincipal const&) override;
  void endRun(RunPrincipal const&) override;

private:

  std::string const& lastClosedFileName() const override;
  void openFile(FileBlock const&) override;
  void respondToOpenInputFile(FileBlock const&) override;
  void readResults(ResultsPrincipal const& resp) override;
  void respondToCloseInputFile(FileBlock const&) override;
  void incrementInputFileNumber() override;
# if ART_HEX_VERSION < 0x20703
  Boundary fileSwitchBoundary() const override;
# endif
  void write(EventPrincipal&) override;
  void writeSubRun(SubRunPrincipal&) override;
  void writeRun(RunPrincipal&) override;
  void setSubRunAuxiliaryRangeSetID(RangeSet const&) override;
  void setRunAuxiliaryRangeSetID(RangeSet const&) override;
  bool isFileOpen() const override;
  void setFileStatus(OutputFileStatus) override;
  bool requestsToCloseFile() const override;
  void doOpenFile();
  void startEndFile() override;
  void writeFileFormatVersion() override;
  void writeFileIndex() override;
  void writeEventHistory() override;
  void writeProcessConfigurationRegistry() override;
  void writeProcessHistoryRegistry() override;
  void writeParameterSetRegistry() override;
  void writeProductDescriptionRegistry() override;
  void writeParentageRegistry() override;
  void writeBranchIDListRegistry() override;
  void
  doWriteFileCatalogMetadata(FileCatalogMetadata::collection_type const& md,
                             FileCatalogMetadata::collection_type const& ssmd)
                             override;
  void writeProductDependencies() override;
  void finishEndFile() override;
  void doRegisterProducts(MasterProductRegistry& mpr,
                          ModuleDescription const& md) override;

private:

  std::string const catalog_;
  bool dropAllEvents_ {false};
  bool dropAllSubRuns_;
  std::string const moduleLabel_;
  int inputFileCount_ {0};
  std::unique_ptr<RootDAQOutFile> rootOutputFile_ {nullptr};
  FileStatsCollector fstats_;
  std::string const filePattern_;
  std::string tmpDir_;
  std::string lastClosedFileName_ {};

  // We keep this set of data members for the use of RootDAQOutFile.
  int const compressionLevel_;
  int64_t const saveMemoryObjectThreshold_;
  int64_t const treeMaxVirtualSize_;
  int const splitLevel_;
  int const basketSize_;
  DropMetaData dropMetaData_;
  bool dropMetaDataForDroppedData_;

  // We keep this for the use of RootDAQOutFile and we also use it
  // during file open to make some choices.
  bool fastCloning_ {true};

  // Set false only for cases where we are guaranteed never to need
  // historical ParameterSet information in the downstream file
  // (e.g. mixing).
  bool writeParameterSets_;
  ClosingCriteria fileProperties_;

  // ResultsProducer management.
  RPManager rpm_;
};

art::RootDAQOut::
RootDAQOut(Parameters const& config)
  : OutputModule{config().omConfig, config.get_PSet()}
  , catalog_{config().catalog()}
  , dropAllSubRuns_{config().dropAllSubRuns()}
  , moduleLabel_{config.get_PSet().get<string>("module_label")}
  , fstats_{moduleLabel_, processName()}
  , filePattern_{config().omConfig().fileName()}
  , tmpDir_{config().tmpDir() == default_tmpDir ? parent_path(filePattern_) : config().tmpDir()}
  , compressionLevel_{config().compressionLevel()}
  , saveMemoryObjectThreshold_{config().saveMemoryObjectThreshold()}
  , treeMaxVirtualSize_{config().treeMaxVirtualSize()}
  , splitLevel_{config().splitLevel()}
  , basketSize_{config().basketSize()}
  , dropMetaData_{config().dropMetaData()}
  , dropMetaDataForDroppedData_{config().dropMetaDataForDroppedData()}
  , writeParameterSets_{config().writeParameterSets()}
  , fileProperties_{
    (detail::validateFileNamePattern(config.get_PSet().has_key(config().fileProperties.name()), filePattern_), // comma operator!
     config().fileProperties())}
  , rpm_{config.get_PSet()}
{
  bool const dropAllEventsSet {config().dropAllEvents(dropAllEvents_)};
  dropAllEvents_ = detail::shouldDropEvents(dropAllEventsSet, dropAllEvents_, dropAllSubRuns_);

  // N.B. Any time file switching is enabled at a boundary other than
  //      InputFile, fastCloning_ ***MUST*** be deactivated.  This is
  //      to ensure that the Event tree from the InputFile is not
  //      accidentally cloned to the output file before the output
  //      module has seen the events that are going to be processed.
  bool const fastCloningSet {config().fastCloning(fastCloning_)};
  fastCloning_ = detail::shouldFastClone(fastCloningSet, fastCloning_, wantAllEvents(), fileProperties_);

  if (!writeParameterSets_) {
    mf::LogWarning("PROVENANCE")
      << "Output module " << moduleLabel_ << " has parameter writeParameterSets set to false.\n"
      << "Parameter set provenance will not be available in subsequent jobs.\n"
      << "Check your experiment's policy on this issue to avoid future problems\n"
      << "with analysis reproducibility.\n";
  }
}

void
art::RootDAQOut::
openFile(FileBlock const& fb)
{
  // Note: The file block here refers to the currently open
  //       input file, so we can find out about the available
  //       products by looping over the branches of the input
  //       file data trees.
  if (!isFileOpen()) {
    doOpenFile();
    respondToOpenInputFile(fb);
  }
}

void
art::RootDAQOut::
postSelectProducts(FileBlock const& fb)
{
  if (isFileOpen()) {
    rootOutputFile_->selectProducts(fb);
  }
}

void
art::RootDAQOut::
respondToOpenInputFile(FileBlock const& fb)
{
  ++inputFileCount_;
  if (!isFileOpen()) return;

  bool fastCloneThisOne = fastCloning_ && (fb.tree() != nullptr) &&
                          ((remainingEvents() < 0) ||
                           (remainingEvents() >= fb.tree()->GetEntries()));
  if (fastCloning_ && !fastCloneThisOne) {
    mf::LogWarning("FastCloning")
      << "Fast cloning deactivated for this input file due to "
      << "empty event tree and/or event limits.";
  }
  if (fastCloneThisOne && !fb.fastClonable()) {
    mf::LogWarning("FastCloning")
      << "Fast cloning deactivated for this input file due to "
      << "information in FileBlock.";
    fastCloneThisOne = false;
  }
  rootOutputFile_->beginInputFile(fb, fastCloneThisOne && fastCloning_);
  fstats_.recordInputFile(fb.fileName());
}

void
art::RootDAQOut::
readResults(ResultsPrincipal const& resp)
{
  rpm_.for_each_RPWorker([&resp](RPWorker& w) {
      Results const res {resp, w.moduleDescription()};
      w.rp().doReadResults(res);
    } );
}

void
art::RootDAQOut::
respondToCloseInputFile(FileBlock const& fb)
{
  if (isFileOpen()) {
    rootOutputFile_->respondToCloseInputFile(fb);
  }
}

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

void art::RootDAQOut::write(EventPrincipal& ep)
{
  TRACE( 10, "RootDAQOut::write begin - line " TOSTRING(__LINE__) );
  if (dropAllEvents_) {
    return;
  }
  if (hasNewlyDroppedBranch()[InEvent]) {
    ep.addToProcessHistory();
  }
  rootOutputFile_->writeOne(ep);
  fstats_.recordEvent(ep.id());
  TRACE( 10, "RootDAQOut::write done/return" );
}

void
art::RootDAQOut::
setSubRunAuxiliaryRangeSetID(RangeSet const& rs)
{
  rootOutputFile_->setSubRunAuxiliaryRangeSetID(rs);
}

void
art::RootDAQOut::
writeSubRun(SubRunPrincipal& sr)
{
  if (dropAllSubRuns_) {
    return;
  }
  if (hasNewlyDroppedBranch()[InSubRun]) {
    sr.addToProcessHistory();
  }
  rootOutputFile_->writeSubRun(sr);
  fstats_.recordSubRun(sr.id());
}

void
art::RootDAQOut::
setRunAuxiliaryRangeSetID(RangeSet const& rs)
{
  rootOutputFile_->setRunAuxiliaryRangeSetID(rs);
}

void
art::RootDAQOut::
writeRun(RunPrincipal& r)
{
  if (hasNewlyDroppedBranch()[InRun]) {
    r.addToProcessHistory();
  }
  rootOutputFile_->writeRun(r);
  fstats_.recordRun(r.id());
}

void
art::RootDAQOut::
startEndFile()
{
  auto resp = std::make_unique<ResultsPrincipal>(ResultsAuxiliary{},
                                                 description().processConfiguration());
  if (ProductMetaData::instance().productProduced(InResults) ||
      hasNewlyDroppedBranch()[InResults]) {
    resp->addToProcessHistory();
  }
  rpm_.for_each_RPWorker([&resp](RPWorker& w) {
      Results res {*resp, w.moduleDescription()};
      w.rp().doWriteResults(*resp, res);
    } );
  rootOutputFile_->writeResults(*resp);
}

void
art::RootDAQOut::
writeFileFormatVersion()
{
  rootOutputFile_->writeFileFormatVersion();
}

void
art::RootDAQOut::
writeFileIndex()
{
  rootOutputFile_->writeFileIndex();
}

void
art::RootDAQOut::
writeEventHistory()
{
  rootOutputFile_->writeEventHistory();
}

void
art::RootDAQOut::
writeProcessConfigurationRegistry()
{
  rootOutputFile_->writeProcessConfigurationRegistry();
}

void
art::RootDAQOut::
writeProcessHistoryRegistry()
{
  rootOutputFile_->writeProcessHistoryRegistry();
}

void
art::RootDAQOut::
writeParameterSetRegistry()
{
  if (writeParameterSets_) {
    rootOutputFile_->writeParameterSetRegistry();
  }
}

void
art::RootDAQOut::
writeProductDescriptionRegistry()
{
  rootOutputFile_->writeProductDescriptionRegistry();
}

void
art::RootDAQOut::
writeParentageRegistry()
{
  rootOutputFile_->writeParentageRegistry();
}

void
art::RootDAQOut::
writeBranchIDListRegistry()
{
  rootOutputFile_->writeBranchIDListRegistry();
}

void
art::RootDAQOut::
doWriteFileCatalogMetadata(FileCatalogMetadata::collection_type const& md,
                           FileCatalogMetadata::collection_type const& ssmd)
{
  rootOutputFile_->writeFileCatalogMetadata(fstats_, md, ssmd);
}

void
art::RootDAQOut::writeProductDependencies()
{
  rootOutputFile_->writeProductDependencies();
}

void
art::RootDAQOut::finishEndFile()
{
# if ART_HEX_VERSION >= 0x20703
  std::string const currentFileName {rootOutputFile_->currentFileName()};
  rootOutputFile_->writeTTrees();
  rootOutputFile_.reset();
  fstats_.recordFileClose();
  lastClosedFileName_ = PostCloseFileRenamer{fstats_}.maybeRenameFile(rootOutputFile_->currentFileName(), filePattern_);
# else
  rootOutputFile_->finishEndFile();
  fstats_.recordFileClose();
  lastClosedFileName_ = PostCloseFileRenamer{fstats_}.maybeRenameFile(rootOutputFile_->currentFileName(), filePattern_);
  rootOutputFile_.reset();
# endif
  detail::logFileAction("Closed output file ", lastClosedFileName_);
  rpm_.invoke(&ResultsProducer::doClear);
}

void
art::RootDAQOut::
doRegisterProducts(MasterProductRegistry& mpr,
                   ModuleDescription const& md)
{
  // Register Results products from ResultsProducers.
  rpm_.for_each_RPWorker([&mpr, &md](RPWorker& w) {
      auto const& params = w.params();
      w.setModuleDescription(ModuleDescription{params.rpPSetID,
                                               params.rpPluginType,
                                               md.moduleLabel() + '#' + params.rpLabel,
                                               md.processConfiguration()});
      w.rp().registerProducts(mpr, w.moduleDescription());
    });
}

void
art::RootDAQOut::setFileStatus(OutputFileStatus const ofs)
{
  if (isFileOpen())
    rootOutputFile_->setFileStatus(ofs);
}

bool
art::RootDAQOut::
isFileOpen() const
{
  TRACE( 10, "RootDAQOut::isFileOpen start" );
  bool ret=rootOutputFile_.get() != nullptr;
  TRACE( 10, "RootDAQOut::isFileOpen return %d", ret );
  return ret;
}

void
art::RootDAQOut::incrementInputFileNumber()
{
  if (isFileOpen())
    rootOutputFile_->incrementInputFileNumber();
}

bool
art::RootDAQOut::
requestsToCloseFile() const
{
  bool ret;
  if (isFileOpen()) {
	  ret = rootOutputFile_->requestsToCloseFile();
  } else {
	  ret = false;
  }
  return ret;
}

#if ART_HEX_VERSION < 0x20703
art::Boundary
art::RootDAQOut::
fileSwitchBoundary() const
{
	TRACE( 10, "RootDAQOut::fileSwitchBoundary start" );
	auto bb=fileProperties_.granularity();
	TRACE( 10, "RootDAQOut::fileSwitchBoundary done/return" );
  return bb;
}
#endif

void
art::RootDAQOut::
doOpenFile()
{
  if (inputFileCount_ == 0) {
    throw art::Exception(art::errors::LogicError)
        << "Attempt to open output file before input file. "
        << "Please report this to the core framework developers.\n";
  }
  rootOutputFile_ = std::make_unique<RootDAQOutFile>(this,
                                                     unique_filename(tmpDir_ + "/RootDAQOut"),
                                                     fileProperties_,
                                                     compressionLevel_,
                                                     saveMemoryObjectThreshold_,
                                                     treeMaxVirtualSize_,
                                                     splitLevel_,
                                                     basketSize_,
                                                     dropMetaData_,
                                                     dropMetaDataForDroppedData_,
                                                     fastCloning_);
  fstats_.recordFileOpen();
  detail::logFileAction("Opened output file with pattern ", filePattern_);
}

string const&
art::RootDAQOut::
lastClosedFileName() const
{
  if (lastClosedFileName_.empty()) {
    throw Exception(errors::LogicError, "RootDAQOut::currentFileName(): ")
        << "called before meaningful.\n";
  }
  return lastClosedFileName_;
}

void
art::RootDAQOut::
beginJob()
{
  rpm_.invoke(&ResultsProducer::doBeginJob);
}

void
art::RootDAQOut::
endJob()
{
  rpm_.invoke(&ResultsProducer::doEndJob);
}

void
art::RootDAQOut::
event(EventPrincipal const& ep)
{
  rpm_.for_each_RPWorker([&ep](RPWorker& w) {
      Event const e {ep, w.moduleDescription()};
      w.rp().doEvent(e);
    });
}

void
art::RootDAQOut::
beginSubRun(art::SubRunPrincipal const& srp)
{
  rpm_.for_each_RPWorker([&srp](RPWorker& w) {
      SubRun const sr {srp, w.moduleDescription()};
      w.rp().doBeginSubRun(sr);
    });
}

void
art::RootDAQOut::
endSubRun(art::SubRunPrincipal const& srp)
{
  rpm_.for_each_RPWorker([&srp](RPWorker& w) {
      SubRun const sr {srp, w.moduleDescription()};
      w.rp().doEndSubRun(sr);
    });
}

void
art::RootDAQOut::
beginRun(art::RunPrincipal const& rp)
{
  rpm_.for_each_RPWorker([&rp](RPWorker& w) {
      Run const r {rp, w.moduleDescription()};
      w.rp().doBeginRun(r);
    });
}

void
art::RootDAQOut::
endRun(art::RunPrincipal const& rp)
{
  rpm_.for_each_RPWorker([&rp](RPWorker& w) {
      Run const r {rp, w.moduleDescription()};
      w.rp().doEndRun(r);
    });
}

DEFINE_ART_MODULE(art::RootDAQOut)

// vim: set sw=2:
