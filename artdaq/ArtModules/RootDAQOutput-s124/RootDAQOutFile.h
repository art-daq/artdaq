#ifndef art_root_io_RootDAQOutFile_h
#define art_root_io_RootDAQOutFile_h
// vim: set sw=2 expandtab :

#include "art/Framework/Core/fwd.h"
#include "art/Framework/IO/ClosingCriteria.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/fwd.h"
#include "art/Framework/Services/System/FileCatalogMetadata.h"
#include "art_root_io/DropMetaData.h"
#include "art_root_io/DummyProductCache.h"
#include "art_root_io/FastCloningEnabled.h"
#include "art_root_io/RootOutputTree.h"
#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/BranchType.h"
#include "canvas/Persistency/Provenance/FileIndex.h"
#include "canvas/Persistency/Provenance/ProductID.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"
#include "canvas/Persistency/Provenance/fwd.h"
#include "cetlib/sqlite/Connection.h"

#include <array>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "TFile.h"

class TTree;

namespace art {
class FileStatsCollector;
class RootFileBlock;

struct OutputItem
{
	~OutputItem();
	explicit OutputItem(BranchDescription const& bd);

	BranchDescription const branchDescription;
	mutable void const* product;
};

class RootDAQOutFile
{
public:
	enum class ClosureRequestMode
	{
		MaxEvents = 0,
		MaxSize = 1,
		Unset = 2
	};
	using RootDAQOutTreePtrArray =
	    std::array<std::unique_ptr<RootDAQOutTree>, NumBranchTypes>;

	~RootDAQOutFile();
	explicit RootDAQOutFile(OutputModule*,
	                        std::string const& fileName,
	                        ClosingCriteria const& fileSwitchCriteria,
	                        int compressionLevel,
	                        unsigned freePercent,
	                        unsigned freeMB,
	                        int64_t saveMemoryObjectThreshold,
	                        int64_t treeMaxVirtualSize,
	                        int splitLevel,
	                        int basketSize,
	                        DropMetaData dropMetaData,
	                        bool dropMetaDataForDroppedData);
	RootDAQOutFile(RootDAQOutFile const&) = delete;
	RootDAQOutFile(RootDAQOutFile&&) = delete;
	RootDAQOutFile& operator=(RootDAQOutFile const&) = delete;
	RootDAQOutFile& operator=(RootDAQOutFile&&) = delete;

	void createDatabaseTables();
	void writeTTrees();
	void writeOne(EventPrincipal const&);
	void writeSubRun(SubRunPrincipal const&);
	void writeRun(RunPrincipal const&);
	void writeFileFormatVersion();
	void writeFileIndex();
#if ART_HEX_VERSION < 0x31100
	void writeEventHistory();
#endif
	void writeProcessConfigurationRegistry();
	void writeProcessHistoryRegistry();
	void writeParameterSetRegistry();
	void writeProductDescriptionRegistry();
	void writeParentageRegistry();
	void writeProductDependencies();
	void writeFileCatalogMetadata(FileStatsCollector const& stats,
	                              FileCatalogMetadata::collection_type const&,
	                              FileCatalogMetadata::collection_type const&);
	void writeResults(ResultsPrincipal& resp);
	void setRunAuxiliaryRangeSetID(RangeSet const&);
	void setSubRunAuxiliaryRangeSetID(RangeSet const&);
	void beginInputFile(RootFileBlock const*,
	                    FastCloningEnabled fastCloningEnabled);
	void incrementInputFileNumber();
	void respondToCloseInputFile(FileBlock const&);
	bool requestsToCloseFile();
	void setFileStatus(OutputFileStatus ofs);
	void selectProducts();
	std::string const& currentFileName() const;
	bool maxEventsPerFileReached(
	    FileIndex::EntryNumber_t maxEventsPerFile) const;
	bool maxSizeReached(unsigned maxFileSize) const;

private:
	template<BranchType>
	void fillBranches(Principal const&, std::vector<ProductProvenance>*);
	template<BranchType BT>
	EDProduct const* getProduct(OutputHandle const&,
	                            RangeSet const& productRS,
	                            std::string const& wrappedName);

	mutable std::recursive_mutex mutex_{};
	OutputModule const* om_;
	std::string file_;
	ClosingCriteria fileSwitchCriteria_;
	OutputFileStatus status_{OutputFileStatus::Closed};
	int const compressionLevel_;
	unsigned freePercent_;
	unsigned freeMB_;
	int64_t const saveMemoryObjectThreshold_;
	int64_t const treeMaxVirtualSize_;
	int const splitLevel_;
	int const basketSize_;
	DropMetaData dropMetaData_;
	bool dropMetaDataForDroppedData_;
	bool wasFastCloned_{false};
	std::unique_ptr<TFile> filePtr_;
	FileIndex fileIndex_;
	FileProperties fp_;
	TTree* metaDataTree_;
	TTree* fileIndexTree_;
	TTree* parentageTree_;
	EventAuxiliary const* pEventAux_{nullptr};
	SubRunAuxiliary const* pSubRunAux_{nullptr};
	RunAuxiliary const* pRunAux_{nullptr};
	ResultsAuxiliary const* pResultsAux_{nullptr};
	ProductProvenances eventProductProvenanceVector_{};
	ProductProvenances subRunProductProvenanceVector_{};
	ProductProvenances runProductProvenanceVector_{};
	ProductProvenances resultsProductProvenanceVector_{};
	ProductProvenances* pEventProductProvenanceVector_{
	    &eventProductProvenanceVector_};
	ProductProvenances* pSubRunProductProvenanceVector_{
	    &subRunProductProvenanceVector_};
	ProductProvenances* pRunProductProvenanceVector_{
	    &runProductProvenanceVector_};
	ProductProvenances* pResultsProductProvenanceVector_{
	    &resultsProductProvenanceVector_};
	History const* pHistory_;
	RootDAQOutTreePtrArray treePointers_;
	bool dataTypeReported_{false};
	std::array<ProductDescriptionsByID, NumBranchTypes> descriptionsToPersist_{
	    {}};
	std::unique_ptr<cet::sqlite::Connection> rootFileDB_;
	std::array<std::map<ProductID, OutputItem>, NumBranchTypes>
	    selectedOutputItemList_{{}};
	DummyProductCache dummyProductCache_;
	unsigned subRunRSID_{-1u};
	unsigned runRSID_{-1u};
	std::chrono::steady_clock::time_point beginTime_;
};

}  // namespace art

// Local Variables:
// mode: c++
// End:

#endif /* art_root_io_RootDAQOutFile_h */
