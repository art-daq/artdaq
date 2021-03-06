#ifndef art_root_io_RootDAQOutFile_h
#define art_root_io_RootDAQOutFile_h
// vim: set sw=2 expandtab :

#include "art/Framework/IO/ClosingCriteria.h"
#include "art/Framework/Services/System/FileCatalogMetadata.h"
#include "art_root_io/DropMetaData.h"
#include "art_root_io/DummyProductCache.h"
#include "art_root_io/RootOutputTree.h"
#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/FileIndex.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"
#include "cetlib/sqlite/Connection.h"

#include <array>
#include <chrono>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "TFile.h"
#include "TROOT.h"

class TTree;

namespace art {
class EDProduct;
class EventAuxiliary;
class EventPrincipal;
class FileBlock;
class FileStatsCollector;
class History;
class OutputHandle;
class OutputModule;
class Principal;
class RangeSet;
class ResultsAuxiliary;
class ResultsPrincipal;
class RootFileBlock;
class RunAuxiliary;
class RunPrincipal;
class SubRunAuxiliary;
class SubRunPrincipal;
class RootDAQOutFile
{
public:  // TYPES
	enum class ClosureRequestMode
	{
		MaxEvents = 0,
		MaxSize = 1,
		Unset = 2
	};
	using RootOutputTreePtrArray =
	    std::array<std::unique_ptr<RootOutputTree>, NumBranchTypes>;
	struct OutputItem
	{
	public:  // MEMBER FUNCTIONS -- Special Member Functions
		~OutputItem();
		explicit OutputItem(BranchDescription bd);

	public:  // MEMBER FUNCTIONS
		std::string const& branchName() const;
		bool operator<(OutputItem const& rh) const;

	public:  // MEMBER DATA
		BranchDescription const branchDescription_;
		mutable void const* product_;

	private:
		OutputItem(OutputItem const&) = delete;
		OutputItem(OutputItem&&) = delete;
		OutputItem& operator=(OutputItem const&) = delete;
		OutputItem& operator=(OutputItem&&) = delete;
	};

public:  // MEMBER FUNCTIONS -- Static API
	static bool shouldFastClone(bool const fastCloningSet,
	                            bool const fastCloning,
	                            bool const wantAllEvents,
	                            ClosingCriteria const& cc);

public:  // MEMBER FUNCTIONS -- Special Member Functions
	~RootDAQOutFile();
	explicit RootDAQOutFile(OutputModule*,
	                        std::string const& fileName,
	                        ClosingCriteria const& fileSwitchCriteria,
	                        int const compressionLevel,
	                        unsigned freePercent,
	                        unsigned freeMB,
	                        int64_t const saveMemoryObjectThreshold,
	                        int64_t const treeMaxVirtualSize,
	                        int const splitLevel,
	                        int const basketSize,
	                        DropMetaData dropMetaData,
	                        bool dropMetaDataForDroppedData,
	                        bool fastCloningRequested);
	RootDAQOutFile(RootDAQOutFile const&) = delete;
	RootDAQOutFile(RootDAQOutFile&&) = delete;
	RootDAQOutFile& operator=(RootDAQOutFile const&) = delete;
	RootDAQOutFile& operator=(RootDAQOutFile&&) = delete;

public:  // MEMBER FUNCTIONS
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
	void beginInputFile(RootFileBlock const*, bool fastClone);
	void incrementInputFileNumber();
	void respondToCloseInputFile(FileBlock const&);
	bool requestsToCloseFile();
	void setFileStatus(OutputFileStatus const ofs);
	void selectProducts();
	std::string const& currentFileName() const;
	bool maxEventsPerFileReached(
	    FileIndex::EntryNumber_t const maxEventsPerFile) const;
	bool maxSizeReached(unsigned const maxFileSize) const;

private:  // MEMBER FUNCTIONS
	template<BranchType>
	void fillBranches(Principal const&, std::vector<ProductProvenance>*);
	template<BranchType BT>
	EDProduct const* getProduct(OutputHandle const&,
	                            RangeSet const& productRS,
	                            std::string const& wrappedName);

private:  // MEMBER DATA
	mutable std::recursive_mutex mutex_;
	OutputModule const* om_;
	std::string file_;
	ClosingCriteria fileSwitchCriteria_;
	OutputFileStatus status_;
	int const compressionLevel_;
	unsigned freePercent_;
	unsigned freeMB_;
	int64_t const saveMemoryObjectThreshold_;
	int64_t const treeMaxVirtualSize_;
	int const splitLevel_;
	int const basketSize_;
	DropMetaData dropMetaData_;
	bool dropMetaDataForDroppedData_;
	bool fastCloningEnabledAtConstruction_;
	bool wasFastCloned_;
	std::unique_ptr<TFile> filePtr_;
	FileIndex fileIndex_;
	FileProperties fp_;
	TTree* metaDataTree_;
	TTree* fileIndexTree_;
	TTree* parentageTree_;
	TTree* eventHistoryTree_;
	EventAuxiliary const* pEventAux_;
	SubRunAuxiliary const* pSubRunAux_;
	RunAuxiliary const* pRunAux_;
	ResultsAuxiliary const* pResultsAux_;
	ProductProvenances eventProductProvenanceVector_;
	ProductProvenances subRunProductProvenanceVector_;
	ProductProvenances runProductProvenanceVector_;
	ProductProvenances resultsProductProvenanceVector_;
	ProductProvenances* pEventProductProvenanceVector_;
	ProductProvenances* pSubRunProductProvenanceVector_;
	ProductProvenances* pRunProductProvenanceVector_;
	ProductProvenances* pResultsProductProvenanceVector_;
	History const* pHistory_;
	RootOutputTreePtrArray treePointers_;
	bool dataTypeReported_;
	std::array<ProductDescriptionsByID, NumBranchTypes> descriptionsToPersist_;
	std::unique_ptr<cet::sqlite::Connection> rootFileDB_;
	std::array<std::set<OutputItem>, NumBranchTypes> selectedOutputItemList_;
	DummyProductCache dummyProductCache_;
	unsigned subRunRSID_;
	unsigned runRSID_;
	std::chrono::steady_clock::time_point beginTime_;
};

}  // namespace art

// Local Variables:
// mode: c++
// End:
#endif /* art_root_io_RootDAQOutFile_h */
