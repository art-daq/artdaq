#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/Table.h"
#include "fhiclcpp/types/OptionalAtom.h"
#include "fhiclcpp/types/OptionalTable.h"
#include "artdaq/ArtModules/NetMonTransportService.h"
#include "artdaq/ArtModules/RootNetOutput.hh"
#include "art/Framework/Core/OutputModule.h"
#if ART_HEX_VERSION >= 0x21002
# include "art/Framework/IO/ClosingCriteria.h"
#else
# include "art/Framework/IO/Root/RootOutputClosingCriteria.h"
#endif

namespace art {
	struct ServicesSchedulerConfig
	{
		fhicl::Atom<bool> errorOnFailureToPut{ fhicl::Name{"errorOnFailureToPut"}, fhicl::Comment{"This parameter is necessary for correct function of artdaq. Do not modify."}, false };
	};
	struct ServicesConfig
	{
		fhicl::Table<ServicesSchedulerConfig> scheduler{ fhicl::Name{"scheduler"} };
		fhicl::OptionalTable<NetMonTransportService::Config> netMonTransportServiceInterface{ fhicl::Name{ "NetMonTransportServiceInterface" } };
	};

	struct AnalyzersConfig {}; ///< \todo Fill in artdaq-provided Analyzer modules
	struct ProducersConfig {}; ///< Artdaq does not provide any producers
	struct FiltersConfig {}; ///< \todo Fill in artdaq-provided Filter modules

	struct PhysicsConfig
	{
		fhicl::Table<AnalyzersConfig> analyzers{ fhicl::Name{"analyzers"} };
		fhicl::Table<ProducersConfig> producers{ fhicl::Name{"producers"} };
		fhicl::Table<FiltersConfig> filters{ fhicl::Name{"filters"} };
		fhicl::Sequence<std::string> my_output_modules{ fhicl::Name{"my_output_modules"}, fhicl::Comment{"Output modules (configured in the outputs block) to use"} };
	};


	struct RootOutputConfig
	{

		using Name = fhicl::Name;
		using Comment = fhicl::Comment;
		template <typename T> using Atom = fhicl::Atom<T>;
		template <typename T> using OptionalAtom = fhicl::OptionalAtom<T>;

		fhicl::TableFragment<art::OutputModule::Config> omConfig;
		Atom<std::string> catalog{ Name("catalog"), "" };
		OptionalAtom<bool> dropAllEvents{ Name("dropAllEvents") };
		Atom<bool> dropAllSubRuns{ Name("dropAllSubRuns"), false };
		OptionalAtom<bool> fastCloning{ Name("fastCloning") };
		Atom<std::string> tmpDir{ Name("tmpDir"), "/tmp" };
		Atom<int> compressionLevel{ Name("compressionLevel"), 7 };
		Atom<int64_t> saveMemoryObjectThreshold{ Name("saveMemoryObjectThreshold"), -1l };
		Atom<int64_t> treeMaxVirtualSize{ Name("treeMaxVirtualSize"), -1 };
		Atom<int> splitLevel{ Name("splitLevel"), 99 };
		Atom<int> basketSize{ Name("basketSize"), 16384 };
		Atom<bool> dropMetaDataForDroppedData{ Name("dropMetaDataForDroppedData"), false };
		Atom<std::string> dropMetaData{ Name("dropMetaData"), "NONE" };
		Atom<bool> writeParameterSets{ Name("writeParameterSets"), true };
		fhicl::Table<ClosingCriteria::Config> fileProperties{ Name("fileProperties") };
		
		struct KeysToIgnore
		{
			std::set<std::string> operator()() const
			{
				std::set<std::string> keys{ art::OutputModule::Config::KeysToIgnore::get() };
				keys.insert("results");
				return keys;
			}
		};

	};

	struct OutputsConfig
	{
		fhicl::OptionalTable<art::RootNetOutput::Config> rootNetOutput{ fhicl::Name{"rootNetOutput"} };
		fhicl::OptionalTable<RootOutputConfig> normalOutput{ fhicl::Name{"normalOutput"} };
		fhicl::OptionalTable<RootOutputConfig> rootDAQOutFile{ fhicl::Name{"rootDAQOutFile"} };
	};

	struct SourceConfig
	{
		fhicl::Atom<std::string> module_type{ fhicl::Name{ "module_type" }, fhicl::Comment{ "Module type of source. Should be \"RawInput\", \"NetMonInput\", or an experiment-defined input type (i.e. \"DemoInput\")" } };
	};

	struct Config
	{
		fhicl::Table<art::ServicesConfig> services{ fhicl::Name{"services"} };
		fhicl::Table<art::PhysicsConfig> physics{ fhicl::Name{"physics"} };
		fhicl::Table<art::OutputsConfig> outputs{ fhicl::Name{"outputs"} };
		fhicl::Table<art::SourceConfig> source{ fhicl::Name{"source"} };
		fhicl::Atom<std::string> process_name{ fhicl::Name{"process_name" },fhicl::Comment{"Name of this art processing job"}, "DAQ" };
	};
}
