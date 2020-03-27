#include "art/Framework/Core/OutputModule.h"
#include "art/Framework/IO/ClosingCriteria.h"
#include "artdaq-utilities/Plugins/MetricManager.hh"
#include "fhiclcpp/types/Atom.h"
#include "fhiclcpp/types/OptionalAtom.h"
#include "fhiclcpp/types/OptionalSequence.h"
#include "fhiclcpp/types/OptionalTable.h"
#include "fhiclcpp/types/Sequence.h"
#include "fhiclcpp/types/Table.h"
#include "fhiclcpp/types/Tuple.h"

namespace art {
/// <summary>
/// Configuration for the ArtdaqFragmentNamingServiceInterface
/// </summary>
struct ArtdaqFragmentNamingServiceInterfaceConfig
{
	/// "service_provider" (REQUIRED): Name of the provider for the ArtdaqFragmentNamingServiceInterface (e.g. ArtdaqDemoFragmentNamingService)
	fhicl::Atom<std::string> service_provider{fhicl::Name{"service_provider"}, fhicl::Comment{"Name of the provider for the ArtdaqFragmentNamingServiceInterface (e.g. ArtdaqDemoFragmentNamingService)"}};
	/// "unidentified_instance_name" (Default: "unidentified"): Name to use for Fragment types which are not identified by the ArtdaqFragmentNamingServiceInterface implementation.
	fhicl::Atom<std::string> unidentified_instance_name{fhicl::Name{"unidentified_instance_name"}, fhicl::Comment{"Name to use for Fragment types which are not identified by the ArtdaqFragmentNamingServiceInterface implementation."}, "unidentified"};
	/// "fragment_type_map" (OPTIONAL): Additional types to register with the ArtdaqFragmentNamingServiceInterface
	fhicl::OptionalSequence<fhicl::Tuple<artdaq::Fragment::type_t, std::string>> fragment_type_map{fhicl::Name{"fragment_type_map"}, fhicl::Comment{"Additional types to register with the ArtdaqFragmentNamingServiceInterface"}};
};

/// <summary>
/// Configuration for the ArtdaqSharedMemoryServiceInterface
/// </summary>
struct ArtdaqSharedMemoryServiceInterfaceConfig
{
	/// "service_provider" (REQUIRED): Name of the provider for the ArtdaqSharedMemoryServiceInterface (e.g. ArtdaqSharedMemoryService)
	fhicl::Atom<std::string> service_provider{fhicl::Name{"service_provider"}, fhicl::Comment{"Name of the provider for the ArtdaqSharedMemoryServiceInterface (e.g. ArtdaqSharedMemoryService)"}};
	/// "read_timeout_us" (Default: "waiting_time" * 1000000): Amount of time (in us) to wait for events from shared memory. Defaults to waiting_time if unspecified.
	fhicl::Atom<size_t> read_timeout_us{fhicl::Name{"read_timeout_us"}, fhicl::Comment{"Amount of time (in us) to wait for events from shared memory. Defaults to waiting_time if unspecified."}, 600000000};
	/// "waiting_time" (Default: 600.0): Amount of time (in s) to wait for events from shared memory. Overridden by read_timeout_us if specified.
	fhicl::Atom<double> waiting_time{fhicl::Name{"waiting_time"}, fhicl::Comment{"Amount of time (in s) to wait for events from shared memory. Overridden by read_timeout_us if specified."}, 600.0};
	/// "resume_after_timeout" (Default: true): Whether to continue to attempt to receive events after a timeout occurs
	fhicl::Atom<bool> resume_after_timeout{fhicl::Name{"resume_after_timeout"}, fhicl::Comment{"Whether to continue to attempt to receive events after a timeout occurs"}, true};
	/// "shared_memory_key" (OPTIONAL): Key to use for Data shared memory segment. Automatically generated using parent PID.
	fhicl::OptionalAtom<int> shared_memory_key{fhicl::Name{"shared_memory_key"}, fhicl::Comment{"Key to use for Data shared memory segment. Automatically generated using parent PID."}};
	/// "broadcast_shared_memory_key" (OPTIONAL): Key to use for Broadcast shared memory segment. Automatically generated using parent PID.
	fhicl::OptionalAtom<int> broadcast_shared_memory_key{fhicl::Name{"broadcast_shared_memory_key"}, fhicl::Comment{"Key to use for Broadcast shared memory segment. Automatically generated using parent PID."}};
	/// "metrics" (OPTIONAL): Configuration for artdaq Metrics
	fhicl::OptionalTable<artdaq::MetricManager::Config> metrics{fhicl::Name{"metrics"}, fhicl::Comment{"Configuration for artdaq Metrics"}};
};

/// <summary>
/// Configuration of the services block for artdaq art processes
/// </summary>
struct ServicesConfig
{
	fhicl::Table<ArtdaqSharedMemoryServiceInterfaceConfig> ArtdaqSharedMemoryServiceInterface{fhicl::Name{"ArtdaqSharedMemoryServiceInterface"}};                ///< Configuration for the ArtdaqSharedMemoryServiceInterface
	fhicl::OptionalTable<ArtdaqFragmentNamingServiceInterfaceConfig> ArtdaqFragmentNamingServiceInterface{fhicl::Name{"ArtdaqFragmentNamingServiceInterface"}};  ///< Configuration for the ArtdaqFragmentNamingServiceInterface
};

struct AnalyzersConfig
{};  ///< \todo Fill in artdaq-provided Analyzer modules
struct ProducersConfig
{};  ///< Artdaq does not provide any producers
struct FiltersConfig
{};  ///< \todo Fill in artdaq-provided Filter modules

/// <summary>
/// Configuration of the physics block for artdaq art processes
/// </summary>
struct PhysicsConfig
{
	fhicl::Table<AnalyzersConfig> analyzers{fhicl::Name{"analyzers"}};  ///< Analyzer module configuration
	fhicl::Table<ProducersConfig> producers{fhicl::Name{"producers"}};  ///< Producer module configuration
	fhicl::Table<FiltersConfig> filters{fhicl::Name{"filters"}};        ///< Filter module configuration
	/// Output modules (configured in the outputs block) to use
	fhicl::Sequence<std::string> my_output_modules{fhicl::Name{"my_output_modules"}, fhicl::Comment{"Output modules (configured in the outputs block) to use"}};
};

/// <summary>
/// Confgiguration for ROOT output modules
/// </summary>
struct RootOutputConfig
{
	using Name = fhicl::Name;        ///< Parameter Name
	using Comment = fhicl::Comment;  ///< Parameter Comment
	/// Configuration Parameter
	template<typename T>
	using Atom = fhicl::Atom<T>;
	/// Optional Configuration Parameter
	template<typename T>
	using OptionalAtom = fhicl::OptionalAtom<T>;

	fhicl::TableFragment<art::OutputModule::Config> omConfig;                          ///< Configuration common to all OutputModules.
	Atom<std::string> catalog{Name("catalog"), ""};                                    ///< ???
	OptionalAtom<bool> dropAllEvents{Name("dropAllEvents")};                           ///< Whether to drop all events ???
	Atom<bool> dropAllSubRuns{Name("dropAllSubRuns"), false};                          ///< Whether to drop all subruns ???
	OptionalAtom<bool> fastCloning{Name("fastCloning")};                               ///< Whether to try to use fastCloning on the file
	Atom<std::string> tmpDir{Name("tmpDir"), "/tmp"};                                  ///< Temporary directory
	Atom<int> compressionLevel{Name("compressionLevel"), 7};                           ///< Compression level to use. artdaq recommends <= 3
	Atom<int64_t> saveMemoryObjectThreshold{Name("saveMemoryObjectThreshold"), -1l};   ///< ???
	Atom<int64_t> treeMaxVirtualSize{Name("treeMaxVirtualSize"), -1};                  ///< ???
	Atom<int> splitLevel{Name("splitLevel"), 99};                                      ///< ???
	Atom<int> basketSize{Name("basketSize"), 16384};                                   ///< ???
	Atom<bool> dropMetaDataForDroppedData{Name("dropMetaDataForDroppedData"), false};  ///< ???
	Atom<std::string> dropMetaData{Name("dropMetaData"), "NONE"};                      ///< Which metadata to drop (Default: "NONE")
	Atom<bool> writeParameterSets{Name("writeParameterSets"), true};                   ///< Write art ParameterSet to output file (Default: true)
	fhicl::Table<ClosingCriteria::Config> fileProperties{Name("fileProperties")};      ///< When should the file be closed

	/// <summary>
	///  These keys should be ignored by the configuration validation processor
	/// </summary>
	struct KeysToIgnore
	{
		/// <summary>
		/// Get the keys to ignore
		/// </summary>
		/// <returns>Set of keys to ignore</returns>
		std::set<std::string> operator()() const
		{
			std::set<std::string> keys{art::OutputModule::Config::KeysToIgnore::get()};
			keys.insert("results");
			return keys;
		}
	};
};

/// <summary>
/// Configuration for the outputs block of artdaq art processes
/// </summary>
struct OutputsConfig
{
	fhicl::OptionalTable<art::OutputModule::Config> rootNetOutput{fhicl::Name{"rootNetOutput"}};  ///< For transferring data from EventBuilders to DataLoggers
	fhicl::OptionalTable<RootOutputConfig> normalOutput{fhicl::Name{"normalOutput"}};             ///< Normal art/ROOT output
	fhicl::OptionalTable<RootOutputConfig> rootDAQOutFile{fhicl::Name{"rootDAQOutFile"}};         ///< art/ROOT output where the filename can be specified at initialization (e.g. to /dev/null), for testing
};

/// <summary>
/// Configuration for the source block of artdaq art processes
/// </summary>
struct SourceConfig
{
	/// "module_type": REQUIRED: Module type of source. Should be "ArtdaqInput"
	fhicl::Atom<std::string> module_type{fhicl::Name{"module_type"}, fhicl::Comment{"Module type of source. Should be \"ArtdaqInput\""}};
	/// "init_fragment_timeout_seconds" (Default: 600.0): Amount of time (in s) ArtdaqInput should wait for an Init Fragment before simply returning Fragments
	fhicl::Atom<double> init_fragment_timeout_seconds{fhicl::Name{"init_fragment_timeout_seconds"}, fhicl::Comment{"Amount of time ArtdaqInput should wait for an Init Fragment before simply returning Fragments"}, 600.0};
	/// "raw_data_label" (Default: "daq"): Label to use for raw data (i.e. Fragments from the DAQ)
	fhicl::Atom<std::string> raw_data_label{fhicl::Name{"raw_data_label"}, fhicl::Comment{"Label to use for raw data (i.e. Fragments from the DAQ)"}, "daq"};
	/// "register_fragment_types" (Default: true): Whether ArtdaqInputHelper should register the known Fragment types from the ArtdaqFragmentNamingServiceInterface. Required for EventBuilders. Disable for processes that don't handle Fragments.
	fhicl::Atom<bool> register_fragment_types{fhicl::Name{"register_fragment_types"}, fhicl::Comment{"Whether ArtdaqInputHelper should register the known Fragment types from the ArtdaqFragmentNamingServiceInterface. Required for EventBuilders. Disable for processes that don't handle Fragments."}, true};
};

/// <summary>
/// Required configuration for art processes started by artdaq, with artdaq-specific defaults where applicable
/// </summary>
struct Config
{
	fhicl::Table<art::ServicesConfig> services{fhicl::Name{"services"}};  ///< Services block
	fhicl::Table<art::PhysicsConfig> physics{fhicl::Name{"physics"}};     ///< Physics block
	fhicl::Table<art::OutputsConfig> outputs{fhicl::Name{"outputs"}};     ///< Outputs block
	fhicl::Table<art::SourceConfig> source{fhicl::Name{"source"}};        ///< Source block
	/// "process_name" (Default: "DAQ"): Name of this art processing job
	fhicl::Atom<std::string> process_name{fhicl::Name{"process_name"}, fhicl::Comment{"Name of this art processing job"}, "DAQ"};
};
}  // namespace art
