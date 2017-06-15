#ifndef artdaq_ArtModules_detail_SharedMemoryReader_hh
#define artdaq_ArtModules_detail_SharedMemoryReader_hh

#include "art/Framework/Core/Frameworkfwd.h"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Data/Fragment.hh"

#include <string>
#include <map>
#include "artdaq-core/Data/RawEvent.hh"

namespace artdaq
{
	namespace detail
	{
		/**
		 * \brief SharedMemoryEventReceiver is a SharedMemoryManager which can receive events (as written by SharedMemoryEventManager) from Shared Memory
		 */
		class SharedMemoryEventReceiver : public SharedMemoryManager
		{
		public:
			/**
			 * \brief Connect to a Shared Memory segment using the given parameters
			 * \param shm_key Key of the Shared Memory segment
			 * \param buffer_count 
			 * \param max_buffer_size 
			 */
			SharedMemoryEventReceiver(int shm_key, size_t buffer_count, size_t max_buffer_size);
			/**
			 * \brief SharedMemoryEventReceiver Destructor
			 */
			virtual ~SharedMemoryEventReceiver() = default;

			/**
			 * \brief Get the Event header
			 * \param err Flag used to indicate if an error has occurred
			 * \return Shared pointer to RawEventHeader from buffer
			 */
			std::shared_ptr<detail::RawEventHeader> ReadHeader(bool& err);
			/**
			 * \brief Get a set of Fragment Types present in the event
			 * \param err Flag used to indicate if an error has occurred
			 * \return std::set of Fragment::type_t of all Fragment types in the event
			 */
			std::set<Fragment::type_t> GetFragmentTypes(bool& err);
			/**
			 * \brief Get a pointer to the Fragments of a given type in the event
			 * \param err Flag used to indicate if an error has occurred
			 * \param type Type of Fragments to get. 
			 * \return std::unique_ptr to a Fragments object containing returned Fragment objects
			 */
			std::unique_ptr<Fragments> GetFragmentsByType(bool& err, Fragment::type_t type);
			/**
			 * \brief Release the buffer currently being read to the Empty state
			 */
			void ReleaseBuffer();

		private:
			int current_read_buffer_;
			std::shared_ptr<detail::RawEventHeader> current_header_;
		};

		/**
		 * \brief The SharedMemoryReader is a class which implements the methods needed by art::Source
		 */
		struct SharedMemoryReader
		{
			/**
			 * \brief Copy Constructor is deleted
			 */
			SharedMemoryReader(SharedMemoryReader const&) = delete;

			/**
			 * \brief Copy Assignment operator is deleted
			 * \return SharedMemoryReader copy
			 */
			SharedMemoryReader& operator=(SharedMemoryReader const&) = delete;

			art::SourceHelper const pmaker; ///< An art::SourceHelper instance
			std::unique_ptr<SharedMemoryEventReceiver> incoming_events; ///< The events from the EventStore
			artdaq::TimeUtils::seconds waiting_time; ///< The amount of time to wait for an event from the queue
			bool resume_after_timeout; ///< Whether to resume if the dequeue action times out
			std::string pretend_module_name; ///< The module name to store data under
			std::string unidentified_instance_name; ///< The name to use for unknown Fragment types
			bool shutdownMsgReceived; ///< Whether a shutdown message has been received
			bool outputFileCloseNeeded; ///< If an explicit output file close message is needed
			size_t bytesRead; ///< running total of number of bytes received

			/**
			 * \brief SharedMemoryReader Constructor
			 * \param ps ParameterSet used for configuring SharedMemoryReader
			 * \param help art::ProductRegistryHelper which is used to inform art about different Fragment types
			 * \param pm art::SourceHelper used to initalize the SourceHelper member
			 *
			 * \verbatim
			 * SharedMemoryReader accepts the following Parameters:
			 * "waiting_time" (Default: 86400.0): The maximum amount of time to wait for an event from the queue
			 * "resume_after_timeout" (Default: true): Whether to continue receiving data after a timeout
			 * "raw_data_label" (Default: "daq"): The label to use for all raw data
			 * "shared_memory_key" (Default: 0xBEE7): The key for the shared memory segment
			 * "buffer_count" (Default: 20): The number of buffers in the shared memory segment
			 * "max_buffer_size" (Default: 1024): The size of the buffers in the shared memory segment
			 * \endverbatim
			 */
			SharedMemoryReader(fhicl::ParameterSet const& ps,
							   art::ProductRegistryHelper& help,
							   art::SourceHelper const& pm);

			/**
			 * \brief SharedMemoryReader Constructor
			 * \param ps ParameterSet used for configuring SharedMemoryReader
			 * \param help art::ProductRegistryHelper which is used to inform art about different Fragment types
			 * \param pm art::SourceHelper used to initalize the SourceHelper member
			 *
			 * This constructor calls the three-parameter constructor, the art::MasterProductRegistry parameter is discarded.
			 */
			SharedMemoryReader(fhicl::ParameterSet const& ps,
							   art::ProductRegistryHelper& help,
							   art::SourceHelper const& pm,
							   art::MasterProductRegistry&) : SharedMemoryReader(ps, help, pm) {}

			/**
			 * \brief SharedMemoryReader destructor
			 */
			virtual ~SharedMemoryReader() = default;

			/**
			 * \brief Emulate closing a file. No-Op.
			 */
			void closeCurrentFile();

			/**
			 * \brief Emulate opening a file
			 * \param[out] fb art::FileBlock object
			 */
			void readFile(std::string const&, art::FileBlock*& fb);

			/**
			 * \brief Whether more data is expected from the SharedMemoryReader
			 * \return True unless a shutdown message has been received in readNext
			 */
			bool hasMoreData() const { return (!shutdownMsgReceived); }

			/**
			 * \brief Dequeue a RawEvent and declare its Fragment contents to art, creating
			 * Run, SubRun, and EventPrincipal objects as necessary
			 * \param[in] inR Input art::RunPrincipal
			 * \param[in] inSR Input art::SubRunPrincipal
			 * \param[out] outR Output art::RunPrincipal
			 * \param[out] outSR  Output art::SubRunPrincipal
			 * \param[out] outE Output art::EventPrincipal
			 * \return Whether an event was returned
			 */
			bool readNext(art::RunPrincipal* const & inR,
						  art::SubRunPrincipal* const & inSR,
						  art::RunPrincipal*& outR,
						  art::SubRunPrincipal*& outSR,
						  art::EventPrincipal*& outE);

			std::map<Fragment::type_t, std::string> fragment_type_map_; ///< The Fragment type names that this SharedMemoryReader knows about
			unsigned readNext_calls_; ///< The number of times readNext has been called
		};
	} // detail
} // artdaq

namespace art
{
	/**
	* \brief  Specialize an art source trait to tell art that we don't care about
	* source.fileNames and don't want the files services to be used.
	*/
	template <>
	struct Source_generator<artdaq::detail::SharedMemoryReader>
	{
		static constexpr bool value = true; ///< Used to suppress use of file services on art Source
	};
}

#endif /* artdaq_ArtModules_detail_SharedMemoryReader_hh */
