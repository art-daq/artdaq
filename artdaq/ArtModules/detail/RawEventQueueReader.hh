#ifndef artdaq_ArtModules_detail_RawEventQueueReader_hh
#define artdaq_ArtModules_detail_RawEventQueueReader_hh

#include "art/Framework/Core/Frameworkfwd.h"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "artdaq-core/Core/GlobalQueue.hh"
#include "fhiclcpp/ParameterSet.h"

#include <string>
#include <map>

namespace artdaq
{
	namespace detail
	{
		/**
		 * \brief The RawEventQueueReader is a class which implements the methods needed by art::Source
		 */
		struct RawEventQueueReader
		{
			/**
			 * \brief Copy Constructor is deleted
			 */
			RawEventQueueReader(RawEventQueueReader const&) = delete;

			/**
			 * \brief Copy Assignment operator is deleted
			 * \return RawEventQueueReader copy
			 */
			RawEventQueueReader& operator=(RawEventQueueReader const&) = delete;

			art::SourceHelper const pmaker; ///< An art::SourceHelper instance
			RawEventQueue& incoming_events; ///< The events from the EventStore
			artdaq::TimeUtils::seconds waiting_time; ///< The amount of time to wait for an event from the queue
			bool resume_after_timeout; ///< Whether to resume if the dequeue action times out
			std::string pretend_module_name; ///< The module name to store data under
			std::string unidentified_instance_name; ///< The name to use for unknown Fragment types
			bool shutdownMsgReceived; ///< Whether a shutdown message has been received
			bool outputFileCloseNeeded; ///< If an explicit output file close message is needed
			size_t bytesRead; ///< running total of number of bytes dequeued

			/**
			 * \brief RawEventQueueReader Constructor
			 * \param ps ParameterSet used for configuring RawEventQueueReader
			 * \param help art::ProductRegistryHelper which is used to inform art about different Fragment types
			 * \param pm art::SourceHelper used to initalize the SourceHelper member
			 *
			 * \verbatim
			 * RawEventQueueReader accepts the following Parameters:
			 * "waiting_time" (Default: 86400.0): The maximum amount of time to wait for an event from the queue
			 * "resume_after_timeout" (Default: true): Whether to continue receiving data after a timeout
			 * "raw_data_label" (Default: "daq"): The label to use for all raw data
			 * \endverbatim
			 */
			RawEventQueueReader(fhicl::ParameterSet const& ps,
								art::ProductRegistryHelper& help,
								art::SourceHelper const& pm);

			/**
			 * \brief RawEventQueueReader Constructor
			 * \param ps ParameterSet used for configuring RawEventQueueReader
			 * \param help art::ProductRegistryHelper which is used to inform art about different Fragment types
			 * \param pm art::SourceHelper used to initalize the SourceHelper member
			 *
			 * This constructor calls the three-parameter constructor, the art::MasterProductRegistry parameter is discarded.
			 */
			RawEventQueueReader(fhicl::ParameterSet const& ps,
								art::ProductRegistryHelper& help,
								art::SourceHelper const& pm,
								art::MasterProductRegistry&) : RawEventQueueReader(ps, help, pm) {}

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
			 * \brief Whether more data is expected from the RawEventQueueReader
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

			std::map<Fragment::type_t, std::string> fragment_type_map_; ///< The Fragment type names that this RawEventQueueReader knows about
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
	struct Source_generator<artdaq::detail::RawEventQueueReader>
	{
		static constexpr bool value = true; ///< Used to suppress use of file services on art Source
	};
}

#endif /* artdaq_ArtModules_detail_RawEventQueueReader_hh */
