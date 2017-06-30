#ifndef artdaq_DAQrate_detail_SharedMemoryEventReceiver_hh
#define artdaq_DAQrate_detail_SharedMemoryEventReceiver_hh

#include <set>

#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/RawEvent.hh"

namespace artdaq
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
			 * \param type Type of Fragments to get. (Use InvalidFragmentType to get all Fragments)
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
} // artdaq

#endif /* artdaq_ArtModules_detail_SharedMemoryEventReceiver_hh */
