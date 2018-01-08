#ifndef artdaq_ArtModules_detail_SharedMemoryReader_hh
#define artdaq_ArtModules_detail_SharedMemoryReader_hh

#include "artdaq/DAQdata/Globals.hh"
#include "art/Framework/Core/Frameworkfwd.h"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "artdaq-core/Core/SharedMemoryManager.hh"
#include "artdaq-core/Utilities/TimeUtils.hh"
#include "fhiclcpp/ParameterSet.h"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Data/ContainerFragment.hh"
#include "artdaq-core/Core/SharedMemoryEventReceiver.hh"
#include "art/Framework/IO/Sources/put_product_in_principal.h"
#include "canvas/Persistency/Provenance/FileFormatVersion.h"
#include <sys/time.h>


#include <string>
#include <map>
#include "artdaq-core/Data/RawEvent.hh"

namespace artdaq
{
	namespace detail
	{
		/**
		 * \brief The SharedMemoryReader is a class which implements the methods needed by art::Source
		 */
		template<std::map<artdaq::Fragment::type_t, std::string> getDefaultTypes() = artdaq::Fragment::MakeSystemTypeMap >
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
			double waiting_time; ///< The amount of time to wait for an event from the queue
			bool resume_after_timeout; ///< Whether to resume if the dequeue action times out
			std::string pretend_module_name; ///< The module name to store data under
			std::string unidentified_instance_name; ///< The name to use for unknown Fragment types
			bool shutdownMsgReceived; ///< Whether a shutdown message has been received
			bool outputFileCloseNeeded; ///< If an explicit output file close message is needed
			size_t bytesRead; ///< running total of number of bytes received
			std::unique_ptr<SharedMemoryManager> data_shm; ///< SharedMemoryManager containing data
			std::unique_ptr<SharedMemoryManager> broadcast_shm; ///< SharedMemoryManager containing broadcasts (control Fragments)

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
			 * \endverbatim
			 */
			SharedMemoryReader(fhicl::ParameterSet const& ps,
							   art::ProductRegistryHelper& help,
							   art::SourceHelper const& pm)
				: pmaker(pm)
				, waiting_time(ps.get<double>("waiting_time", 86400.0))
				, resume_after_timeout(ps.get<bool>("resume_after_timeout", true))
				, pretend_module_name(ps.get<std::string>("raw_data_label", "daq"))
				, unidentified_instance_name("unidentified")
				, shutdownMsgReceived(false)
				, outputFileCloseNeeded(false)
				, bytesRead(0)
				, fragment_type_map_(getDefaultTypes())
				, readNext_calls_(0)
			{
				// For testing
				if (ps.has_key("buffer_count") && (ps.has_key("max_event_size_bytes") || (ps.has_key("expected_fragments_per_event") && ps.has_key("max_fragment_size_bytes"))))
				{
					data_shm.reset(new SharedMemoryManager(ps.get<uint32_t>("shared_memory_key", 0xBEE70000 + getppid()), ps.get<int>("buffer_count"), ps.has_key("max_event_size_bytes") ? ps.get<size_t>("max_event_size_bytes") : ps.get<size_t>("expected_fragments_per_event") * ps.get<size_t>("max_fragment_size_bytes")));
					broadcast_shm.reset(new SharedMemoryManager(ps.get<uint32_t>("broadcast_shared_memory_key", 0xCEE70000 + getppid()), ps.get<int>("broadcast_buffer_count", 5), ps.get<size_t>("broadcast_buffer_size", 0x100000)));
				}
				incoming_events.reset(new SharedMemoryEventReceiver(ps.get<uint32_t>("shared_memory_key", 0xBEE70000 + getppid()), ps.get<uint32_t>("broadcast_shared_memory_key", 0xCEE70000 + getppid())));

				help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, unidentified_instance_name);
				for (auto it = fragment_type_map_.begin(); it != fragment_type_map_.end(); ++it)
				{
					help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, it->second);
					help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, "Container" + it->second);
				}
				auto extraTypes = ps.get<std::vector<std::pair<Fragment::type_t, std::string>>>("fragment_type_map", std::vector<std::pair<Fragment::type_t, std::string>>());
				for (auto it = extraTypes.begin(); it != extraTypes.end(); ++it)
				{
					fragment_type_map_[it->first] = it->second;
					help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, it->second);
					help.reconstitutes<Fragments, art::InEvent>(pretend_module_name, "Container" + it->second);
				}
				TLOG_INFO("SharedMemoryReader") << "SharedMemoryReader initialized with ParameterSet: " << ps.to_string() << TLOG_ENDL;
				//for(auto& type : fragment_type_map_)
				//{
				//	TLOG_INFO("SharedMemoryReader") << "Fragment Type " << type.second << " has typeid " << std::to_string(type.first) << TLOG_ENDL;
				//}
			}

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
							   art::MasterProductRegistry&) : SharedMemoryReader(ps, help, pm)
			{}

			/**
			 * \brief SharedMemoryReader destructor
			 */
			virtual ~SharedMemoryReader() = default;

			/**
			 * \brief Emulate closing a file. No-Op.
			 */
			void closeCurrentFile() {}

			/**
			 * \brief Emulate opening a file
			 * \param[out] fb art::FileBlock object
			 */
			void readFile(std::string const&, art::FileBlock*& fb)
			{
				TRACE(5, "SharedMemoryReader::readFile enter/start");
				fb = new art::FileBlock(art::FileFormatVersion(1, "RawEvent2011"), "nothing");
			}

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
						  art::EventPrincipal*& outE)
			{
				TLOG_DEBUG("SharedMemoryReader") << "readNext BEGIN" << TLOG_ENDL;
				/*if (outputFileCloseNeeded) {
				outputFileCloseNeeded = false;
				return false;
				}*/
				// Establish default 'results'
				outR = 0;
				outSR = 0;
				outE = 0;
				// Try to get an event from the queue. We'll continuously loop, either until:
				//   1) we have read a RawEvent off the queue, or
				//   2) we have timed out, AND we are told the when we timeout we
				//      should stop.
				// In any case, if we time out, we emit an informational message.

				if (shutdownMsgReceived) return false;

			start:
				bool keep_looping = true;
				bool got_event = false;
				auto sleepTimeUsec = waiting_time * 1000; // waiting_time * 1000000 us/s / 1000 reps = us/rep
				if (sleepTimeUsec > 100000) sleepTimeUsec = 100000; // Don't wait longer than 1/10th of a second
				while (keep_looping)
				{
					keep_looping = false;
					auto start_time = std::chrono::steady_clock::now();
					while (!got_event && TimeUtils::GetElapsedTime(start_time) < waiting_time)
					{
						got_event = incoming_events->ReadyForRead();
						if (!got_event)
						{
							usleep(sleepTimeUsec);
							//TLOG_INFO("SharedMemoryReader") << "Waited " << std::to_string(TimeUtils::GetElapsedTime(start_time)) << " of " << std::to_string(waiting_time) << TLOG_ENDL;
						}
					}
					if (!got_event)
					{
						TLOG_INFO("SharedMemoryReader")
							<< "InputFailure: Reading timed out in SharedMemoryReader::readNext()" << TLOG_ENDL;
						keep_looping = resume_after_timeout;
					}
				}

				if (!got_event)
				{
					TLOG_INFO("SharedMemoryReader") << "Did not receive an event from Shared Memory, returning false" << TLOG_ENDL;
					shutdownMsgReceived = true;
					return false;
				}
				TLOG_DEBUG("SharedMemoryReader") << "Got Event!" << TLOG_ENDL;

				auto errflag = false;
				auto evtHeader = incoming_events->ReadHeader(errflag);
				if (errflag) goto start; // Buffer was changed out from under reader!
				auto fragmentTypes = incoming_events->GetFragmentTypes(errflag);
				if (errflag) goto start; // Buffer was changed out from under reader!
				if (fragmentTypes.size() == 0)
				{
					TLOG_ERROR("SharedMemoryReader") << "Event has no Fragments! Aborting!" << TLOG_ENDL;
					incoming_events->ReleaseBuffer();
					return false;
				}
				auto firstFragmentType = *fragmentTypes.begin();
				TLOG_DEBUG("SharedMemoryReader") << "First Fragment type is " << std::to_string(firstFragmentType) << TLOG_ENDL;

				// We return false, indicating we're done reading, if:
				//   1) we did not obtain an event, because we timed out and were
				//      configured NOT to keep trying after a timeout, or
				//   2) the event we read was the end-of-data marker: a null
				//      pointer
				if (firstFragmentType == Fragment::EndOfDataFragmentType)
				{
					TLOG_DEBUG("SharedMemoryReader") << "Received shutdown message, returning false" << TLOG_ENDL;
					shutdownMsgReceived = true;
					incoming_events->ReleaseBuffer();
					return false;
				}

				size_t qsize = incoming_events->ReadReadyCount(); // save the qsize at this point

																  // Check the number of fragments in the RawEvent.  If we have a single
																  // fragment and that fragment is marked as EndRun or EndSubrun we'll create
																  // the special principals for that.
				art::Timestamp currentTime = time(0);

				// make new run if inR is 0 or if the run has changed
				if (inR == 0 || inR->run() != evtHeader->run_id)
				{
					outR = pmaker.makeRunPrincipal(evtHeader->run_id,
												   currentTime);
				}

				if (firstFragmentType == Fragment::EndOfRunFragmentType)
				{
					art::EventID const evid(art::EventID::flushEvent());
					outR = pmaker.makeRunPrincipal(evid.runID(), currentTime);
					outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
					outE = pmaker.makeEventPrincipal(evid, currentTime);
					incoming_events->ReleaseBuffer();
					return true;
				}
				else if (firstFragmentType == Fragment::EndOfSubrunFragmentType)
				{
					// Check if inR == 0 or is a new run
					if (inR == 0 || inR->run() != evtHeader->run_id)
					{
						outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id,
														   evtHeader->subrun_id,
														   currentTime);
						art::EventID const evid(art::EventID::flushEvent(outSR->id()));
						outE = pmaker.makeEventPrincipal(evid, currentTime);
					}
					else
					{
						// If the previous subrun was neither 0 nor flush and was identical with the current
						// subrun, then it must have been associated with a data event.  In that case, we need
						// to generate a flush event with a valid run but flush subrun and event number in order
						// to end the subrun.
						if (inSR != 0 && !inSR->id().isFlush() && inSR->subRun() == evtHeader->subrun_id)
						{
							art::EventID const evid(art::EventID::flushEvent(inR->id()));
							outSR = pmaker.makeSubRunPrincipal(evid.subRunID(), currentTime);
							outE = pmaker.makeEventPrincipal(evid, currentTime);
							// If this is either a new or another empty subrun, then generate a flush event with
							// valid run and subrun numbers but flush event number
							//} else if(inSR==0 || inSR->id().isFlush()){
						}
						else
						{
							outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id,
															   evtHeader->subrun_id,
															   currentTime);
							art::EventID const evid(art::EventID::flushEvent(outSR->id()));
							outE = pmaker.makeEventPrincipal(evid, currentTime);
							// Possible error condition
							//} else {
						}
						outR = 0;
					}
					//outputFileCloseNeeded = true;
					incoming_events->ReleaseBuffer();
					return true;
				}

				// make new subrun if inSR is 0 or if the subrun has changed
				art::SubRunID subrun_check(evtHeader->run_id, evtHeader->subrun_id);
				if (inSR == 0 || subrun_check != inSR->id())
				{
					outSR = pmaker.makeSubRunPrincipal(evtHeader->run_id,
													   evtHeader->subrun_id,
													   currentTime);
				}
				outE = pmaker.makeEventPrincipal(evtHeader->run_id,
												 evtHeader->subrun_id,
												 evtHeader->sequence_id,
												 currentTime);

				// insert the Fragments of each type into the EventPrincipal
				std::map<Fragment::type_t, std::string>::const_iterator iter_end =
					fragment_type_map_.end();
				for (auto& type_code : fragmentTypes)
				{
					std::map<Fragment::type_t, std::string>::const_iterator iter =
						fragment_type_map_.find(type_code);
					auto product = incoming_events->GetFragmentsByType(errflag, type_code);
					if (errflag) goto start; // Buffer was changed out from under reader!
					for (auto &frag : *product)
						bytesRead += frag.sizeBytes();
					if (iter != iter_end)
					{
					  if (type_code == artdaq::Fragment::ContainerFragmentType)
						{
						  std::unordered_map<std::string, std::unique_ptr<Fragments>> derived_fragments;
						  derived_fragments[iter->second] = std::make_unique<Fragments>();

						  for(size_t ii = 0; ii < product->size(); ++ii)
							{
							  ContainerFragment cf(product->at(ii));
							  auto contained_type = fragment_type_map_.find(cf.fragment_type());
							  if (contained_type != iter_end)
								{
								  auto label = iter->second + contained_type->second;
								  if (!derived_fragments.count(label))
									{
									  derived_fragments[label] = std::make_unique<Fragments>();
									}
								  derived_fragments[label]->emplace_back(std::move(product->at(ii)));
								}
							  else
								{
								  derived_fragments[iter->second]->emplace_back(std::move(product->at(ii)));
								}
							}

						  for (auto& type : derived_fragments)
							{
							  put_product_in_principal(std::move(type.second),
													   *outE,
													   pretend_module_name,
													   type.first);
							}

						}
					  else
						{
						  put_product_in_principal(std::move(product),
												   *outE,
												   pretend_module_name,
												   iter->second);
						}
					}
					else
					{
						put_product_in_principal(std::move(product),
												 *outE,
												 pretend_module_name,
												 unidentified_instance_name);
						TLOG_WARNING("SharedMemoryReader")
							<< "UnknownFragmentType: The product instance name mapping for fragment type \""
							<< ((int)type_code) << "\" is not known. Fragments of this "
							<< "type will be stored in the event with an instance name of \""
							<< unidentified_instance_name << "\"." << TLOG_ENDL;
					}
				}
				incoming_events->ReleaseBuffer();
				TLOG_ARB(10, "SharedMemoryReader") << "readNext: bytesRead=" << std::to_string(bytesRead) << " qsize=" << std::to_string(qsize) << " cap=" << std::to_string(incoming_events->size()) << " metricMan=" << (void*)metricMan << TLOG_ENDL;
				if (metricMan)
				{
					metricMan->sendMetric("bytesRead", bytesRead, "B", 5, MetricMode::Accumulate, "", true);
					metricMan->sendMetric("queue%Used", static_cast<unsigned long int>(qsize * 100 / incoming_events->size()), "%", 5, MetricMode::LastPoint, "", true);
				}

				return true;
			}

			std::map<Fragment::type_t, std::string> fragment_type_map_; ///< The Fragment type names that this SharedMemoryReader knows about
			unsigned readNext_calls_; ///< The number of times readNext has been called
		};
	} // detail
} // artdaq


#endif /* artdaq_ArtModules_detail_SharedMemoryReader_hh */
