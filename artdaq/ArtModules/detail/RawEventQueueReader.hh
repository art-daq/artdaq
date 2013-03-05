#ifndef artdaq_ArtModules_detail_RawEventQueueReader_hh
#define artdaq_ArtModules_detail_RawEventQueueReader_hh

#include "art/Framework/Core/Frameworkfwd.h"

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/PrincipalMaker.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Principal/EventPrincipal.h"
#include "art/Framework/Principal/RunPrincipal.h"
#include "art/Framework/Principal/SubRunPrincipal.h"
#include "artdaq/DAQrate/GlobalQueue.hh"
#include "fhiclcpp/ParameterSet.h"

#include <string>
#include <map>

namespace artdaq {
  namespace detail {
    struct RawEventQueueReader {
      RawEventQueueReader(RawEventQueueReader const &) = delete;
      RawEventQueueReader & operator=(RawEventQueueReader const &) = delete;

      art::PrincipalMaker const   pmaker;
      RawEventQueue       &       incoming_events;
      daqrate::seconds            waiting_time;
      bool                        resume_after_timeout;
      std::string                 pretend_module_name;
      std::string                 unidentified_instance_name;

      RawEventQueueReader(fhicl::ParameterSet const & ps,
                          art::ProductRegistryHelper & help,
                          art::PrincipalMaker const & pm);

      void closeCurrentFile();
      void readFile(std::string const & name, art::FileBlock *& fb);

      bool readNext(art::RunPrincipal * const & inR,
                    art::SubRunPrincipal * const & inSR,
                    art::RunPrincipal *& outR,
                    art::SubRunPrincipal *& outSR,
                    art::EventPrincipal *& outE);

      std::map<Fragment::type_t, std::string> fragment_type_map_;
    };

  } // detail
} // artdaq

// Specialize an art source trait to tell art that we don't care about
// source.fileNames and don't want the files services to be used.
namespace art {
  template <>
  struct Source_generator<artdaq::detail::RawEventQueueReader> {
    static constexpr bool value = true;
  };
}

#endif /* artdaq_ArtModules_detail_RawEventQueueReader_hh */
