
#include "artdaq/ArtModules/ArtdaqInput.hh"
#include "artdaq/TransferPlugins/TransferWrapper.hh"

namespace art {

// Trait definition (must precede source typedef).
template <>
struct Source_generator<ArtdaqInput<artdaq::TransferWrapper>> {
    static constexpr bool value = true;
};

// Source declaration.
  typedef art::Source<ArtdaqInput<artdaq::TransferWrapper>> TransferInput;

} // namespace art

DEFINE_ART_INPUT_SOURCE(art::TransferInput)

