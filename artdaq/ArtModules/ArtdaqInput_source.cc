#include "artdaq/ArtModules/ArtdaqInputHelper.hh"
#include "artdaq/ArtModules/detail/ShmemWrapper.hh"

#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/IO/Sources/Source.h"

/**
 * \brief Namespace used for classes that interact directly with art
 */
namespace art {
/**
 * \brief Trait definition (must precede source typedef).
 */
template<>
struct Source_generator<ArtdaqInputHelper<ShmemWrapper>>
{
	static constexpr bool value = true;  ///< dummy parameter
};

// Source declaration.
/**
 * \brief ArtdaqInput is an art::Source using an ArtdaqInputHelper-wrapped ShmemWrapper
 */
using ArtdaqInput = art::Source<ArtdaqInputHelper<ShmemWrapper>>;
}  // namespace art

DEFINE_ART_INPUT_SOURCE(art::ArtdaqInput)
