#include "artdaq/ArtModules/ArtdaqInputHelper.hh"
#include "artdaq/ArtModules/detail/ShmemWrapper.hh"

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
typedef art::Source<ArtdaqInputHelper<ShmemWrapper>> ArtdaqInput;
}  // namespace art

DEFINE_ART_INPUT_SOURCE(art::ArtdaqInput)
