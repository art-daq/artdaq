#include "artdaq/ArtModules/ArtdaqInput.hh"
#include "artdaq/ArtModules/NetMonWrapper.hh"

namespace art
{
	// Trait definition (must precede source typedef).
	template <>
	struct Source_generator<ArtdaqInput<NetMonWrapper>>
	{
		static constexpr bool value = true;
	};

	// Source declaration.
	typedef art::Source<ArtdaqInput<NetMonWrapper>> NetMonInput;
} // namespace art

DEFINE_ART_INPUT_SOURCE(art::NetMonInput)
