#include "art/Framework/IO/Sources/Source.h"
#include "artdaq/ArtModules/detail/RawEventQueueReader.hh"
#include "art/Framework/Core/InputSourceMacros.h"

#include <string>
using std::string;

namespace artdaq
{
	/**
	 * \brief RawInput is a typedef of art::Source<detail::RawEventQueueReader>
	 */
	typedef art::Source<detail::RawEventQueueReader> RawInput;
}

DEFINE_ART_INPUT_SOURCE(artdaq::RawInput)
