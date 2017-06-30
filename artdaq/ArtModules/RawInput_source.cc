#include "art/Framework/IO/Sources/Source.h"
#include "artdaq/ArtModules/detail/SharedMemoryReader.hh"
#include "art/Framework/Core/InputSourceMacros.h"

#include <string>
using std::string;

namespace artdaq
{
	/**
	 * \brief RawInput is a typedef of art::Source<detail::SharedMemoryReader>
	 */
	typedef art::Source<detail::SharedMemoryReader<>> RawInput;
}

DEFINE_ART_INPUT_SOURCE(artdaq::RawInput)
