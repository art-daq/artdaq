#include "art/Framework/IO/Sources/Source.h"
#include "artdaq/ArtModules/detail/RTIDDSReader.hh"
#include "art/Framework/Core/InputSourceMacros.h"

#include <string>
using std::string;

namespace artdaq {

  typedef art::Source<detail::RTIDDSReader> RTIDDSInput;
}

DEFINE_ART_INPUT_SOURCE(artdaq::RTIDDSInput)
