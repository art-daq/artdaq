#ifndef artdaq_ArtModules_InputUtilities_hh
#define artdaq_ArtModules_InputUtilities_hh

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/Source.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Persistency/Common/EDProduct.h"
#include "art/Persistency/Common/Wrapper.h"
#include "art/Persistency/Provenance/BranchDescription.h"
#include "art/Persistency/Provenance/BranchIDList.h"
#include "art/Persistency/Provenance/BranchIDListHelper.h"
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "art/Persistency/Provenance/BranchKey.h"
#include "art/Persistency/Provenance/History.h"
#include "art/Persistency/Provenance/MasterProductRegistry.h"
#include "art/Persistency/Provenance/ParentageRegistry.h"
#include "art/Persistency/Provenance/ProcessConfiguration.h"
#include "art/Persistency/Provenance/ProcessHistory.h"
#include "art/Persistency/Provenance/ProcessHistoryID.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductList.h"
#include "art/Persistency/Provenance/ProductMetaData.h"
#include "art/Persistency/Provenance/ProductProvenance.h"

#include "TBufferFile.h"

#include <memory>
#include <string>

namespace art {

template <typename T> 
T* ReadObjectAny(const std::unique_ptr<TBufferFile>& infile, const std::string& className) {

  static TClass* tclassPtr = TClass::GetClass(className.c_str());

  if (tclassPtr == nullptr) {
    throw art::Exception(art::errors::DictionaryNotFound) <<
      "artdaq::ReadObjectAny: "
      "Could not get TClass for " << className << "!";
  }

  // JCF, May-24-2016

  // Be aware of the following from the TBufferFile documentation,
  // concerning TBufferFile::ReadObjectAny:

  // " In case of multiple inheritance, the return value might not be
  // the real beginning of the object in memory. You will need to use
  // a dynamic_cast later if you need to retrieve it."

  return reinterpret_cast<T*>( infile->ReadObjectAny(tclassPtr) );
}

}

#endif
