#ifndef artdaq_ArtModules_InputUtilities_hh
#define artdaq_ArtModules_InputUtilities_hh

#include "art/Framework/Core/FileBlock.h"
#include "art/Framework/Core/InputSourceMacros.h"
#include "art/Framework/Core/ProductRegistryHelper.h"
#include "art/Framework/IO/Sources/Source.h"
#include "art/Framework/IO/Sources/SourceHelper.h"
#include "art/Framework/IO/Sources/SourceTraits.h"
#include "art/Framework/Services/Registry/ServiceHandle.h"
#include "art/Persistency/Provenance/BranchIDListHelper.h"
#include "art/Persistency/Provenance/BranchIDListRegistry.h"
#include "art/Persistency/Provenance/MasterProductRegistry.h"
#include "art/Persistency/Provenance/ProcessHistoryRegistry.h"
#include "art/Persistency/Provenance/ProductMetaData.h"

#ifdef CANVAS
#include "canvas/Persistency/Common/EDProduct.h"
#include "canvas/Persistency/Common/Wrapper.h"
#include "canvas/Persistency/Provenance/BranchDescription.h"
#include "canvas/Persistency/Provenance/BranchIDList.h"
#include "canvas/Persistency/Provenance/BranchKey.h"
#include "canvas/Persistency/Provenance/History.h"
#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ProcessConfiguration.h"
#include "canvas/Persistency/Provenance/ProcessHistory.h"
#include "canvas/Persistency/Provenance/ProcessHistoryID.h"
#include "canvas/Persistency/Provenance/ProductList.h"
#include "canvas/Persistency/Provenance/ProductProvenance.h"
#include "canvas/Utilities/DebugMacros.h"
#else
#include "art/Persistency/Common/EDProduct.h"
#include "art/Persistency/Common/Wrapper.h"
#include "art/Persistency/Provenance/BranchDescription.h"
#include "art/Persistency/Provenance/BranchIDList.h"
#include "art/Persistency/Provenance/BranchKey.h"
#include "art/Persistency/Provenance/History.h"
#include "art/Persistency/Provenance/ParentageRegistry.h"
#include "art/Persistency/Provenance/ProcessConfiguration.h"
#include "art/Persistency/Provenance/ProcessHistory.h"
#include "art/Persistency/Provenance/ProcessHistoryID.h"
#include "art/Persistency/Provenance/ProductList.h"
#include "art/Persistency/Provenance/ProductProvenance.h"
//#include "art/Utilities/DebugMacros.h"
#endif

#include "TBufferFile.h"

#include <memory>
#include <string>
#include <iostream>

namespace art {

  template <typename T> 
  T* ReadObjectAny(const std::unique_ptr<TBufferFile>& infile, const std::string& className, const std::string& callerName) {

    static TClass* tclassPtr = TClass::GetClass(className.c_str());

    if (tclassPtr == nullptr) {
      throw art::Exception(art::errors::DictionaryNotFound) <<
	callerName << " call to ReadObjectAny: "
	"Could not get TClass for " << className << "!";
    }

    // JCF, May-24-2016

    // Be aware of the following from the TBufferFile documentation,
    // concerning TBufferFile::ReadObjectAny:

    // " In case of multiple inheritance, the return value might not be
    // the real beginning of the object in memory. You will need to use
    // a dynamic_cast later if you need to retrieve it."

    T* ptr = reinterpret_cast<T*>( infile->ReadObjectAny(tclassPtr) );

    mf::LogDebug(callerName) << "ReadObjectAny: Got object of class " << className << 
      ", located at " << static_cast<void*>(ptr);

    return ptr;
  }

  template <typename T>
  void printProcessHistoryID(const std::string& label, const T& object ) {

    if (art::debugit() >= 1) {
      if (object->processHistoryID().isValid()) {
	std::ostringstream OS;
	object->processHistoryID().print(OS);
	mf::LogDebug("printProcessHistoryID") << label << ": "
		  << "ProcessHistoryID: '"
		  << OS.str() << "'\n";
      }
      else {
	mf::LogDebug("printProcessHistoryID") << label << ": "
		   << "ProcessHistoryID: 'INVALID'\n";
      }
    }
  }

  template <typename T>
  void printProcessMap(const T& mappable, const std::string description ) {
    mf::LogDebug("printProcessMap") << "Got " << description << "\n";
    
    if (art::debugit() >= 1) {
      mf::LogDebug("printProcessMap") << "Dumping " << description << "...\n";
      mf::LogDebug("printProcessMap") << "Size: "
		<< (unsigned long) mappable.size() << '\n';
      for (auto I = mappable.begin(), E = mappable.end(); I != E; ++I) {
	std::ostringstream OS;
	I->first.print(OS);
	mf::LogDebug("printProcessMap") << description << ": id: '" << OS.str() << "'\n";
	OS.str("");
	mf::LogDebug("printProcessMap") << description << ": data.size(): "
		  << I->second.data().size() << '\n';
	I->second.data().back().id().print(OS);

	mf::LogDebug("printProcessMap") << description << ": data.back().id(): '"
		  << OS.str() << "'\n";
      }
    }
  }
}


#endif
