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

#include "TBufferFile.h"

#include <memory>
#include <string>
#include <iostream>

// JCF, Jul-18-2016

// If LOGDEBUG is defined, the mf::LogDebug calls will print into the
// aggregator logfiles using artdaq v1_13_00; this creates HUGE files
// and slows things down considerably, so only define this if you're
// troubleshooting
//#define LOGDEBUG

namespace art
{
	template <typename T>
	T* ReadObjectAny(const std::unique_ptr<TBufferFile>& infile, const std::string& className, const std::string& callerName)
	{
		static TClass* tclassPtr = TClass::GetClass(className.c_str());

		if (tclassPtr == nullptr)
		{
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

		T* ptr = reinterpret_cast<T*>(infile->ReadObjectAny(tclassPtr));
#ifdef LOGDEBUG
	mf::LogDebug(callerName) << "ReadObjectAny: Got object of class " << className << 
	  ", located at " << static_cast<void*>(ptr);
#endif

		return ptr;
	}

	template <typename T>
	void printProcessHistoryID(const std::string& label, const T& object)
	{
		(void)label; // Otherwise we get an error if LOGDEBUG isn't defined, since description won't be used

		if (art::debugit() >= 1)
		{
			if (object->processHistoryID().isValid())
			{
				std::ostringstream OS;
				object->processHistoryID().print(OS);
#ifdef LOGDEBUG
	mf::LogDebug("printProcessHistoryID") << label << ": "
		  << "ProcessHistoryID: '"
		  << OS.str() << "'\n";
#endif
			}
			else
			{
#ifdef LOGDEBUG
	mf::LogDebug("printProcessHistoryID") << label << ": "
		   << "ProcessHistoryID: 'INVALID'\n";
#endif
			}
		}
	}

	template <typename T>
	void printProcessMap(const T& mappable, const std::string description)
	{
		(void) description; // Otherwise we get an error if LOGDEBUG isn't defined, since description won't be used

#ifdef LOGDEBUG
	mf::LogDebug("printProcessMap") << "Got " << description << "\n";
#endif

		if (art::debugit() >= 1)
		{
#ifdef LOGDEBUG
	  mf::LogDebug("printProcessMap") << "Dumping " << description << "...\n";
	  mf::LogDebug("printProcessMap") << "Size: "
		<< (unsigned long) mappable.size() << '\n';
#endif
			for (auto I = mappable.begin(), E = mappable.end(); I != E; ++I)
			{
				std::ostringstream OS;
				I->first.print(OS);
#ifdef LOGDEBUG
	mf::LogDebug("printProcessMap") << description << ": id: '" << OS.str() << "'\n";
#endif
				OS.str("");
#ifdef LOGDEBUG
	mf::LogDebug("printProcessMap") << description << ": data.size(): "
		  << I->second.data().size() << '\n';
#endif
				I->second.data().back().id().print(OS);

#ifdef LOGDEBUG
	mf::LogDebug("printProcessMap") << description << ": data.back().id(): '"
		  << OS.str() << "'\n";
#endif
			}
		}
	}
}


#endif
