#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ParentageID.h"
#include "canvas/Persistency/Common/Wrapper.h"

#if ART_HEX_VERSION >= 0x20703
namespace art
{
	typedef art::ParentageRegistry::collection_type ParentageMap;
}
#endif
namespace
{
	/**
	 * \brief Contains information needed by art for reconstructing artdaq objects
	 */
	struct dictionary
	{
		std::pair<const art::ParentageID, art::Parentage> pp; ///< A pair relating a ParentageID to a Parentage
		art::ParentageMap pm; ///< A art::ParentageMap
	};
}
