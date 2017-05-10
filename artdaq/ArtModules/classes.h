#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ParentageID.h"
#include "canvas/Persistency/Common/Wrapper.h"

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
