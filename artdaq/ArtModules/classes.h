#ifdef CANVAS
#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ParentageID.h"
#include "canvas/Persistency/Common/Wrapper.h"
#else
#include "art/Persistency/Provenance/ParentageRegistry.h"
#include "art/Persistency/Provenance/ParentageID.h"
#include "art/Persistency/Common/Wrapper.h"
#endif

namespace {
  struct dictionary {
    std::pair<const art::ParentageID, art::Parentage> pp;
    art::ParentageMap pm;
  };
}
