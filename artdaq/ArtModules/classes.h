#include "canvas/Persistency/Provenance/ParentageRegistry.h"
#include "canvas/Persistency/Provenance/ParentageID.h"
#include "canvas/Persistency/Common/Wrapper.h"

namespace {
  struct dictionary {
    std::pair<const art::ParentageID, art::Parentage> pp;
    art::ParentageMap pm;
  };
}
