#include "canvas/Persistency/Provenance/Parentage.h"
#include "canvas/Persistency/Provenance/ParentageID.h"

#include <map>

namespace art {
  typedef std::map<ParentageID const, Parentage> ParentageMap;
}
