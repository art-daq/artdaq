#ifndef artdaq_ArtModules_TransferServiceInterface_h
#define artdaq_ArtModules_TransferServiceInterface_h

#include "artdaq-core/Data/Fragment.hh"

class TransferServiceInterface {
public:
  
  virtual void receiveFragmentFrom(artdaq::Fragment& fragment,
			   size_t receiveTimeout) = 0;

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment) = 0;

};

#endif /* artdaq_ArtModules_TransferServiceInterface_h */

// Local Variables:
// mode: c++
// End:
