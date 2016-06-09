#ifndef artdaq_ArtModules_shmemTransfer_h
#define artdaq_ArtModules_shmemTransfer_h

#include "artdaq/ArtModules/TransferInterface.h"
#include "artdaq/DAQrate/RHandles.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "messagefacility/MessageLogger/MessageLogger.h"

#include <boost/tokenizer.hpp>

#include <sys/shm.h>
#include <memory>
#include <iostream>
#include <string>
#include <limits>

namespace fhicl {
class ParameterSet;
}

// ----------------------------------------------------------------------

namespace artdaq {

class shmemTransfer : public artdaq::TransferInterface {

public:

  shmemTransfer(fhicl::ParameterSet const& , Role );
  ~shmemTransfer();

  virtual size_t receiveFragmentFrom(artdaq::Fragment& fragment,
				     size_t receiveTimeout);

  virtual void copyFragmentTo(bool& fragmentHasBeenCopied,
			      bool& esrHasBeenCopied,
			      bool& eodHasBeenCopied,
			      artdaq::Fragment& fragment,
			      size_t send_timeout_usec = std::numeric_limits<size_t>::max());
private:

  struct ShmStruct {
    size_t hasFragment;
    size_t fragmentSizeWords;
    artdaq::RawDataType fragmentInnards[2];
  };

  uint64_t max_fragment_size_words_;
  size_t first_data_sender_rank_; // Only here to mimic AggregatorCore code
  size_t send_timeout_usec_;
  int shm_segment_id_;
  ShmStruct* shm_ptr_;
  size_t fragment_count_to_shm_;
  Role role_;
  const std::string name_;
};

}

artdaq::shmemTransfer::shmemTransfer(fhicl::ParameterSet const& pset, Role role) :
  TransferInterface(pset, role),
  max_fragment_size_words_(pset.get<uint64_t>("max_fragment_size_words")),
  first_data_sender_rank_(pset.get<size_t>("first_event_builder_rank", std::numeric_limits<size_t>::max())),
  shm_segment_id_(-1),
  shm_ptr_(nullptr),
  fragment_count_to_shm_(0),
  role_(role),
  name_("shmemTransfer")
{
  
  // Should there be a special function to be called inside of
  // AggregatorCore::process_fragments() which contains these lines,
  // or can we leave them in the constructor?

  int shmKey = 0x40470000;
  char* keyChars = getenv("ARTDAQ_SHM_KEY");
  if (keyChars != nullptr) {
    std::string keyString(keyChars);
    try {
      shmKey = boost::lexical_cast<int>(keyString);
    }
    catch (...) {}
  }

  shm_segment_id_ =
    shmget(shmKey, (max_fragment_size_words_ * sizeof(artdaq::RawDataType)),
           IPC_CREAT | 0666);
  
  if (shm_segment_id_ > -1) {
    mf::LogDebug(name_)
      << "Created/fetched shared memory segment with ID = " << shm_segment_id_
      << " and size " << (max_fragment_size_words_ * sizeof(artdaq::RawDataType))
      << " bytes";
    shm_ptr_ = (ShmStruct*) shmat(shm_segment_id_, 0, 0);
    if (shm_ptr_) {
      if (role_ == Role::receive) {
        shm_ptr_->hasFragment = 0;
      }
      mf::LogDebug(name_)
        << "Attached to shared memory segment at address 0x"
        << std::hex << shm_ptr_ << std::dec;
    }
    else {
      mf::LogError(name_) << "Failed to attach to shared memory segment "
			  << shm_segment_id_;
    }
  }
  else {
    mf::LogError(name_) << "Failed to connect to shared memory segment"
			<< ", errno = " << errno << ".  Please check "
			<< "if a stale shared memory segment needs to "
			<< "be cleaned up. (ipcs, ipcrm -m <segId>)";
  }
}

artdaq::shmemTransfer::~shmemTransfer() {
 
  if (shm_ptr_) {
    shmdt(shm_ptr_);
    shm_ptr_ = nullptr;
  }

  if (role_ == Role::receive && shm_segment_id_ > -1) {
    shmctl(shm_segment_id_, IPC_RMID, NULL);
  }
}


size_t artdaq::shmemTransfer::receiveFragmentFrom(artdaq::Fragment& fragment,
					size_t receiveTimeout) {

  if (shm_ptr_) {
    int loopCount = 0;
    int nloops = 10;
    size_t sleepTime = receiveTimeout / nloops;
    while (shm_ptr_->hasFragment == 0 && loopCount < nloops) {
      usleep(sleepTime);
      ++loopCount;
    }

    if (shm_ptr_->hasFragment == 1) {
      fragment.resize(shm_ptr_->fragmentSizeWords);
      artdaq::RawDataType* fragAddr = fragment.headerAddress();
      size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);
      memcpy(fragAddr, &shm_ptr_->fragmentInnards[0], fragSize);
      shm_ptr_->hasFragment = 0;

      if (fragment.type() != artdaq::Fragment::DataFragmentType) {
	mf::LogDebug(name_)
          << "Received fragment from shared memory, type ="
          << ((int)fragment.type()) << ", sequenceID = "
          << fragment.sequenceID();
      }

      return first_data_sender_rank_;
    } else {
      return artdaq::RHandles::RECV_TIMEOUT;
    }
  } else {

    usleep(receiveTimeout);

    // JCF, Jun-9-2016

    // AggregatorCore code on develop branch returns
    // artdaq::RHandles::RECV_TIMEOUT, but having a null shm_ptr_
    // strikes me as an error...

    mf::LogError(name_) << "Call to shmemTransfer::receiveFragmentFrom with null shm_ptr_";
    throw cet::exception(name_) << "Call to shmemTransfer::receiveFragmentFrom with null shm_ptr_"; 
  }
}

void artdaq::shmemTransfer::copyFragmentTo(bool& fragmentWasCopied,
					   bool& esrWasCopied,
					   bool& eodWasCopied,
					   artdaq::Fragment& fragment,
					   size_t send_timeout_usec) {

  if (fragmentWasCopied) {return;}
  
  size_t fragmentType = fragment.type();
  if (fragmentType == artdaq::Fragment::EndOfSubrunFragmentType &&
      esrWasCopied) {return;}
  if (fragmentType == artdaq::Fragment::EndOfDataFragmentType &&
      eodWasCopied) {return;}

  if (shm_ptr_ == nullptr) {return;}

  // wait for the shm to become free, if requested                                           
  if (send_timeout_usec > 0) {
    size_t sleepTime = (send_timeout_usec / 10);
    int loopCount = 0;
    while (shm_ptr_->hasFragment == 1 && loopCount < 10) {
      if (fragmentType != artdaq::Fragment::DataFragmentType) {
	mf::LogDebug(name_) << "Trying to copy fragment of type "
			    << fragmentType
			    << ", loopCount = "
			    << loopCount;
      }
      usleep(sleepTime);
      ++loopCount;
    }
  }

  // copy the fragment if the shm is available                                               
  if (shm_ptr_->hasFragment == 0) {
    artdaq::RawDataType* fragAddr = fragment.headerAddress();
    size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);

    // 10-Sep-2013, KAB - protect against large events and                                   
    // invalid events (and large, invalid events)                                            
    if (fragment.type() != artdaq::Fragment::InvalidFragmentType &&
        fragSize < ((max_fragment_size_words_ *
                     sizeof(artdaq::RawDataType)) -
                    sizeof(ShmStruct))) {
      memcpy(&shm_ptr_->fragmentInnards[0], fragAddr, fragSize);
      shm_ptr_->fragmentSizeWords = fragment.size();

      fragmentWasCopied = true;
      if (fragmentType == artdaq::Fragment::EndOfSubrunFragmentType) {
        esrWasCopied = true;
      }
      if (fragmentType == artdaq::Fragment::EndOfDataFragmentType) {
        eodWasCopied = true;
      }

      shm_ptr_->hasFragment = 1;

      ++fragment_count_to_shm_;
      if ((fragment_count_to_shm_ % 250) == 0) {
	mf::LogDebug(name_) << "Copied " << fragment_count_to_shm_
			    << " fragments to shared memory in this run.";
      }
    }
    else {
      mf::LogWarning(name_) << "Fragment invalid for shared memory! "
			    << "fragment address and size = "
			    << fragAddr << " " << fragSize << " "
			    << "sequence ID, fragment ID, and type = "
			    << fragment.sequenceID() << " "
			    << fragment.fragmentID() << " "
			    << ((int) fragment.type());
    }
  }
}

DEFINE_ARTDAQ_TRANSFER(artdaq::shmemTransfer)

#endif /* artdaq_ArtModules_shmemTransfer_h */

// Local Variables:
// mode: c++
// End:
