
#include "artdaq/TransferPlugins/TransferInterface.hh"
#include "artdaq-core/Data/Fragment.hh"
#include "artdaq-core/Utilities/ExceptionHandler.hh"

#include "messagefacility/MessageLogger/MessageLogger.h"

#include <boost/tokenizer.hpp>

#include <sys/shm.h>
#include <memory>
#include <iostream>
#include <string>
#include <limits>
#include <sstream>

namespace fhicl {
	class ParameterSet;
}

// ----------------------------------------------------------------------

namespace artdaq {

	class ShmemTransfer : public artdaq::TransferInterface {

	public:

		ShmemTransfer(fhicl::ParameterSet const&, Role);
		~ShmemTransfer();

		virtual size_t receiveFragment(artdaq::Fragment& fragment,
			size_t receiveTimeout);

		virtual CopyStatus copyFragment(artdaq::Fragment& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max());
		virtual CopyStatus moveFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max());
	private:
		CopyStatus sendFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec, bool reliable = false);

		struct ShmStruct {
			size_t hasFragment;
			size_t fragmentSizeWords;
			artdaq::RawDataType fragmentInnards[2];
		};

		uint64_t max_fragment_size_words_;
		size_t send_timeout_usec_;
		int shm_segment_id_;
		ShmStruct* shm_ptr_;
		int shm_key_;

		size_t fragment_count_to_shm_;
		Role role_;
	};

}

artdaq::ShmemTransfer::ShmemTransfer(fhicl::ParameterSet const& pset, Role role) :
	TransferInterface(pset, role),
	max_fragment_size_words_(pset.get<uint64_t>("max_fragment_size_words")),
	shm_segment_id_(-1),
	shm_ptr_(NULL),
	shm_key_(pset.get<int>("shm_key", std::hash<std::string>()(uniqueLabel()))),
	fragment_count_to_shm_(0),
	role_(role)
{

	// char* keyChars = getenv("ARTDAQ_SHM_KEY");
	// if (keyChars != NULL && shm_key_ == static_cast<int>(std::hash<std::string>()(unique_label_))) {
	//   std::string keyString(keyChars);
	//   try {
	//     shm_key_ = boost::lexical_cast<int>(keyString);
	//   }
	//   catch (...) {
	//     std::stringstream errmsg;
	//     errmsg << uniqueLabel() << ": Problem performing lexical cast on " << keyString;
	//     ExceptionHandler(ExceptionHandlerRethrow::yes, errmsg.str()); 
	//   }
	// }

	// JCF, Aug-16-2016

	// Note that there's a small but nonzero chance of a race condition
	// here where another process creates the shared memory buffer
	// between the first and second calls to shmget
	
	shm_segment_id_ =
		shmget(shm_key_, (max_fragment_size_words_ * sizeof(artdaq::RawDataType)),
			0666);

	if (shm_segment_id_ == -1) {
		shm_segment_id_ =
			shmget(shm_key_, (max_fragment_size_words_ * sizeof(artdaq::RawDataType)),
				IPC_CREAT | 0666);
	}

	mf::LogDebug(uniqueLabel()) << "shm_key == " << shm_key_ << ", shm_segment_id == " << shm_segment_id_;

	if (shm_segment_id_ > -1) {
		mf::LogDebug(uniqueLabel())
			<< "Created/fetched shared memory segment with ID = " << shm_segment_id_
			<< " and size " << (max_fragment_size_words_ * sizeof(artdaq::RawDataType))
			<< " bytes";
		shm_ptr_ = (ShmStruct*)shmat(shm_segment_id_, 0, 0);
		if (shm_ptr_ && shm_ptr_ != (void *)-1) {
			if (role_ == Role::kReceive) {
				shm_ptr_->hasFragment = 0;
			}
			mf::LogDebug(uniqueLabel())
				<< "Attached to shared memory segment at address "
				<< std::hex << shm_ptr_ << std::dec;
		}
		else {
			mf::LogError(uniqueLabel()) << "Failed to attach to shared memory segment "
				<< shm_segment_id_;
		}
	}
	else {
		mf::LogError(uniqueLabel()) << "Failed to connect to shared memory segment"
			<< ", errno = " << errno << ".  Please check "
			<< "if a stale shared memory segment needs to "
			<< "be cleaned up. (ipcs, ipcrm -m <segId>)";
	}
}

artdaq::ShmemTransfer::~ShmemTransfer() {

	if (shm_ptr_) {
		shmdt(shm_ptr_);
		shm_ptr_ = NULL;
	}

	if (role_ == Role::kReceive && shm_segment_id_ > -1) {
		shmctl(shm_segment_id_, IPC_RMID, NULL);
	}
}


size_t artdaq::ShmemTransfer::receiveFragment(artdaq::Fragment& fragment,
	size_t receiveTimeout) {

	if (shm_ptr_) {
		size_t loopCount = 0;
		size_t sleepTime = 1000; // microseconds
		size_t nloops = receiveTimeout / sleepTime;

		while (shm_ptr_->hasFragment == 0 && loopCount < nloops) {
			usleep(sleepTime);
			++loopCount;
		}

		while (shm_ptr_->hasFragment >= 1) {
			auto initialHF = shm_ptr_->hasFragment;
			// JCF, Jul-7-2016

			// Calling artdaq::Fragment::resize with the argument
			// shm_ptr_->fragmentSizeWords actually allocates more memory
			// for "fragment" than is needed as shm_ptr_->fragmentSizeWords
			// is the FULL size of the received fragment, not just the size
			// of its payload. We correct for this below.

			fragment.resize(shm_ptr_->fragmentSizeWords);

			artdaq::RawDataType* fragAddr = fragment.headerAddress();
			size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);
			memcpy(fragAddr, &shm_ptr_->fragmentInnards[0], fragSize);

			auto diff = shm_ptr_->hasFragment - initialHF;
			shm_ptr_->hasFragment--;

			if (diff > 0) continue;

			auto wordsOfHeaderAndMetadata = &*fragment.dataBegin() - &*fragment.headerBegin();
			fragment.resize(shm_ptr_->fragmentSizeWords - wordsOfHeaderAndMetadata);

			if (fragment.type() != artdaq::Fragment::DataFragmentType) {
				mf::LogDebug(uniqueLabel())
					<< "Received fragment from shared memory, type =" << ((int)fragment.type())
					<< ", sequenceID = " << fragment.sequenceID()
					<< ", source_rank = " << source_rank();
			}

			return source_rank();
		}

			return artdaq::TransferInterface::RECV_TIMEOUT;
	}
	else {

		mf::LogError(uniqueLabel()) << "Error in shared memory transfer plugin: pointer to shared memory segment is null, will sleep for " << receiveTimeout / 1.0e6 << " seconds and then return a timeout";
		usleep(receiveTimeout);
		return artdaq::TransferInterface::RECV_TIMEOUT; // Should we EVER get shm_ptr_ == 0?
	}
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::copyFragment(artdaq::Fragment& fragment, size_t send_timeout_usec)
{
	return sendFragment(std::move(fragment), send_timeout_usec, false);
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::moveFragment(artdaq::Fragment&& fragment,
	size_t send_timeout_usec) {
	return sendFragment(std::move(fragment), send_timeout_usec, true);
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::sendFragment(artdaq::Fragment&& fragment, size_t send_timeout_usec, bool reliableMode)
{
	size_t fragmentType = fragment.type();

	if (!shm_ptr_) { return CopyStatus::kErrorNotRequiringException; }

	// wait for the shm to become free, if requested                                           
	if (send_timeout_usec > 0) {
		size_t sleepTime = (send_timeout_usec / 10);
		int loopCount = 0;
		while (shm_ptr_->hasFragment == 1 && loopCount < 10) {
			if (fragmentType != artdaq::Fragment::DataFragmentType) {
				mf::LogDebug(uniqueLabel()) << "Trying to copy fragment of type "
					<< fragmentType
					<< ", loopCount = "
					<< loopCount;
			}
			usleep(sleepTime);
			++loopCount;
		}
	}

	// copy the fragment if the shm is available                                               
	if (shm_ptr_->hasFragment == 0 && reliableMode) {
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

			shm_ptr_->hasFragment++;

			++fragment_count_to_shm_;
			if ((fragment_count_to_shm_ % 250) == 0) {
				mf::LogDebug(uniqueLabel()) << "Copied " << fragment_count_to_shm_
					<< " fragments to shared memory in this run.";
			}

			return CopyStatus::kSuccess;
		}
		else {
			mf::LogWarning(uniqueLabel()) << "Fragment invalid for shared memory! "
				<< "fragment address and size = "
				<< fragAddr << " " << fragSize << " "
				<< "sequence ID, fragment ID, and type = "
				<< fragment.sequenceID() << " "
				<< fragment.fragmentID() << " "
				<< ((int)fragment.type());
			return CopyStatus::kErrorNotRequiringException;
		}
	}

	return CopyStatus::kTimeout;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::ShmemTransfer)

// Local Variables:
// mode: c++
// End:
