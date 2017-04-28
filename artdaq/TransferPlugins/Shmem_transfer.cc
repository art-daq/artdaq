// #include "artdaq/TransferPlugins/TransferInterface.hh"
// #include "artdaq-core/Data/Fragment.hh"
// #include "artdaq-core/Utilities/ExceptionHandler.hh"

// #include "messagefacility/MessageLogger/MessageLogger.h"

#include "artdaq/DAQdata/Globals.hh"
// #include <boost/tokenizer.hpp>

#include <sys/shm.h>
// #include <memory>
// #include <iostream>
// #include <string>
// #include <limits>
// #include <sstream>
#include "artdaq/TransferPlugins/ShmemTransfer.hh"

#define SHMEM_SLEEP 0
// ----------------------------------------------------------------------


artdaq::ShmemTransfer::ShmemTransfer(fhicl::ParameterSet const& pset, Role role) :
                                                                                 TransferInterface(pset, role)
                                                                                 , shm_segment_id_(-1)
                                                                                 , shm_ptr_(NULL)
                                                                                 , shm_key_(pset.get<int>("shm_key", std::hash<std::string>()(uniqueLabel())))
                                                                                 , role_(role)
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

	if (buffer_count_ > 100)
	{
		throw cet::exception("ConfigurationException", "Buffer Count is too large for Shmem transfer!");
	}

	size_t shmSize = buffer_count_ * max_fragment_size_words_ * sizeof(artdaq::RawDataType) + sizeof(ShmStruct);

	shm_segment_id_ = shmget(shm_key_, shmSize, 0666);

	if (shm_segment_id_ == -1)
	{
		shm_segment_id_ = shmget(shm_key_, shmSize, IPC_CREAT | 0666);
	}

	mf::LogDebug(uniqueLabel()) << "shm_key == " << shm_key_ << ", shm_segment_id == " << shm_segment_id_;

	if (shm_segment_id_ > -1)
	{
		mf::LogDebug(uniqueLabel())
			<< "Created/fetched shared memory segment with ID = " << shm_segment_id_
			<< " and size " << shmSize
			<< " bytes";
		shm_ptr_ = (ShmStruct*)shmat(shm_segment_id_, 0, 0);
		mf::LogDebug(uniqueLabel())
			<< "Attached to shared memory segment at address "
			<< std::hex << shm_ptr_ << std::dec;
		if (shm_ptr_ && shm_ptr_ != (void *)-1)
		{
			if (role_ == Role::kReceive)
			{
				shm_ptr_->read_pos = 0;
				shm_ptr_->write_pos = 0;
				size_t offset = sizeof(ShmStruct);
				for (size_t ii = 0; ii < buffer_count_; ++ii)
				{
					mf::LogDebug(uniqueLabel()) << "Buffer " << ii << " is at 0x" << std::hex << offset << std::dec;
					shm_ptr_->buffers[ii].fragmentSizeWords = 0;
					shm_ptr_->buffers[ii].offset = offset;
					shm_ptr_->buffers[ii].sem = BUFFER_EMPTY;
					offset += max_fragment_size_words_ * sizeof(artdaq::RawDataType);
				}
			}
		}
		else
		{
			mf::LogError(uniqueLabel()) << "Failed to attach to shared memory segment "
				<< shm_segment_id_;
		}
	}
	else
	{
		mf::LogError(uniqueLabel()) << "Failed to connect to shared memory segment"
			<< ", errno = " << errno << ".  Please check "
			<< "if a stale shared memory segment needs to "
			<< "be cleaned up. (ipcs, ipcrm -m <segId>)";
	}
}

artdaq::ShmemTransfer::~ShmemTransfer()
{
	TRACE(5, "ShmemTransfer::~ShmemTransfer called");
	if (shm_ptr_)
	{
		shmdt(shm_ptr_);
		shm_ptr_ = NULL;
	}

	if (role_ == Role::kReceive && shm_segment_id_ > -1)
	{
		shmctl(shm_segment_id_, IPC_RMID, NULL);
	}
	TRACE(5, "ShmemTransfer::~ShmemTransfer done");
}

bool artdaq::ShmemTransfer::readyForRead_()
{
	return shm_ptr_->buffers[shm_ptr_->read_pos].sem == FRAGMENT_READY;
}

bool artdaq::ShmemTransfer::readyForWrite_()
{
	return shm_ptr_->buffers[shm_ptr_->write_pos].sem == BUFFER_EMPTY;
}

int artdaq::ShmemTransfer::receiveFragment(artdaq::Fragment& fragment,
                                           size_t receiveTimeout)
{
	if (shm_ptr_)
	{
		auto waitStart = std::chrono::steady_clock::now();
		while (!readyForRead_() && std::chrono::duration_cast<std::chrono::duration<size_t, std::ratio<1, 1000000>>>(std::chrono::steady_clock::now() - waitStart).count() < 1000)
		{
			// BURN THAT CPU!
		}
		if (!readyForRead_())
		{
			int64_t loopCount = 0;
			size_t sleepTime = 1000; // microseconds
			int64_t nloops = (receiveTimeout - 1000) / sleepTime;

			while (!readyForRead_() && loopCount < nloops)
			{
				usleep(sleepTime);
				++loopCount;
			}
		}

		//mf::LogDebug(uniqueLabel()) << "delta_=" << delta_() << ", rp=" << (int)shm_ptr_->read_pos << ", wp=" << (int)shm_ptr_->write_pos << ", loopCount=" << loopCount << ", nloops=" << nloops;

		if (readyForRead_())
		{
			// JCF, Jul-7-2016

			// Calling artdaq::Fragment::resize with the argument
			// shm_ptr_->fragmentSizeWords actually allocates more memory
			// for "fragment" than is needed as shm_ptr_->fragmentSizeWords
			// is the FULL size of the received fragment, not just the size
			// of its payload. We correct for this below.
			auto buf = &shm_ptr_->buffers[shm_ptr_->read_pos];
			auto initCount = buf->writeCount;

			TRACE(TRANSFER_RECEIVE1, "Setting semaphore on buf %u", shm_ptr_->read_pos.load());
			buf->sem = READING_FRAGMENT;
			RawDataType* bufPtr = offsetToPtr(buf->offset);
			//mf::LogDebug(uniqueLabel()) << "Pointer is " << bufPtr;
			fragment.resize(buf->fragmentSizeWords);

			artdaq::RawDataType* fragAddr = fragment.headerAddress();
			size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);
			//mf::LogDebug(uniqueLabel()) << "Copying fragment of size " << fragSize << " from buffer " << (int)shm_ptr_->read_pos << " at 0x" << std::hex << buf->offset << std::dec;
			memcpy(fragAddr, bufPtr, fragSize);
			//mf::LogDebug(uniqueLabel()) << "Done with copy";

			auto wordsOfHeaderAndMetadata = &*fragment.dataBegin() - &*fragment.headerBegin();
			fragment.resize(buf->fragmentSizeWords - wordsOfHeaderAndMetadata);

			if (buf->sem != READING_FRAGMENT || buf->writeCount != initCount)
			{
				//mf::LogWarning(uniqueLabel()) << "Semaphore was unset! This buffer has been clobbered!";
				return RECV_TIMEOUT; // Buffer was clobbered by a non-reliable writer...
			}

			shm_ptr_->read_pos++;
			if (shm_ptr_->read_pos >= buffer_count_) shm_ptr_->read_pos = 0;
			buf->sem = BUFFER_EMPTY;

			if (fragment.type() != artdaq::Fragment::DataFragmentType)
			{
				TRACE(TRANSFER_RECEIVE2, "Recvd frag from shmem, type=%d, sequenceID=%zu, source_rank=%d", (int)fragment.type(), fragment.sequenceID(), source_rank());
			}

			return source_rank();
		}

		return artdaq::TransferInterface::RECV_TIMEOUT;
	}
	else
	{
		mf::LogError(uniqueLabel()) << "Error in shared memory transfer plugin: pointer to shared memory segment is null, will sleep for "
			<< receiveTimeout / 1.0e6 << " seconds and then return a timeout";
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
                                    size_t send_timeout_usec)
{
	return sendFragment(std::move(fragment), send_timeout_usec, true);
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::sendFragment(artdaq::Fragment&& fragment, size_t send_timeout_usec, bool reliableMode)
{
	if (!shm_ptr_) { return CopyStatus::kErrorNotRequiringException; }


	// wait for the shm to become free, if requested     
	if (send_timeout_usec > 0)
	{
		auto waitStart = std::chrono::steady_clock::now();
		while (!readyForWrite_() && std::chrono::duration_cast<std::chrono::duration<size_t, std::ratio<1, 1000000>>>(std::chrono::steady_clock::now() - waitStart).count() < 1000)
		{
			// BURN THAT CPU!
		}
		if (!readyForWrite_())
		{
			int64_t loopCount = 0;
			size_t sleepTime = 1000; // microseconds
			int64_t nloops = (send_timeout_usec - 1000) / sleepTime;

			while (reliableMode && !readyForWrite_() && loopCount < nloops)
			{
				usleep(sleepTime);
				++loopCount;
			}
		}
	}

	//mf::LogDebug(uniqueLabel()) << "delta_=" << delta_() << ", rp=" << (int)shm_ptr_->read_pos << ", wp=" << (int)shm_ptr_->write_pos;

	// copy the fragment if the shm is available                                               
	if ((reliableMode && readyForWrite_()) || !reliableMode)
	{
		TRACE(TRANSFER_SEND2, "Sending fragment with seqID=%zu", fragment.sequenceID());
		artdaq::RawDataType* fragAddr = fragment.headerAddress();
		size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);

		// 10-Sep-2013, KAB - protect against large events and                                   
		// invalid events (and large, invalid events)                                            
		if (fragment.type() != artdaq::Fragment::InvalidFragmentType && fragSize < (max_fragment_size_words_ * sizeof(artdaq::RawDataType)))
		{
			auto buf = &shm_ptr_->buffers[shm_ptr_->write_pos];
			//mf::LogDebug(uniqueLabel()) << "Waiting for buffer " << (int)shm_ptr_->write_pos;
			buf->sem = WRITING_FRAGMENT;
			//mf::LogDebug(uniqueLabel()) << "Copying fragment with size " << fragSize << " into buffer " << (int)shm_ptr_->write_pos << " at 0x" << std::hex << buf->offset << std::dec;
			memcpy(offsetToPtr(buf->offset), fragAddr, fragSize);
			//mf::LogDebug(uniqueLabel()) << "Done with copy";
			buf->fragmentSizeWords = fragment.size();

			shm_ptr_->write_pos++;
			if (shm_ptr_->write_pos >= buffer_count_) shm_ptr_->write_pos = 0;
			if (shm_ptr_->write_pos == shm_ptr_->read_pos && !reliableMode)
			{
				shm_ptr_->read_pos++;
				if (shm_ptr_->read_pos >= buffer_count_) shm_ptr_->read_pos = 0;
			}
			buf->sem = FRAGMENT_READY;

			return CopyStatus::kSuccess;
		}
		else
		{
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

artdaq::RawDataType* artdaq::ShmemTransfer::offsetToPtr(size_t offset)
{
	auto res = reinterpret_cast<RawDataType*>(reinterpret_cast<uint8_t*>(shm_ptr_) + offset);
	//mf::LogDebug(uniqueLabel()) << std::hex << "base=" << shm_ptr_ << ", offset=0x" << offset << ", res=" << res << std::dec;
	return res;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::ShmemTransfer)

// Local Variables:
// mode: c++
// End:
