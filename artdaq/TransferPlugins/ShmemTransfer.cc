#include "artdaq/DAQdata/Globals.hh"
#define TRACE_NAME (app_name + "_ShmemTransfer").c_str()
#include "TRACE/tracemf.h"

#include "artdaq/TransferPlugins/ShmemTransfer.hh"

#include <boost/lexical_cast.hpp>

#include <csignal>
#include <memory>

artdaq::ShmemTransfer::ShmemTransfer(fhicl::ParameterSet const& pset, Role role)
    : TransferInterface(pset, role)
{
	TLOG(TLVL_DEBUG + 32) << GetTraceName() << "Constructor BEGIN";
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

	auto partition = GetPartitionNumber() + 1;  // Can't be 0

	auto shmKey = pset.get<uint32_t>("shm_key_offset", 0) + (partition << 24) + ((source_rank() & 0xFFF) << 12) + (destination_rank() & 0xFFF);

	// Configured Shared Memory key overrides everything! Needed for Online Monitor connections!
	if (pset.has_key("shm_key"))
	{
		shmKey = pset.get<uint32_t>("shm_key");
	}

	if (role == Role::kReceive)
	{
		shm_manager_ = std::make_unique<SharedMemoryFragmentManager>(shmKey, buffer_count_, max_fragment_size_words_ * sizeof(artdaq::RawDataType), pset.get<size_t>("stale_buffer_timeout_usec", 100 * 1000000));
	}
	else
	{
		shm_manager_ = std::make_unique<SharedMemoryFragmentManager>(shmKey);
	}
	TLOG(TLVL_DEBUG + 32) << GetTraceName() << "Constructor END";
}

artdaq::ShmemTransfer::~ShmemTransfer() noexcept
{
	TLOG(TLVL_DEBUG + 34) << GetTraceName() << " ~ShmemTransfer called - " << uniqueLabel();
	shm_manager_.reset(nullptr);
	TLOG(TLVL_DEBUG + 34) << GetTraceName() << " ~ShmemTransfer done - " << uniqueLabel();
}

int artdaq::ShmemTransfer::receiveFragment(artdaq::Fragment& fragment,
                                           size_t receiveTimeout)
{
	auto waitStart = std::chrono::steady_clock::now();
	while (!shm_manager_->ReadyForRead() && TimeUtils::GetElapsedTimeMicroseconds(waitStart) < 1000)
	{
		// BURN THAT CPU!
	}
	if (!shm_manager_->ReadyForRead())
	{
		int64_t loopCount = 0;
		size_t sleepTime = 1000;  // microseconds
		int64_t nloops = (receiveTimeout - 1000) / sleepTime;

		while (!shm_manager_->ReadyForRead() && loopCount < nloops)
		{
			usleep(sleepTime);
			++loopCount;
		}
	}
	if (!shm_manager_->ReadyForRead() && shm_manager_->IsEndOfData())
	{
		return artdaq::TransferInterface::DATA_END;
	}

	TLOG(TLVL_DEBUG + 33) << GetTraceName() << "receiveFragment ReadyForRead=" << shm_manager_->ReadyForRead();

	if (shm_manager_->ReadyForRead())
	{
		auto sts = shm_manager_->ReadFragment(fragment);

		if (sts != 0)
		{
			TLOG(TLVL_DEBUG + 33) << "Non-zero status (" << sts << ") returned from ReadFragment, returning...";
			return RECV_TIMEOUT;
		}

		if (fragment.type() != artdaq::Fragment::DataFragmentType)
		{
			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Recvd frag from shmem, type=" << fragment.typeString() << ", sequenceID=" << fragment.sequenceID() << ", source_rank=" << source_rank();
		}

		return source_rank();
	}

	return artdaq::TransferInterface::RECV_TIMEOUT;
}

int artdaq::ShmemTransfer::receiveFragmentHeader(detail::RawFragmentHeader& header, size_t receiveTimeout)
{
	auto waitStart = std::chrono::steady_clock::now();
	while (!shm_manager_->ReadyForRead() && TimeUtils::GetElapsedTimeMicroseconds(waitStart) < 1000)
	{
		// BURN THAT CPU!
	}
	if (!shm_manager_->ReadyForRead())
	{
		int64_t loopCount = 0;
		size_t sleepTime = 1000;  // microseconds
		int64_t nloops = (receiveTimeout - 1000) / sleepTime;

		while (!shm_manager_->ReadyForRead() && loopCount < nloops)
		{
			usleep(sleepTime);
			++loopCount;
		}
	}

	if (!shm_manager_->ReadyForRead() && shm_manager_->IsEndOfData())
	{
		return artdaq::TransferInterface::DATA_END;
	}

	// TLOG(TLVL_DEBUG + 33) << GetTraceName() << "delta_=" << delta_() << ", rp=" << (int)shm_ptr_->read_pos << ", wp=" << (int)shm_ptr_->write_pos << ", loopCount=" << loopCount << ", nloops=" << nloops ;

	if (shm_manager_->ReadyForRead())
	{
		auto sts = shm_manager_->ReadFragmentHeader(header);

		if (sts != 0)
		{
			TLOG(TLVL_DEBUG + 33) << "Non-zero status (" << sts << ") returned from ReadFragmentHeader, returning...";
			return RECV_TIMEOUT;
		}

		if (header.type != artdaq::Fragment::DataFragmentType)
		{
			TLOG(TLVL_DEBUG + 38) << GetTraceName() << "Recvd fragment header from shmem, type=" << static_cast<int>(header.type)
			                      << ", sequenceID=" << header.sequence_id << ", source_rank=" << source_rank();
		}

		return source_rank();
	}

	return artdaq::TransferInterface::RECV_TIMEOUT;
}

int artdaq::ShmemTransfer::receiveFragmentData(RawDataType* destination, size_t word_count)
{
	auto sts = shm_manager_->ReadFragmentData(destination, word_count);

	TLOG(TLVL_DEBUG + 33) << GetTraceName() << "Return status from ReadFragmentData is " << sts;

	if (sts != 0)
	{
		TLOG(TLVL_DEBUG + 33) << "Non-zero status (" << sts << ") returned from ReadFragmentData, returning...";
		return RECV_TIMEOUT;
	}

	return source_rank();

	return artdaq::TransferInterface::RECV_TIMEOUT;
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::transfer_fragment_min_blocking_mode(artdaq::Fragment const& fragment, size_t send_timeout_usec)
{
	return sendFragment(Fragment(fragment), send_timeout_usec, false);
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::transfer_fragment_reliable_mode(artdaq::Fragment&& fragment)
{
	return sendFragment(std::move(fragment), 0, true);
}

artdaq::TransferInterface::CopyStatus
artdaq::ShmemTransfer::sendFragment(artdaq::Fragment&& fragment, size_t send_timeout_usec, bool reliableMode)
{
	if (!isRunning())
	{
		shm_manager_->Attach();
		if (!isRunning())
		{
			TLOG(TLVL_ERROR) << GetTraceName() << "Attempted to send Fragment when not attached to Shared Memory! Returning kErrorNotRequiringException, and dropping data!";
			return CopyStatus::kErrorNotRequiringException;
		}
	}
	shm_manager_->SetRank(my_rank);
	// wait for the shm to become free, if requested

	TLOG(TLVL_DEBUG + 34) << GetTraceName() << "Sending fragment with seqID=" << fragment.sequenceID();
	artdaq::RawDataType* fragAddr = fragment.headerAddress();
	size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);

	// 10-Sep-2013, KAB - protect against large events and
	// invalid events (and large, invalid events)
	if (fragment.type() != artdaq::Fragment::InvalidFragmentType && fragSize < (max_fragment_size_words_ * sizeof(artdaq::RawDataType)))
	{
		auto seq = fragment.sequenceID();
		TLOG(TLVL_DEBUG + 34) << GetTraceName() << "Writing fragment with seqID=" << seq;
		auto sts = shm_manager_->WriteFragment(std::move(fragment), !reliableMode, send_timeout_usec);
		if (sts == -3)
		{
			TLOG(TLVL_WARNING) << GetTraceName() << "Timeout writing fragment with seqID=" << seq;
			return CopyStatus::kTimeout;
		}
		if (sts != 0)
		{
			TLOG(TLVL_WARNING) << GetTraceName() << "Error writing fragment with seqID=" << seq;
			return CopyStatus::kErrorNotRequiringException;
		}

		TLOG(TLVL_DEBUG + 34) << GetTraceName() << "Successfully sent Fragment with seqID=" << seq;
		return CopyStatus::kSuccess;
	}

	TLOG(TLVL_WARNING) << GetTraceName() << "Fragment invalid for shared memory! "
	                   << "fragment address and size = "
	                   << fragAddr << " " << fragSize << " "
	                   << "sequence ID, fragment ID, and type = "
	                   << fragment.sequenceID() << " "
	                   << fragment.fragmentID() << " "
	                   << fragment.typeString();
	return CopyStatus::kErrorNotRequiringException;

	TLOG(TLVL_WARNING) << GetTraceName() << "Unreachable code reached!";
	return CopyStatus::kErrorNotRequiringException;
}

bool artdaq::ShmemTransfer::isRunning()
{
	bool ret = false;
	switch (role())
	{
		case TransferInterface::Role::kSend:
			ret = shm_manager_->IsValid() && !shm_manager_->IsEndOfData();
			break;
		case TransferInterface::Role::kReceive:
			ret = shm_manager_->GetAttachedCount() > 1;
			break;
	}
	return ret;
}

void artdaq::ShmemTransfer::flush_buffers()
{
	for (size_t ii = 0; ii < shm_manager_->size(); ++ii)
	{
		shm_manager_->MarkBufferEmpty(ii, true);
	}
}

// Local Variables:
// mode: c++
// End:
