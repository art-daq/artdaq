#include "artdaq/TransferPlugins/ShmemTransfer.hh"
#include "cetlib_except/exception.h"

artdaq::ShmemTransfer::ShmemTransfer(fhicl::ParameterSet const& pset, Role role) :
	TransferInterface(pset, role)
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

	auto shmKey = pset.get<int>("shm_key", std::hash<std::string>()(uniqueLabel()));
	shm_manager_ = std::make_unique<SharedMemoryFragmentManager>(shmKey, buffer_count_, max_fragment_size_words_ * sizeof(artdaq::RawDataType));
}

artdaq::ShmemTransfer::~ShmemTransfer()
{
	TRACE(5, "ShmemTransfer::~ShmemTransfer called");
	shm_manager_.reset(nullptr);
	TRACE(5, "ShmemTransfer::~ShmemTransfer done");
}

int artdaq::ShmemTransfer::receiveFragment(artdaq::Fragment& fragment,
										   size_t receiveTimeout)
{
	auto waitStart = std::chrono::steady_clock::now();
	while (!shm_manager_->ReadyForRead() && std::chrono::duration_cast<std::chrono::duration<size_t, std::ratio<1, 1000000>>>(std::chrono::steady_clock::now() - waitStart).count() < 1000)
	{
		// BURN THAT CPU!
	}
	if (!shm_manager_->ReadyForRead())
	{
		int64_t loopCount = 0;
		size_t sleepTime = 1000; // microseconds
		int64_t nloops = (receiveTimeout - 1000) / sleepTime;

		while (!shm_manager_->ReadyForRead() && loopCount < nloops)
		{
			usleep(sleepTime);
			++loopCount;
		}
	}

	//TLOG_DEBUG(uniqueLabel()) << "delta_=" << delta_() << ", rp=" << (int)shm_ptr_->read_pos << ", wp=" << (int)shm_ptr_->write_pos << ", loopCount=" << loopCount << ", nloops=" << nloops << TLOG_ENDL;

	if (shm_manager_->ReadyForRead())
	{
		auto sts = shm_manager_->ReadFragment(fragment);

		if (sts != 0) return RECV_TIMEOUT;

		if (fragment.type() != artdaq::Fragment::DataFragmentType)
		{
			TRACE(TRANSFER_RECEIVE2, "Recvd frag from shmem, type=%d, sequenceID=%zu, source_rank=%d", (int)fragment.type(), fragment.sequenceID(), source_rank());
		}

		return source_rank();
	}

	return artdaq::TransferInterface::RECV_TIMEOUT;
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
	// wait for the shm to become free, if requested     
	if (send_timeout_usec > 0)
	{
		auto waitStart = std::chrono::steady_clock::now();
		while (!shm_manager_->ReadyForWrite(!reliableMode) && std::chrono::duration_cast<std::chrono::duration<size_t, std::ratio<1, 1000000>>>(std::chrono::steady_clock::now() - waitStart).count() < 1000)
		{
			// BURN THAT CPU!
		}
		if (!shm_manager_->ReadyForWrite(!reliableMode))
		{
			int64_t loopCount = 0;
			size_t sleepTime = 1000; // microseconds
			int64_t nloops = (send_timeout_usec - 1000) / sleepTime;

			while (reliableMode && !shm_manager_->ReadyForWrite(!reliableMode) && loopCount < nloops)
			{
				usleep(sleepTime);
				++loopCount;
			}
		}
	}

	TLOG_ARB(TRANSFER_SEND2, "ShmemTransfer") << "Either write has timed out or buffer ready" << TLOG_ENDL;

	// copy the fragment if the shm is available                                               
	if (shm_manager_->ReadyForWrite(!reliableMode))
	{
		TRACE(TRANSFER_SEND2, "Sending fragment with seqID=%zu", fragment.sequenceID());
		artdaq::RawDataType* fragAddr = fragment.headerAddress();
		size_t fragSize = fragment.size() * sizeof(artdaq::RawDataType);

		// 10-Sep-2013, KAB - protect against large events and                                   
		// invalid events (and large, invalid events)                                            
		if (fragment.type() != artdaq::Fragment::InvalidFragmentType && fragSize < (max_fragment_size_words_ * sizeof(artdaq::RawDataType)))
		{
			auto sts = shm_manager_->WriteFragment(std::move(fragment), !reliableMode);
			if (sts != 0) return CopyStatus::kErrorNotRequiringException;

			return CopyStatus::kSuccess;
		}
		else
		{
			TLOG_WARNING(uniqueLabel()) << "Fragment invalid for shared memory! "
				<< "fragment address and size = "
				<< fragAddr << " " << fragSize << " "
				<< "sequence ID, fragment ID, and type = "
				<< fragment.sequenceID() << " "
				<< fragment.fragmentID() << " "
				<< ((int)fragment.type()) << TLOG_ENDL;
			return CopyStatus::kErrorNotRequiringException;
		}
	}

	return CopyStatus::kTimeout;
}

DEFINE_ARTDAQ_TRANSFER(artdaq::ShmemTransfer)

// Local Variables:
// mode: c++
// End:
