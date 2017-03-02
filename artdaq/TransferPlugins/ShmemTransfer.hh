#ifndef artdaq_TransferPlugins_ShmemTransfer_hh
#define artdaq_TransferPlugins_ShemmTransfer_hh

#include <fhiclcpp/fwd.h>
#include <atomic>

#include "artdaq/TransferPlugins/TransferInterface.hh"

#define BUFFER_EMPTY 0UL
#define WRITING_FRAGMENT 1UL
#define FRAGMENT_READY 2UL
#define READING_FRAGMENT 3UL

namespace artdaq {

	class ShmemTransfer : public artdaq::TransferInterface {

	public:

		ShmemTransfer(fhicl::ParameterSet const&, Role);
		~ShmemTransfer() noexcept;

		virtual int receiveFragment(artdaq::Fragment& fragment,
			size_t receiveTimeout);

		virtual CopyStatus copyFragment(artdaq::Fragment& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max());
		virtual CopyStatus moveFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max());
	private:
		CopyStatus sendFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec, bool reliable = false);

	  bool readyForRead_();
	  bool readyForWrite_();

		RawDataType* offsetToPtr(size_t offset);

		struct ShmBuffer {
			size_t offset;
			size_t fragmentSizeWords;
			std::atomic<unsigned int> sem;
			unsigned int writeCount;
		};
		struct ShmStruct {
			std::atomic<unsigned int> read_pos;
			std::atomic<unsigned int> write_pos;
			ShmBuffer buffers[100];
		};


		size_t send_timeout_usec_;
		int shm_segment_id_;
		ShmStruct* shm_ptr_;
		int shm_key_;

		Role role_;
	};

}

#endif // artdaq_TransferPlugins/ShmemTransfer_hh
