#ifndef artdaq_TransferPlugins_ShmemTransfer_hh
#define artdaq_TransferPlugins_ShemmTransfer_hh

#include <fhiclcpp/fwd.h>

#include "artdaq/TransferPlugins/TransferInterface.hh"

namespace artdaq {

	class ShmemTransfer : public artdaq::TransferInterface {

	public:

		ShmemTransfer(fhicl::ParameterSet const&, Role);
		~ShmemTransfer();

		virtual int receiveFragment(artdaq::Fragment& fragment,
			size_t receiveTimeout);

		virtual CopyStatus copyFragment(artdaq::Fragment& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max());
		virtual CopyStatus moveFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec = std::numeric_limits<size_t>::max());
	private:
		CopyStatus sendFragment(artdaq::Fragment&& fragment,
			size_t send_timeout_usec, bool reliable = false);

		int delta_();
		RawDataType* offsetToPtr(size_t offset);

		enum class bufsem : uint8_t {
			buffer_empty = 0,
			writing_fragment = 1,
			fragment_ready = 2,
			reading_fragment = 3
		};

		struct ShmBuffer {
			uint64_t offset;
			uint64_t fragmentSizeWords;
			bufsem sem;
			uint32_t writeCount;
		};
		struct ShmStruct {
			uint8_t read_pos;
			uint8_t write_pos;
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
