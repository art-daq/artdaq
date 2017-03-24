#ifndef artdaq_RTIDDS_RTIDDS_hh
#define artdaq_RTIDDS_RTIDDS_hh

#include "artdaq-core/Data/Fragment.hh"

#include <ndds/ndds_cpp.h>

#include <string>
#include <queue>
#include <mutex>


namespace artdaq
{
	class RTIDDS;
}

class artdaq::RTIDDS
{
public:

	enum class IOType
	{
		reader,
		writer
	};

	RTIDDS(std::string name, IOType iotype, std::string max_size = "1000000");

	~RTIDDS() = default;

	// JCF, Apr-7-2016
	// Are copy constructor, assignment operators, etc., logical absurdities?

	void copyFragmentToDDS_(artdaq::Fragment& fragment);

	void moveFragmentToDDS_(artdaq::Fragment&& fragment);

	class OctetsListener: public DDSDataReaderListener
	{
	public:

		void on_data_available(DDSDataReader* reader);

		bool receiveFragmentFromDDS(artdaq::Fragment& fragment,
		                            const size_t receiveTimeout);

	private:

		DDS_Octets dds_octets_;
		std::queue<DDS_Octets> dds_octets_queue_;

		std::mutex queue_mutex_;
	};

	OctetsListener octets_listener_;

private:

	std::string name_;
	IOType iotype_;
	std::string max_size_;

	std::unique_ptr<DDSDomainParticipant, std::function<void(DDSDomainParticipant * )>> participant_;

	DDSTopic* topic_octets_;
	DDSOctetsDataWriter* octets_writer_;
	DDSDataReader* octets_reader_;

	static void participantDeleter(DDSDomainParticipant* participant);
};

#endif
